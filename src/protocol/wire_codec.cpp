#include "tidepool/protocol/wire_codec.h"

#include <algorithm>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>

namespace tidepool::wire {
namespace {

Status Invalid(std::string message) {
    return Status::InvalidArgument("wire_codec: " + std::move(message));
}

Status Corrupt(std::string message) {
    return Status::Corruption("wire_codec: " + std::move(message));
}

Status Capacity(std::string message) {
    return Status(StatusCode::kOutOfCapacity,
                  "wire_codec: " + std::move(message));
}

Status InputError(bool network_input, std::string message) {
    return network_input ? Corrupt(std::move(message))
                         : Invalid(std::move(message));
}

void PutU16(std::uint8_t* out, std::uint16_t value) {
    out[0] = static_cast<std::uint8_t>((value >> 8U) & 0xffU);
    out[1] = static_cast<std::uint8_t>(value & 0xffU);
}

void PutU32(std::uint8_t* out, std::uint32_t value) {
    for (std::size_t i = 0; i < 4; ++i) {
        const unsigned shift = static_cast<unsigned>((3U - i) * 8U);
        out[i] = static_cast<std::uint8_t>((value >> shift) & 0xffU);
    }
}

void PutU64(std::uint8_t* out, std::uint64_t value) {
    for (std::size_t i = 0; i < 8; ++i) {
        const unsigned shift = static_cast<unsigned>((7U - i) * 8U);
        out[i] = static_cast<std::uint8_t>((value >> shift) & 0xffU);
    }
}

std::uint16_t GetU16(const std::uint8_t* data) {
    return static_cast<std::uint16_t>(
        (static_cast<std::uint16_t>(data[0]) << 8U) |
        static_cast<std::uint16_t>(data[1]));
}

std::uint32_t GetU32(const std::uint8_t* data) {
    std::uint32_t value = 0;
    for (std::size_t i = 0; i < 4; ++i) {
        value = static_cast<std::uint32_t>(
            (value << 8U) | static_cast<std::uint32_t>(data[i]));
    }
    return value;
}

std::uint64_t GetU64(const std::uint8_t* data) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        value = static_cast<std::uint64_t>(
            (value << 8U) | static_cast<std::uint64_t>(data[i]));
    }
    return value;
}

bool IsKnownKind(MessageKind kind) {
    switch (kind) {
        case MessageKind::kRequest:
        case MessageKind::kResponse:
            return true;
    }
    return false;
}

bool IsKnownOpcode(Opcode opcode) {
    switch (opcode) {
        case Opcode::kGet:
        case Opcode::kPut:
        case Opcode::kPing:
            return true;
    }
    return false;
}

bool IsKnownWireStatus(WireStatus status) {
    switch (status) {
        case WireStatus::kOk:
        case WireStatus::kNotImplemented:
        case WireStatus::kNotFound:
        case WireStatus::kInvalidArgument:
        case WireStatus::kAlreadyExists:
        case WireStatus::kIoError:
        case WireStatus::kNetworkError:
        case WireStatus::kUnavailable:
        case WireStatus::kOutOfCapacity:
        case WireStatus::kCorruption:
        case WireStatus::kInternal:
            return true;
    }
    return false;
}

Result<std::size_t> ValidateFrameSizeImpl(std::uint64_t payload_bytes,
                                         const CodecLimits& limits,
                                         bool network_input) {
    if (limits.max_frame_bytes < kHeaderBytes) {
        return Invalid("max_frame_bytes is smaller than the 32-byte header");
    }

    const std::uint64_t max_payload =
        limits.max_frame_bytes - static_cast<std::uint64_t>(kHeaderBytes);
    if (payload_bytes > max_payload) {
        return InputError(network_input, "payload is larger than max_frame_bytes");
    }

    constexpr std::size_t kSizeMax =
        std::numeric_limits<std::size_t>::max();
    if constexpr (sizeof(std::size_t) < sizeof(std::uint64_t)) {
        if (payload_bytes > static_cast<std::uint64_t>(kSizeMax)) {
            return InputError(network_input,
                              "payload length cannot be represented by size_t");
        }
    }
    if (payload_bytes >
        static_cast<std::uint64_t>(kSizeMax - kHeaderBytes)) {
        return InputError(network_input, "header plus payload length overflows size_t");
    }

    return kHeaderBytes + static_cast<std::size_t>(payload_bytes);
}

Status ValidatePayloadLength(const FrameHeader& header,
                             const CodecLimits& limits,
                             bool network_input) {
    const auto fail = [network_input](std::string message) {
        return InputError(network_input, std::move(message));
    };

    if (header.kind == MessageKind::kRequest) {
        if (header.status != WireStatus::kOk) {
            return fail("request status must be kOk");
        }
        switch (header.opcode) {
            case Opcode::kGet:
                if (header.payload_bytes != kBlockKeyBytes) {
                    return fail("Get request payload must be exactly 16 bytes");
                }
                break;
            case Opcode::kPut:
                if (header.payload_bytes <= kBlockKeyBytes) {
                    return fail("Put request must contain a non-empty Block blob");
                }
                break;
            case Opcode::kPing:
                if (header.payload_bytes != 0) {
                    return fail("Ping request payload must be empty");
                }
                break;
        }
        return Status::Ok();
    }

    if (header.status != WireStatus::kOk) {
        if (header.payload_bytes < 4) {
            return fail("error response payload is shorter than its length field");
        }
        const std::uint64_t max_error_payload =
            4ULL + static_cast<std::uint64_t>(limits.max_error_message_bytes);
        if (header.payload_bytes > max_error_payload) {
            return fail("error response payload exceeds the error message limit");
        }
        return Status::Ok();
    }

    switch (header.opcode) {
        case Opcode::kGet:
            if (header.payload_bytes == 0) {
                return fail("successful Get response requires a non-empty Block blob");
            }
            break;
        case Opcode::kPut:
            if (header.payload_bytes != 0) {
                return fail("successful Put response payload must be empty");
            }
            break;
        case Opcode::kPing:
            if (header.payload_bytes != 0) {
                return fail("successful Ping response payload must be empty");
            }
            break;
    }
    return Status::Ok();
}

Status ValidateHeaderFields(const FrameHeader& header,
                            const CodecLimits& limits,
                            bool network_input) {
    if (!IsKnownKind(header.kind)) {
        return InputError(network_input, "unknown message kind");
    }
    if (!IsKnownOpcode(header.opcode)) {
        return InputError(network_input, "unknown opcode");
    }
    if (!IsKnownWireStatus(header.status)) {
        return InputError(network_input, "unknown wire status");
    }
    if (header.flags != 0) {
        return InputError(network_input, "v1 flags must be zero");
    }

    auto frame_size =
        ValidateFrameSizeImpl(header.payload_bytes, limits, network_input);
    if (!frame_size.ok()) return frame_size.status();
    return ValidatePayloadLength(header, limits, network_input);
}

Status ValidatePayloadPointer(const std::uint8_t* payload, std::size_t size) {
    if (payload == nullptr && size != 0) {
        return Corrupt("payload pointer is null with non-zero size");
    }
    return Status::Ok();
}

Status ValidateDeclaredPayloadSize(const FrameHeader& header,
                                   const std::uint8_t* payload,
                                   std::size_t size) {
    if (Status status = ValidatePayloadPointer(payload, size); !status.ok()) {
        return status;
    }
    if (header.payload_bytes != static_cast<std::uint64_t>(size)) {
        return Corrupt("payload size does not match header payload_bytes");
    }
    return Status::Ok();
}

Result<ByteBuffer> CopyBytes(std::string_view bytes) {
    if (bytes.empty()) return ByteBuffer{};
    try {
        return ByteBuffer(
            reinterpret_cast<const std::uint8_t*>(bytes.data()),
            reinterpret_cast<const std::uint8_t*>(bytes.data()) + bytes.size());
    } catch (const std::bad_alloc&) {
        return Capacity("byte buffer allocation failed");
    } catch (const std::length_error&) {
        return Capacity("byte buffer is larger than the container limit");
    }
}

Result<std::string> CopyString(const std::uint8_t* bytes, std::size_t size) {
    if (size == 0) return std::string{};
    try {
        return std::string(reinterpret_cast<const char*>(bytes), size);
    } catch (const std::bad_alloc&) {
        return Capacity("string allocation failed");
    } catch (const std::length_error&) {
        return Capacity("string is larger than the container limit");
    }
}

}  // namespace

Result<std::size_t> ValidateFrameSize(std::uint64_t payload_bytes,
                                      const CodecLimits& limits) {
    return ValidateFrameSizeImpl(payload_bytes, limits, false);
}

Result<EncodedHeader> EncodeHeader(const FrameHeader& header,
                                   const CodecLimits& limits) {
    if (Status status = ValidateHeaderFields(header, limits, false);
        !status.ok()) {
        return status;
    }

    EncodedHeader encoded{};
    PutU32(encoded.data(), kMagic);
    PutU16(encoded.data() + 4, kVersion);
    PutU16(encoded.data() + 6, static_cast<std::uint16_t>(kHeaderBytes));
    encoded[8] = static_cast<std::uint8_t>(header.kind);
    encoded[9] = static_cast<std::uint8_t>(header.opcode);
    PutU16(encoded.data() + 10, static_cast<std::uint16_t>(header.status));
    PutU32(encoded.data() + 12, header.flags);
    PutU64(encoded.data() + 16, header.request_id);
    PutU64(encoded.data() + 24, header.payload_bytes);
    return encoded;
}

Result<FrameHeader> DecodeHeader(const std::uint8_t* bytes, std::size_t size,
                                 const CodecLimits& limits) {
    if (size < kHeaderBytes) {
        return Corrupt("truncated fixed header");
    }
    if (size > kHeaderBytes) {
        return Corrupt("unexpected bytes after fixed header");
    }
    if (bytes == nullptr) {
        return Corrupt("header pointer is null");
    }
    if (limits.max_frame_bytes < kHeaderBytes) {
        return Invalid("max_frame_bytes is smaller than the 32-byte header");
    }
    if (GetU32(bytes) != kMagic) {
        return Corrupt("bad magic");
    }
    if (GetU16(bytes + 4) != kVersion) {
        return Corrupt("unsupported version");
    }
    if (GetU16(bytes + 6) != kHeaderBytes) {
        return Corrupt("invalid header size");
    }

    FrameHeader header;
    header.kind = static_cast<MessageKind>(bytes[8]);
    header.opcode = static_cast<Opcode>(bytes[9]);
    header.status = static_cast<WireStatus>(GetU16(bytes + 10));
    header.flags = GetU32(bytes + 12);
    header.request_id = GetU64(bytes + 16);
    header.payload_bytes = GetU64(bytes + 24);

    if (Status status = ValidateHeaderFields(header, limits, true);
        !status.ok()) {
        return status;
    }
    return header;
}

Result<EncodedBlockKey> EncodeBlockKey(const BlockKey& key) {
    EncodedBlockKey encoded{};
    PutU64(encoded.data(), key.prefix_hash);
    PutU32(encoded.data() + 8, key.prefix_len);
    PutU32(encoded.data() + 12, key.model_fingerprint);
    return encoded;
}

Result<BlockKey> DecodeBlockKey(const std::uint8_t* bytes, std::size_t size) {
    if (size < kBlockKeyBytes) {
        return Corrupt("truncated BlockKey");
    }
    if (size > kBlockKeyBytes) {
        return Corrupt("unexpected bytes after BlockKey");
    }
    if (bytes == nullptr) {
        return Corrupt("BlockKey pointer is null");
    }

    BlockKey key;
    key.prefix_hash = GetU64(bytes);
    key.prefix_len = GetU32(bytes + 8);
    key.model_fingerprint = GetU32(bytes + 12);
    return key;
}

Result<ByteBuffer> EncodeGetRequest(const BlockKey& key) {
    auto encoded_key = EncodeBlockKey(key);
    if (!encoded_key.ok()) return encoded_key.status();
    try {
        return ByteBuffer(encoded_key.value().begin(), encoded_key.value().end());
    } catch (const std::bad_alloc&) {
        return Capacity("Get request allocation failed");
    } catch (const std::length_error&) {
        return Capacity("Get request is larger than the container limit");
    }
}

Result<BlockKey> DecodeGetRequest(const std::uint8_t* payload,
                                  std::size_t size) {
    return DecodeBlockKey(payload, size);
}

Result<ByteBuffer> EncodeGetSuccessPayload(std::string_view block_blob,
                                           const CodecLimits& limits) {
    if (block_blob.empty()) {
        return Invalid("successful Get payload requires a non-empty Block blob");
    }
    auto frame_size = ValidateFrameSize(block_blob.size(), limits);
    if (!frame_size.ok()) return frame_size.status();
    return CopyBytes(block_blob);
}

Result<std::string> DecodeGetSuccessPayload(const std::uint8_t* payload,
                                            std::size_t size,
                                            const CodecLimits& limits) {
    if (size == 0) {
        return Corrupt("successful Get payload requires a non-empty Block blob");
    }
    if (Status status = ValidatePayloadPointer(payload, size); !status.ok()) {
        return status;
    }
    auto frame_size = ValidateFrameSizeImpl(size, limits, true);
    if (!frame_size.ok()) return frame_size.status();
    return CopyString(payload, size);
}

Result<ByteBuffer> EncodePutRequest(const BlockKey& key,
                                    std::string_view block_blob,
                                    const CodecLimits& limits) {
    if (block_blob.empty()) {
        return Invalid("Put request requires a non-empty Block blob");
    }
    if (block_blob.size() >
        std::numeric_limits<std::uint64_t>::max() - kBlockKeyBytes) {
        return Invalid("Put request length overflows uint64_t");
    }
    const std::uint64_t payload_bytes =
        static_cast<std::uint64_t>(kBlockKeyBytes) + block_blob.size();
    auto frame_size = ValidateFrameSize(payload_bytes, limits);
    if (!frame_size.ok()) return frame_size.status();

    auto encoded_key = EncodeBlockKey(key);
    if (!encoded_key.ok()) return encoded_key.status();
    try {
        ByteBuffer encoded;
        encoded.reserve(static_cast<std::size_t>(payload_bytes));
        encoded.insert(encoded.end(), encoded_key.value().begin(),
                       encoded_key.value().end());
        encoded.insert(
            encoded.end(),
            reinterpret_cast<const std::uint8_t*>(block_blob.data()),
            reinterpret_cast<const std::uint8_t*>(block_blob.data()) +
                block_blob.size());
        return encoded;
    } catch (const std::bad_alloc&) {
        return Capacity("Put request allocation failed");
    } catch (const std::length_error&) {
        return Capacity("Put request is larger than the container limit");
    }
}

Result<DecodedPutRequest> DecodePutRequest(const std::uint8_t* payload,
                                           std::size_t size,
                                           const CodecLimits& limits) {
    if (size <= kBlockKeyBytes) {
        return Corrupt("Put request must contain a key and non-empty Block blob");
    }
    if (Status status = ValidatePayloadPointer(payload, size); !status.ok()) {
        return status;
    }
    auto frame_size = ValidateFrameSizeImpl(size, limits, true);
    if (!frame_size.ok()) return frame_size.status();

    auto key = DecodeBlockKey(payload, kBlockKeyBytes);
    if (!key.ok()) return key.status();

    auto block_blob = CopyString(payload + kBlockKeyBytes,
                                 size - kBlockKeyBytes);
    if (!block_blob.ok()) return block_blob.status();
    return DecodedPutRequest{key.value(), std::move(block_blob.value())};
}

Result<ByteBuffer> EncodePingRequest() { return ByteBuffer{}; }

Status DecodePingRequest(const std::uint8_t* payload, std::size_t size) {
    if (payload == nullptr && size == 0) return Status::Ok();
    if (size != 0) return Corrupt("Ping request payload must be empty");
    return Status::Ok();
}

Result<ByteBuffer> EncodeEmptySuccessPayload() { return ByteBuffer{}; }

Status DecodeEmptySuccessPayload(const std::uint8_t* payload,
                                 std::size_t size) {
    if (payload == nullptr && size == 0) return Status::Ok();
    if (size != 0) return Corrupt("successful response payload must be empty");
    return Status::Ok();
}

Result<ByteBuffer> EncodeErrorPayload(std::string_view message,
                                      const CodecLimits& limits) {
    if (message.size() > limits.max_error_message_bytes) {
        return Invalid("error message exceeds max_error_message_bytes");
    }
    if (message.size() > std::numeric_limits<std::uint32_t>::max()) {
        return Invalid("error message length overflows uint32_t");
    }
    const std::uint64_t payload_bytes = 4ULL + message.size();
    auto frame_size = ValidateFrameSize(payload_bytes, limits);
    if (!frame_size.ok()) return frame_size.status();

    try {
        ByteBuffer encoded(static_cast<std::size_t>(payload_bytes));
        PutU32(encoded.data(), static_cast<std::uint32_t>(message.size()));
        if (!message.empty()) {
            const auto* begin =
                reinterpret_cast<const std::uint8_t*>(message.data());
            std::copy(begin, begin + message.size(), encoded.begin() + 4);
        }
        return encoded;
    } catch (const std::bad_alloc&) {
        return Capacity("error payload allocation failed");
    } catch (const std::length_error&) {
        return Capacity("error payload is larger than the container limit");
    }
}

Result<std::string> DecodeErrorPayload(const std::uint8_t* payload,
                                       std::size_t size,
                                       const CodecLimits& limits) {
    if (size < 4) {
        return Corrupt("truncated error payload length field");
    }
    if (payload == nullptr) {
        return Corrupt("error payload pointer is null");
    }
    auto frame_size = ValidateFrameSizeImpl(size, limits, true);
    if (!frame_size.ok()) return frame_size.status();

    const std::uint32_t message_bytes = GetU32(payload);
    if (message_bytes > limits.max_error_message_bytes) {
        return Corrupt("error message exceeds max_error_message_bytes");
    }
    if (static_cast<std::size_t>(message_bytes) != size - 4) {
        return Corrupt("error message length does not exactly match payload");
    }
    return CopyString(payload + 4, message_bytes);
}

Status ValidateRequest(const FrameHeader& header,
                       const std::uint8_t* payload, std::size_t size,
                       const CodecLimits& limits) {
    if (Status status = ValidateHeaderFields(header, limits, true);
        !status.ok()) {
        return status;
    }
    if (header.kind != MessageKind::kRequest) {
        return Corrupt("ValidateRequest received a response header");
    }
    if (Status status = ValidateDeclaredPayloadSize(header, payload, size);
        !status.ok()) {
        return status;
    }

    switch (header.opcode) {
        case Opcode::kGet:
            return DecodeGetRequest(payload, size).status();
        case Opcode::kPut:
            return DecodePutRequest(payload, size, limits).status();
        case Opcode::kPing:
            return DecodePingRequest(payload, size);
    }
    return Corrupt("unknown request opcode");
}

Status ValidateResponse(const FrameHeader& header,
                        const std::uint8_t* payload, std::size_t size,
                        const CodecLimits& limits) {
    if (Status status = ValidateHeaderFields(header, limits, true);
        !status.ok()) {
        return status;
    }
    if (header.kind != MessageKind::kResponse) {
        return Corrupt("ValidateResponse received a request header");
    }
    if (Status status = ValidateDeclaredPayloadSize(header, payload, size);
        !status.ok()) {
        return status;
    }
    if (header.status != WireStatus::kOk) {
        return DecodeErrorPayload(payload, size, limits).status();
    }

    switch (header.opcode) {
        case Opcode::kGet:
            return DecodeGetSuccessPayload(payload, size, limits).status();
        case Opcode::kPut:
        case Opcode::kPing:
            return DecodeEmptySuccessPayload(payload, size);
    }
    return Corrupt("unknown response opcode");
}

Result<WireStatus> ToWireStatus(StatusCode code) {
    switch (code) {
        case StatusCode::kOk:
            return WireStatus::kOk;
        case StatusCode::kNotImplemented:
            return WireStatus::kNotImplemented;
        case StatusCode::kNotFound:
            return WireStatus::kNotFound;
        case StatusCode::kInvalidArgument:
            return WireStatus::kInvalidArgument;
        case StatusCode::kAlreadyExists:
            return WireStatus::kAlreadyExists;
        case StatusCode::kIoError:
            return WireStatus::kIoError;
        case StatusCode::kNetworkError:
            return WireStatus::kNetworkError;
        case StatusCode::kUnavailable:
            return WireStatus::kUnavailable;
        case StatusCode::kOutOfCapacity:
            return WireStatus::kOutOfCapacity;
        case StatusCode::kCorruption:
            return WireStatus::kCorruption;
        case StatusCode::kInternal:
            return WireStatus::kInternal;
    }
    return Invalid("unmapped StatusCode");
}

Result<StatusCode> FromWireStatus(WireStatus status) {
    switch (status) {
        case WireStatus::kOk:
            return StatusCode::kOk;
        case WireStatus::kNotImplemented:
            return StatusCode::kNotImplemented;
        case WireStatus::kNotFound:
            return StatusCode::kNotFound;
        case WireStatus::kInvalidArgument:
            return StatusCode::kInvalidArgument;
        case WireStatus::kAlreadyExists:
            return StatusCode::kAlreadyExists;
        case WireStatus::kIoError:
            return StatusCode::kIoError;
        case WireStatus::kNetworkError:
            return StatusCode::kNetworkError;
        case WireStatus::kUnavailable:
            return StatusCode::kUnavailable;
        case WireStatus::kOutOfCapacity:
            return StatusCode::kOutOfCapacity;
        case WireStatus::kCorruption:
            return StatusCode::kCorruption;
        case WireStatus::kInternal:
            return StatusCode::kInternal;
    }
    return Corrupt("unknown WireStatus");
}

}  // namespace tidepool::wire
