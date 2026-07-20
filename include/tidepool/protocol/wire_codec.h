// wire_codec.h — TidePool Wire Protocol v1 pure-memory codec.
//
// This module owns only the outer network representation. It deliberately has
// no socket, thread, routing, or StorageNode dependency. The embedded Block
// blob is produced by SerializeBlock and remains an opaque byte string here.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "tidepool/api/block_key.h"
#include "tidepool/api/status.h"

namespace tidepool::wire {

inline constexpr std::uint32_t kMagic = 0x54504e57U;  // ASCII "TPNW"
inline constexpr std::uint16_t kVersion = 1;
inline constexpr std::size_t kHeaderBytes = 32;
inline constexpr std::size_t kBlockKeyBytes = 16;
inline constexpr std::uint64_t kDefaultMaxFrameBytes = 256ULL * 1024ULL * 1024ULL;
inline constexpr std::uint32_t kDefaultMaxErrorMessageBytes = 4096;

enum class MessageKind : std::uint8_t {
    kRequest = 0x01,
    kResponse = 0x02,
};

enum class Opcode : std::uint8_t {
    kGet = 0x01,
    kPut = 0x02,
    kPing = 0x03,
};

// These values are protocol constants. They intentionally do not depend on
// the declaration order or underlying values of tidepool::StatusCode.
enum class WireStatus : std::uint16_t {
    kOk = 0,
    kNotImplemented = 1,
    kNotFound = 2,
    kInvalidArgument = 3,
    kAlreadyExists = 4,
    kIoError = 5,
    kNetworkError = 6,
    kUnavailable = 7,
    kOutOfCapacity = 8,
    kCorruption = 9,
    kInternal = 10,
};

struct FrameHeader {
    MessageKind kind = MessageKind::kRequest;
    Opcode opcode = Opcode::kPing;
    WireStatus status = WireStatus::kOk;
    std::uint32_t flags = 0;
    std::uint64_t request_id = 0;
    std::uint64_t payload_bytes = 0;
};

struct CodecLimits {
    std::uint64_t max_frame_bytes = kDefaultMaxFrameBytes;
    std::uint32_t max_error_message_bytes = kDefaultMaxErrorMessageBytes;
};

using EncodedHeader = std::array<std::uint8_t, kHeaderBytes>;
using EncodedBlockKey = std::array<std::uint8_t, kBlockKeyBytes>;
using ByteBuffer = std::vector<std::uint8_t>;

// Owns block_blob so callers cannot retain a view into a temporary network
// payload. The bytes are copied verbatim and are not parsed by this module.
struct DecodedPutRequest {
    BlockKey key;
    std::string block_blob;
};

// Validates that header + payload fits both limits.max_frame_bytes and size_t,
// without performing an overflowing addition. On success returns total frame
// bytes, including the fixed header.
Result<std::size_t> ValidateFrameSize(
    std::uint64_t payload_bytes, const CodecLimits& limits = {});

Result<EncodedHeader> EncodeHeader(
    const FrameHeader& header, const CodecLimits& limits = {});
Result<FrameHeader> DecodeHeader(
    const std::uint8_t* bytes, std::size_t size,
    const CodecLimits& limits = {});

Result<EncodedBlockKey> EncodeBlockKey(const BlockKey& key);
Result<BlockKey> DecodeBlockKey(const std::uint8_t* bytes, std::size_t size);

Result<ByteBuffer> EncodeGetRequest(const BlockKey& key);
Result<BlockKey> DecodeGetRequest(const std::uint8_t* payload,
                                  std::size_t size);

Result<ByteBuffer> EncodeGetSuccessPayload(
    std::string_view block_blob, const CodecLimits& limits = {});
Result<std::string> DecodeGetSuccessPayload(
    const std::uint8_t* payload, std::size_t size,
    const CodecLimits& limits = {});

Result<ByteBuffer> EncodePutRequest(
    const BlockKey& key, std::string_view block_blob,
    const CodecLimits& limits = {});
Result<DecodedPutRequest> DecodePutRequest(
    const std::uint8_t* payload, std::size_t size,
    const CodecLimits& limits = {});

Result<ByteBuffer> EncodePingRequest();
Status DecodePingRequest(const std::uint8_t* payload, std::size_t size);

Result<ByteBuffer> EncodeEmptySuccessPayload();
Status DecodeEmptySuccessPayload(const std::uint8_t* payload,
                                 std::size_t size);

Result<ByteBuffer> EncodeErrorPayload(
    std::string_view message, const CodecLimits& limits = {});
Result<std::string> DecodeErrorPayload(
    const std::uint8_t* payload, std::size_t size,
    const CodecLimits& limits = {});

// Validates the complete, contiguous payload against a decoded or constructed
// header. These functions also require payload size to equal payload_bytes.
Status ValidateRequest(const FrameHeader& header, const std::uint8_t* payload,
                       std::size_t size, const CodecLimits& limits = {});
Status ValidateResponse(const FrameHeader& header,
                        const std::uint8_t* payload, std::size_t size,
                        const CodecLimits& limits = {});

Result<WireStatus> ToWireStatus(StatusCode code);
Result<StatusCode> FromWireStatus(WireStatus status);

}  // namespace tidepool::wire
