#include "tidepool/rpc/tcp_connector.h"

#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "tidepool/api/block_codec.h"

namespace tidepool {
namespace {

const std::uint8_t* DataOrNull(const wire::ByteBuffer& bytes) noexcept {
    return bytes.empty() ? nullptr : bytes.data();
}

const std::uint8_t* DataOrNull(const std::string& bytes) noexcept {
    return bytes.empty()
               ? nullptr
               : reinterpret_cast<const std::uint8_t*>(bytes.data());
}

std::string_view ByteView(const wire::ByteBuffer& bytes) noexcept {
    if (bytes.empty()) return {};
    return std::string_view(reinterpret_cast<const char*>(bytes.data()),
                            bytes.size());
}

Status Capacity(std::string message) {
    return Status(StatusCode::kOutOfCapacity,
                  "TcpConnector: " + std::move(message));
}

Status ProtocolError(std::string message) {
    return Status::Corruption("TcpConnector: " + std::move(message));
}

Status ValidateBlockSize(const Block& block,
                         const wire::CodecLimits& limits) {
    constexpr std::uint64_t kEnvelopeBytes =
        static_cast<std::uint64_t>(wire::kBlockKeyBytes) +
        static_cast<std::uint64_t>(block_codec::kHeaderSize);

    if constexpr (sizeof(std::size_t) > sizeof(std::uint64_t)) {
        if (block.data.size() >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint64_t>::max())) {
            return Capacity("Block payload length does not fit uint64_t");
        }
    }
    const std::uint64_t data_bytes =
        static_cast<std::uint64_t>(block.data.size());
    if (data_bytes > std::numeric_limits<std::uint64_t>::max() -
                         kEnvelopeBytes) {
        return Capacity("Put payload length overflows uint64_t");
    }
    auto frame_size =
        wire::ValidateFrameSize(kEnvelopeBytes + data_bytes, limits);
    return frame_size.ok() ? Status::Ok() : frame_size.status();
}

}  // namespace

TcpConnector::TcpConnector(TcpEndpoint endpoint, TcpConnectorOptions options)
    : endpoint_(std::move(endpoint)), options_(options) {}

TcpConnector::~TcpConnector() { Close(); }

Status TcpConnector::ValidateConfigurationLocked() const {
    if (endpoint_.host.empty()) {
        return Status::InvalidArgument("TcpConnector: endpoint host is empty");
    }
    if (endpoint_.port == 0) {
        return Status::InvalidArgument("TcpConnector: endpoint port is zero");
    }
    if (options_.connection.connect_timeout.count() < 0 ||
        options_.connection.read_timeout.count() < 0 ||
        options_.connection.write_timeout.count() < 0) {
        return Status::InvalidArgument(
            "TcpConnector: connection timeouts must be non-negative");
    }
    if (options_.codec_limits.max_frame_bytes < wire::kHeaderBytes) {
        return Status::InvalidArgument(
            "TcpConnector: max_frame_bytes is smaller than the Wire header");
    }
    return Status::Ok();
}

Status TcpConnector::EnsureConnectedLocked() {
    if (connection_ != nullptr && connection_->IsOpen()) {
        return Status::Ok();
    }
    DropConnectionLocked();

    auto connected = TcpConnection::Connect(
        endpoint_.host, endpoint_.port, options_.connection);
    if (!connected.ok()) return connected.status();

    try {
        connection_ =
            std::make_unique<TcpConnection>(std::move(connected.value()));
    } catch (const std::bad_alloc&) {
        return Capacity("connection object allocation failed");
    } catch (const std::length_error&) {
        return Capacity("connection object allocation exceeded a limit");
    }
    return Status::Ok();
}

void TcpConnector::DropConnectionLocked() noexcept {
    if (connection_ == nullptr) return;
    connection_->Close();
    connection_.reset();
}

std::uint64_t TcpConnector::AllocateRequestIdLocked() noexcept {
    const std::uint64_t request_id = next_request_id_;
    next_request_id_ =
        request_id == std::numeric_limits<std::uint64_t>::max()
            ? 1
            : request_id + 1;
    return request_id;
}

Result<WireFrame> TcpConnector::ExecuteLocked(
    wire::Opcode opcode, const wire::ByteBuffer& payload) {
    const std::uint64_t request_id = AllocateRequestIdLocked();
    const wire::FrameHeader request{
        wire::MessageKind::kRequest,
        opcode,
        wire::WireStatus::kOk,
        0,
        request_id,
        static_cast<std::uint64_t>(payload.size()),
    };

    if (Status status = wire::ValidateRequest(
            request, DataOrNull(payload), payload.size(),
            options_.codec_limits);
        !status.ok()) {
        return status;
    }
    if (Status status = EnsureConnectedLocked(); !status.ok()) return status;

    try {
        if (Status status = WriteWireFrame(
                *connection_, request, ByteView(payload),
                options_.codec_limits);
            !status.ok()) {
            DropConnectionLocked();
            return status;
        }

        auto response =
            ReadWireFrame(*connection_, options_.codec_limits);
        if (!response.ok()) {
            DropConnectionLocked();
            return response.status();
        }

        if (response.value().header.kind !=
            wire::MessageKind::kResponse) {
            DropConnectionLocked();
            return ProtocolError("peer sent a request where a response was expected");
        }
        if (response.value().header.opcode != opcode) {
            DropConnectionLocked();
            return ProtocolError("response opcode does not match request");
        }
        if (response.value().header.request_id != request_id) {
            DropConnectionLocked();
            return ProtocolError("response request_id does not match request");
        }

        const Status validation = wire::ValidateResponse(
            response.value().header, DataOrNull(response.value().payload),
            response.value().payload.size(), options_.codec_limits);
        if (!validation.ok()) {
            // A fully consumed frame remains synchronized when only a local
            // allocation failed during semantic validation.
            if (validation.code() != StatusCode::kOutOfCapacity) {
                DropConnectionLocked();
            }
            return validation;
        }
        return std::move(response.value());
    } catch (const std::bad_alloc&) {
        DropConnectionLocked();
        return Capacity("RPC allocation failed after network I/O began");
    } catch (const std::length_error&) {
        DropConnectionLocked();
        return Capacity("RPC allocation exceeded a limit after network I/O began");
    }
}

Result<TcpConnector::DecodedRemoteError>
TcpConnector::DecodeRemoteErrorLocked(const WireFrame& response) {
    auto message = wire::DecodeErrorPayload(
        DataOrNull(response.payload), response.payload.size(),
        options_.codec_limits);
    if (!message.ok()) return message.status();

    auto code = wire::FromWireStatus(response.header.status);
    if (!code.ok()) return code.status();
    if (code.value() == StatusCode::kOk) {
        return ProtocolError("remote error decoder received kOk");
    }
    return DecodedRemoteError{code.value(), std::move(message.value())};
}

Result<Block> TcpConnector::Get(const BlockKey& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        if (Status status = ValidateConfigurationLocked(); !status.ok()) {
            return status;
        }

        auto payload = wire::EncodeGetRequest(key);
        if (!payload.ok()) return payload.status();
        auto response = ExecuteLocked(wire::Opcode::kGet, payload.value());
        if (!response.ok()) return response.status();

        if (response.value().header.status != wire::WireStatus::kOk) {
            auto remote_error = DecodeRemoteErrorLocked(response.value());
            if (!remote_error.ok()) {
                if (remote_error.status().code() !=
                    StatusCode::kOutOfCapacity) {
                    DropConnectionLocked();
                }
                return remote_error.status();
            }
            return Status(remote_error.value().code,
                          std::move(remote_error.value().message));
        }

        auto block_blob = wire::DecodeGetSuccessPayload(
            DataOrNull(response.value().payload),
            response.value().payload.size(), options_.codec_limits);
        if (!block_blob.ok()) {
            if (block_blob.status().code() != StatusCode::kOutOfCapacity) {
                DropConnectionLocked();
            }
            return block_blob.status();
        }

        Block block;
        std::size_t payload_bytes = 0;
        std::size_t payload_offset = 0;
        const Status decoded =
            DeserializeHeader(block_blob.value(), &block.metadata,
                              &payload_bytes, &payload_offset);
        if (!decoded.ok()) {
            DropConnectionLocked();
            return ProtocolError("invalid Block blob in Get response: " +
                                 decoded.message());
        }

        const auto* bytes = reinterpret_cast<const std::uint8_t*>(
            block_blob.value().data());
        block.data.assign(bytes + payload_offset,
                          bytes + payload_offset + payload_bytes);
        return block;
    } catch (const std::bad_alloc&) {
        return Capacity("Get Block allocation failed");
    } catch (const std::length_error&) {
        return Capacity("Get Block exceeds vector capacity");
    }
}

Status TcpConnector::Put(const BlockKey& key, const Block& block) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        if (Status status = ValidateConfigurationLocked(); !status.ok()) {
            return status;
        }
        if (Status status = ValidateBlockSize(block, options_.codec_limits);
            !status.ok()) {
            return status;
        }

        std::string block_blob;
        block_blob = SerializeBlock(block);

        auto payload =
            wire::EncodePutRequest(key, block_blob, options_.codec_limits);
        if (!payload.ok()) return payload.status();
        auto response = ExecuteLocked(wire::Opcode::kPut, payload.value());
        if (!response.ok()) return response.status();

        if (response.value().header.status != wire::WireStatus::kOk) {
            auto remote_error = DecodeRemoteErrorLocked(response.value());
            if (!remote_error.ok()) {
                if (remote_error.status().code() !=
                    StatusCode::kOutOfCapacity) {
                    DropConnectionLocked();
                }
                return remote_error.status();
            }
            return Status(remote_error.value().code,
                          std::move(remote_error.value().message));
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Capacity("Put allocation failed");
    } catch (const std::length_error&) {
        return Capacity("Put allocation exceeded a limit");
    }
}

Status TcpConnector::Ping() {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        if (Status status = ValidateConfigurationLocked(); !status.ok()) {
            return status;
        }

        auto payload = wire::EncodePingRequest();
        if (!payload.ok()) return payload.status();
        auto response = ExecuteLocked(wire::Opcode::kPing, payload.value());
        if (!response.ok()) return response.status();

        if (response.value().header.status != wire::WireStatus::kOk) {
            auto remote_error = DecodeRemoteErrorLocked(response.value());
            if (!remote_error.ok()) {
                if (remote_error.status().code() !=
                    StatusCode::kOutOfCapacity) {
                    DropConnectionLocked();
                }
                return remote_error.status();
            }
            return Status(remote_error.value().code,
                          std::move(remote_error.value().message));
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Capacity("Ping allocation failed");
    } catch (const std::length_error&) {
        return Capacity("Ping allocation exceeded a limit");
    }
}

void TcpConnector::Close() {
    std::lock_guard<std::mutex> lock(mutex_);
    DropConnectionLocked();
}

bool TcpConnector::IsConnected() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return connection_ != nullptr && connection_->IsOpen();
}

}  // namespace tidepool
