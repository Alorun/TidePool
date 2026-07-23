// tcp_connector.h — serialized RPC client for one remote TidePool node.
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "tidepool/api/block.h"
#include "tidepool/api/block_key.h"
#include "tidepool/api/status.h"
#include "tidepool/protocol/wire_codec.h"
#include "tidepool/transport/tcp_connection.h"
#include "tidepool/transport/wire_framing.h"

namespace tidepool {

struct TcpEndpoint {
    std::string host;
    std::uint16_t port = 0;
};

struct TcpConnectorOptions {
    TcpConnectionOptions connection;
    wire::CodecLimits codec_limits;
};

// Owns at most one persistent connection to one endpoint. Public RPCs and
// Close are serialized by one mutex; there is deliberately no multiplexing,
// background reconnect, or replay of a failed request. Destruction must not
// race with a public call; there is no asynchronous cancellation API.
class TcpConnector {
public:
    explicit TcpConnector(TcpEndpoint endpoint,
                          TcpConnectorOptions options = {});
    ~TcpConnector();

    TcpConnector(const TcpConnector&) = delete;
    TcpConnector& operator=(const TcpConnector&) = delete;
    TcpConnector(TcpConnector&&) = delete;
    TcpConnector& operator=(TcpConnector&&) = delete;

    const TcpEndpoint& endpoint() const noexcept { return endpoint_; }

    Result<Block> Get(const BlockKey& key);
    Status Put(const BlockKey& key, const Block& block);
    Status Ping();

    // Waits for an in-progress RPC, then drops the connection. The connector
    // remains usable: the next new RPC may connect again. Request IDs do not
    // reset.
    void Close();
    bool IsConnected() const;

private:
    struct DecodedRemoteError {
        StatusCode code;
        std::string message;
    };

    Status ValidateConfigurationLocked() const;
    Status EnsureConnectedLocked();
    void DropConnectionLocked() noexcept;
    std::uint64_t AllocateRequestIdLocked() noexcept;

    Result<WireFrame> ExecuteLocked(wire::Opcode opcode,
                                    const wire::ByteBuffer& payload);
    Result<DecodedRemoteError> DecodeRemoteErrorLocked(
        const WireFrame& response);

    TcpEndpoint endpoint_;
    TcpConnectorOptions options_;

    mutable std::mutex mutex_;
    std::unique_ptr<TcpConnection> connection_;
    std::uint64_t next_request_id_ = 1;

    friend struct TcpConnectorTestAccess;
};

}  // namespace tidepool
