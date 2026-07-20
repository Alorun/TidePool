// wire_framing.h — transport of complete Wire Protocol v1 frames.
#pragma once

#include <string>
#include <string_view>

#include "tidepool/api/status.h"
#include "tidepool/protocol/wire_codec.h"
#include "tidepool/transport/tcp_connection.h"

namespace tidepool {

struct WireFrame {
    wire::FrameHeader header;
    std::string payload;  // owns the exact payload bytes
};

Result<WireFrame> ReadWireFrame(
    TcpConnection& connection, const wire::CodecLimits& limits = {});

Status WriteWireFrame(
    TcpConnection& connection, const wire::FrameHeader& header,
    std::string_view payload, const wire::CodecLimits& limits = {});

}  // namespace tidepool
