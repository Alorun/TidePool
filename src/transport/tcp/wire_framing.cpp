#include "tidepool/transport/wire_framing.h"

#include <array>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>

namespace tidepool {

Result<WireFrame> ReadWireFrame(TcpConnection& connection,
                                const wire::CodecLimits& limits) {
    std::array<std::uint8_t, wire::kHeaderBytes> encoded_header{};
    if (Status status =
            connection.ReadExact(encoded_header.data(), encoded_header.size());
        !status.ok()) {
        return status;
    }

    auto decoded_header = wire::DecodeHeader(
        encoded_header.data(), encoded_header.size(), limits);
    if (!decoded_header.ok()) {
        connection.Shutdown();
        return decoded_header.status();
    }

    auto frame_size =
        wire::ValidateFrameSize(decoded_header.value().payload_bytes, limits);
    if (!frame_size.ok()) {
        connection.Shutdown();
        return frame_size.status();
    }
    const std::size_t payload_size = frame_size.value() - wire::kHeaderBytes;

    std::string payload;
    try {
        payload.resize(payload_size);
    } catch (const std::bad_alloc&) {
        connection.Shutdown();
        return Status(StatusCode::kOutOfCapacity,
                      "ReadWireFrame: payload allocation failed");
    } catch (const std::length_error&) {
        connection.Shutdown();
        return Status(StatusCode::kOutOfCapacity,
                      "ReadWireFrame: payload exceeds string capacity");
    }

    if (payload_size != 0) {
        if (Status status = connection.ReadExact(payload.data(), payload_size);
            !status.ok()) {
            return status;
        }
    }
    return WireFrame{decoded_header.value(), std::move(payload)};
}

Status WriteWireFrame(TcpConnection& connection,
                      const wire::FrameHeader& header,
                      std::string_view payload,
                      const wire::CodecLimits& limits) {
    if constexpr (sizeof(std::size_t) > sizeof(std::uint64_t)) {
        if (payload.size() >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint64_t>::max())) {
            return Status::InvalidArgument(
                "WriteWireFrame: payload size does not fit uint64_t");
        }
    }
    if (header.payload_bytes != static_cast<std::uint64_t>(payload.size())) {
        return Status::InvalidArgument(
            "WriteWireFrame: header payload_bytes does not match payload size");
    }

    auto encoded_header = wire::EncodeHeader(header, limits);
    if (!encoded_header.ok()) return encoded_header.status();

    if (Status status = connection.WriteExact(
            encoded_header.value().data(), encoded_header.value().size());
        !status.ok()) {
        return status;
    }
    if (!payload.empty()) {
        return connection.WriteExact(payload.data(), payload.size());
    }
    return Status::Ok();
}

}  // namespace tidepool
