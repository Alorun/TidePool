#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "tidepool/api/block_codec.h"
#include "tidepool/protocol/wire_codec.h"
#include "tidepool/rpc/tcp_connector.h"
#include "tidepool/transport/tcp_connection.h"
#include "tidepool/transport/wire_framing.h"

using namespace tidepool;
using namespace tidepool::wire;
using namespace std::chrono_literals;

namespace tidepool {

struct TcpConnectorTestAccess {
    static void SetNextRequestId(TcpConnector* connector,
                                 std::uint64_t request_id) {
        std::lock_guard<std::mutex> lock(connector->mutex_);
        connector->next_request_id_ = request_id;
    }
};

}  // namespace tidepool

namespace {

std::atomic<int> g_checks{0};

#define CHECK(cond, msg)                                                       \
    do {                                                                       \
        g_checks.fetch_add(1, std::memory_order_relaxed);                      \
        if (!(cond)) {                                                         \
            std::fprintf(stderr, "CHECK failed: %s (%s:%d)\n", (msg),         \
                         __FILE__, __LINE__);                                  \
            std::abort();                                                      \
        }                                                                      \
    } while (0)

const std::uint8_t* DataOrNull(const std::string& bytes) {
    return bytes.empty()
               ? nullptr
               : reinterpret_cast<const std::uint8_t*>(bytes.data());
}

std::string_view ByteView(const ByteBuffer& bytes) {
    if (bytes.empty()) return {};
    return std::string_view(reinterpret_cast<const char*>(bytes.data()),
                            bytes.size());
}

struct Listener {
    int fd = -1;
    std::uint16_t port = 0;

    Listener() = default;
    ~Listener() {
        if (fd >= 0) ::close(fd);
    }
    Listener(const Listener&) = delete;
    Listener& operator=(const Listener&) = delete;
    Listener(Listener&& other) noexcept : fd(other.fd), port(other.port) {
        other.fd = -1;
        other.port = 0;
    }
    Listener& operator=(Listener&& other) noexcept {
        if (this == &other) return *this;
        if (fd >= 0) ::close(fd);
        fd = other.fd;
        port = other.port;
        other.fd = -1;
        other.port = 0;
        return *this;
    }
};

Listener MakeListener() {
    Listener listener;
    listener.fd =
        ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
    CHECK(listener.fd >= 0, "scripted peer creates IPv4 listener");

    int one = 1;
    CHECK(::setsockopt(listener.fd, SOL_SOCKET, SO_REUSEADDR, &one,
                       sizeof(one)) == 0,
          "scripted peer enables SO_REUSEADDR");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    CHECK(::bind(listener.fd, reinterpret_cast<sockaddr*>(&address),
                 sizeof(address)) == 0,
          "scripted peer binds loopback port zero");
    socklen_t address_size = sizeof(address);
    CHECK(::getsockname(listener.fd, reinterpret_cast<sockaddr*>(&address),
                        &address_size) == 0,
          "scripted peer discovers ephemeral port");
    listener.port = ntohs(address.sin_port);
    CHECK(::listen(listener.fd, 16) == 0,
          "scripted peer listens on ephemeral port");
    return listener;
}

TcpConnectionOptions PeerConnectionOptions() {
    TcpConnectionOptions options;
    options.connect_timeout = 1s;
    options.read_timeout = 2s;
    options.write_timeout = 2s;
    return options;
}

TcpConnectorOptions ClientOptions(
    std::chrono::milliseconds read_timeout = 750ms,
    std::chrono::milliseconds write_timeout = 750ms) {
    TcpConnectorOptions options;
    options.connection.connect_timeout = 750ms;
    options.connection.read_timeout = read_timeout;
    options.connection.write_timeout = write_timeout;
    return options;
}

TcpEndpoint Endpoint(const Listener& listener) {
    return TcpEndpoint{"127.0.0.1", listener.port};
}

int AcceptRaw(int listener_fd) {
    int accepted = -1;
    do {
#if defined(SOCK_CLOEXEC)
        accepted = ::accept4(listener_fd, nullptr, nullptr, SOCK_CLOEXEC);
#else
        accepted = ::accept(listener_fd, nullptr, nullptr);
#endif
    } while (accepted < 0 && errno == EINTR);
    CHECK(accepted >= 0, "scripted peer accepts client connection");
    return accepted;
}

TcpConnection AcceptConnection(int listener_fd, int* accepted_fd_out = nullptr) {
    const int accepted = AcceptRaw(listener_fd);
    if (accepted_fd_out != nullptr) *accepted_fd_out = accepted;

    auto connection = TcpConnection::AdoptConnectedSocket(
        accepted, PeerConnectionOptions());
    CHECK(connection.ok(), "scripted peer adopts accepted socket");
    return std::move(connection.value());
}

bool ReceiveAll(int fd, void* output, std::size_t bytes) {
    auto* destination = static_cast<std::uint8_t*>(output);
    std::size_t completed = 0;
    while (completed < bytes) {
        const ssize_t result =
            ::recv(fd, destination + completed, bytes - completed, 0);
        if (result > 0) {
            completed += static_cast<std::size_t>(result);
            continue;
        }
        if (result < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}

WireFrame ReadRequest(TcpConnection& connection, Opcode expected_opcode) {
    auto frame = ReadWireFrame(connection);
    CHECK(frame.ok(), "scripted peer reads complete request frame");
    CHECK(frame.value().header.kind == MessageKind::kRequest,
          "client sends Request kind");
    CHECK(frame.value().header.opcode == expected_opcode,
          "client sends expected opcode");
    CHECK(frame.value().header.status == WireStatus::kOk &&
              frame.value().header.flags == 0,
          "client request uses kOk and zero flags");
    CHECK(frame.value().header.request_id != 0,
          "client never sends request_id zero");
    CHECK(ValidateRequest(frame.value().header,
                          DataOrNull(frame.value().payload),
                          frame.value().payload.size())
              .ok(),
          "client request passes protocol semantic validation");
    return std::move(frame.value());
}

Status WriteResponse(TcpConnection& connection, const WireFrame& request,
                     WireStatus status, const ByteBuffer& payload,
                     Opcode opcode) {
    const FrameHeader header{
        MessageKind::kResponse,
        opcode,
        status,
        0,
        request.header.request_id,
        static_cast<std::uint64_t>(payload.size()),
    };
    return WriteWireFrame(connection, header, ByteView(payload));
}

Status WriteResponse(TcpConnection& connection, const WireFrame& request,
                     WireStatus status, const ByteBuffer& payload) {
    return WriteResponse(connection, request, status, payload,
                         request.header.opcode);
}

Status WriteEmptySuccess(TcpConnection& connection,
                         const WireFrame& request) {
    const auto payload = EncodeEmptySuccessPayload();
    CHECK(payload.ok(), "scripted peer encodes empty success");
    return WriteResponse(connection, request, WireStatus::kOk,
                         payload.value());
}

Status WriteError(TcpConnection& connection, const WireFrame& request,
                  WireStatus status, std::string_view message) {
    auto payload = EncodeErrorPayload(message);
    CHECK(payload.ok(), "scripted peer encodes error payload");
    return WriteResponse(connection, request, status, payload.value());
}

bool WriteRaw(TcpConnection& connection, const EncodedHeader& header,
              std::string_view payload = {}) {
    if (!connection.WriteExact(header.data(), header.size()).ok()) return false;
    return payload.empty() ||
           connection.WriteExact(payload.data(), payload.size()).ok();
}

void PutU16(EncodedHeader* header, std::size_t offset, std::uint16_t value) {
    (*header)[offset] = static_cast<std::uint8_t>(value >> 8U);
    (*header)[offset + 1] = static_cast<std::uint8_t>(value);
}

void PutU32(EncodedHeader* header, std::size_t offset, std::uint32_t value) {
    for (std::size_t i = 0; i < 4; ++i) {
        (*header)[offset + i] = static_cast<std::uint8_t>(
            value >> static_cast<unsigned>((3U - i) * 8U));
    }
}

void PutU64(EncodedHeader* header, std::size_t offset, std::uint64_t value) {
    for (std::size_t i = 0; i < 8; ++i) {
        (*header)[offset + i] = static_cast<std::uint8_t>(
            value >> static_cast<unsigned>((7U - i) * 8U));
    }
}

BlockKey Key(std::uint64_t hash = 0x0102030405060708ULL) {
    return BlockKey{hash, 0x11121314U, 0x21222324U};
}

Block MakeBlock(std::uint8_t marker = 0x31) {
    Block block;
    block.metadata.num_tokens = 17;
    block.metadata.num_layers = 32;
    block.metadata.dtype_size = 2;
    block.metadata.kv_heads = 8;
    block.metadata.created_unix_ns = 0x0102030405060708ULL;
    block.metadata.model_fingerprint = 0x21222324U;
    block.data = {0x00, 0x7f, 0x80, 0xff, marker, 0x00, 0x42};
    return block;
}

bool SameBlock(const Block& left, const Block& right) {
    return left.metadata.num_tokens == right.metadata.num_tokens &&
           left.metadata.num_layers == right.metadata.num_layers &&
           left.metadata.dtype_size == right.metadata.dtype_size &&
           left.metadata.kv_heads == right.metadata.kv_heads &&
           left.metadata.created_unix_ns ==
               right.metadata.created_unix_ns &&
           left.metadata.model_fingerprint ==
               right.metadata.model_fingerprint &&
           left.data == right.data;
}

ByteBuffer BlockPayload(const Block& block) {
    auto payload = EncodeGetSuccessPayload(SerializeBlock(block));
    CHECK(payload.ok(), "scripted peer encodes real Block success payload");
    return std::move(payload.value());
}

void WaitFor(const std::atomic<bool>& flag) {
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (!flag.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    CHECK(flag.load(std::memory_order_acquire),
          "test synchronization completes before deadline");
}

void TestConfigurationValidation() {
    TcpConnector empty_host(TcpEndpoint{"", 1});
    CHECK(empty_host.Ping().code() == StatusCode::kInvalidArgument,
          "empty endpoint host is rejected before connect");

    TcpConnector zero_port(TcpEndpoint{"127.0.0.1", 0});
    CHECK(zero_port.Ping().code() == StatusCode::kInvalidArgument,
          "zero endpoint port is rejected before connect");

    TcpConnectorOptions invalid_timeout;
    invalid_timeout.connection.read_timeout = -1ms;
    TcpConnector bad_timeout(TcpEndpoint{"127.0.0.1", 1},
                             invalid_timeout);
    CHECK(bad_timeout.Ping().code() == StatusCode::kInvalidArgument,
          "negative connector timeout is rejected");

    TcpConnectorOptions invalid_frame;
    invalid_frame.codec_limits.max_frame_bytes = wire::kHeaderBytes - 1;
    TcpConnector bad_frame(TcpEndpoint{"127.0.0.1", 1}, invalid_frame);
    CHECK(bad_frame.Ping().code() == StatusCode::kInvalidArgument,
          "connector rejects frame limit below fixed Header");
    CHECK(!bad_frame.IsConnected(),
          "local configuration failures do not create a connection");
}

void TestOversizedPutFailsBeforeConnect() {
    Listener listener = MakeListener();
    const Block block = MakeBlock();
    TcpConnectorOptions options = ClientOptions();
    options.codec_limits.max_frame_bytes =
        wire::kHeaderBytes + wire::kBlockKeyBytes +
        block_codec::kHeaderSize + block.data.size() - 1;

    std::uint64_t observed_id = 0;
    std::thread peer([&] {
        TcpConnection connection = AcceptConnection(listener.fd);
        WireFrame request = ReadRequest(connection, Opcode::kPing);
        observed_id = request.header.request_id;
        CHECK(WriteEmptySuccess(connection, request).ok(),
              "Ping after local oversized Put succeeds");
    });

    TcpConnector connector(Endpoint(listener), options);
    Status put = connector.Put(Key(), block);
    CHECK(!put.ok(), "Put exceeding configured frame is rejected locally");
    CHECK(!connector.IsConnected(),
          "oversized local Put does not establish or damage connection");
    CHECK(connector.Ping().ok(),
          "healthy connection can be established after local Put failure");
    connector.Close();
    peer.join();
    CHECK(observed_id == 1,
          "local serialization/size failure does not consume request ID");
}

void TestPersistentMixedRpcAndRequestIds() {
    Listener listener = MakeListener();
    const Block block = MakeBlock();
    const BlockKey key = Key();
    std::vector<std::uint64_t> request_ids;
    std::atomic<int> accepted{0};

    std::thread peer([&] {
        TcpConnection connection = AcceptConnection(listener.fd);
        accepted.fetch_add(1);

        WireFrame first = ReadRequest(connection, Opcode::kPing);
        request_ids.push_back(first.header.request_id);
        CHECK(first.payload.empty(), "Ping request payload is empty");
        CHECK(WriteEmptySuccess(connection, first).ok(),
              "first Ping response succeeds");

        WireFrame second = ReadRequest(connection, Opcode::kPing);
        request_ids.push_back(second.header.request_id);
        CHECK(WriteEmptySuccess(connection, second).ok(),
              "second Ping response succeeds on persistent connection");

        WireFrame third = ReadRequest(connection, Opcode::kGet);
        request_ids.push_back(third.header.request_id);
        auto decoded_key =
            DecodeGetRequest(DataOrNull(third.payload), third.payload.size());
        CHECK(decoded_key.ok() && decoded_key.value() == key,
              "Get request carries exact network BlockKey");
        ByteBuffer get_payload = BlockPayload(block);
        CHECK(WriteResponse(connection, third, WireStatus::kOk,
                            get_payload)
                  .ok(),
              "mixed Get response succeeds");

        WireFrame fourth = ReadRequest(connection, Opcode::kPut);
        request_ids.push_back(fourth.header.request_id);
        auto decoded_put =
            DecodePutRequest(DataOrNull(fourth.payload), fourth.payload.size());
        CHECK(decoded_put.ok() && decoded_put.value().key == key &&
                  decoded_put.value().block_blob == SerializeBlock(block),
              "Put preserves key and canonical Block blob");
        CHECK(WriteEmptySuccess(connection, fourth).ok(),
              "mixed Put response succeeds");
    });

    TcpConnector connector(Endpoint(listener));
    CHECK(!connector.IsConnected(), "new TcpConnector starts disconnected");
    CHECK(connector.Ping().ok(), "first Ping establishes connection");
    CHECK(connector.IsConnected(), "successful Ping leaves connection healthy");
    CHECK(connector.Ping().ok(), "second Ping reuses connection");
    auto get = connector.Get(key);
    CHECK(get.ok() && SameBlock(get.value(), block),
          "mixed Get returns owned decoded Block");
    CHECK(connector.Put(key, block).ok(), "mixed Put succeeds");
    connector.Close();
    peer.join();

    CHECK(accepted.load() == 1,
          "mixed RPCs use exactly one persistent TCP connection");
    CHECK(request_ids == std::vector<std::uint64_t>({1, 2, 3, 4}),
          "Ping/Get/Put share one monotonically increasing ID sequence");
}

void TestGetSuccessNotFoundAndReuse() {
    Listener listener = MakeListener();
    const Block block = MakeBlock(0x55);
    const BlockKey first_key = Key(0x1111111111111111ULL);
    const BlockKey missing_key = Key(0x2222222222222222ULL);
    std::atomic<int> accepted{0};

    std::thread peer([&] {
        TcpConnection connection = AcceptConnection(listener.fd);
        accepted.fetch_add(1);

        WireFrame first = ReadRequest(connection, Opcode::kGet);
        auto first_decoded =
            DecodeGetRequest(DataOrNull(first.payload), first.payload.size());
        CHECK(first_decoded.ok() && first_decoded.value() == first_key,
              "successful Get transmits exact BlockKey");
        ByteBuffer payload = BlockPayload(block);
        CHECK(WriteResponse(connection, first, WireStatus::kOk, payload).ok(),
              "scripted Get success writes real SerializeBlock blob");

        WireFrame second = ReadRequest(connection, Opcode::kGet);
        auto second_decoded =
            DecodeGetRequest(DataOrNull(second.payload), second.payload.size());
        CHECK(second_decoded.ok() && second_decoded.value() == missing_key,
              "missing Get transmits exact BlockKey");
        CHECK(WriteError(connection, second, WireStatus::kNotFound,
                         "remote block missing")
                  .ok(),
              "scripted peer writes NotFound ErrorPayload");

        WireFrame third = ReadRequest(connection, Opcode::kPing);
        CHECK(WriteEmptySuccess(connection, third).ok(),
              "Ping succeeds after legal NotFound");
    });

    TcpConnector connector(Endpoint(listener));
    auto get = connector.Get(first_key);
    CHECK(get.ok() && SameBlock(get.value(), block),
          "Get decodes metadata and binary NUL payload");
    Block owned = std::move(get.value());
    CHECK(owned.data == block.data,
          "Get result remains owned after network frame destruction");

    auto missing = connector.Get(missing_key);
    CHECK(!missing.ok() &&
              missing.status().code() == StatusCode::kNotFound &&
              missing.status().message() == "remote block missing",
          "legal remote NotFound preserves code and message");
    CHECK(connector.IsConnected(),
          "legal NotFound does not invalidate persistent connection");
    CHECK(connector.Ping().ok(), "connection remains reusable after NotFound");
    connector.Close();
    peer.join();
    CHECK(accepted.load() == 1,
          "Get success, NotFound, and Ping share one connection");
}

void TestPutSuccessAndBusinessErrors() {
    Listener listener = MakeListener();
    const Block block = MakeBlock(0x66);
    const BlockKey key = Key(0x3333333333333333ULL);
    const std::string expected_blob = SerializeBlock(block);
    std::atomic<int> accepted{0};

    std::thread peer([&] {
        TcpConnection connection = AcceptConnection(listener.fd);
        accepted.fetch_add(1);

        WireFrame first = ReadRequest(connection, Opcode::kPut);
        auto put =
            DecodePutRequest(DataOrNull(first.payload), first.payload.size());
        CHECK(put.ok() && put.value().key == key &&
                  put.value().block_blob == expected_blob,
              "Put sends BlockKey followed by byte-identical SerializeBlock");
        CHECK(WriteEmptySuccess(connection, first).ok(),
              "Put success has empty payload");

        WireFrame second = ReadRequest(connection, Opcode::kPut);
        CHECK(WriteError(connection, second, WireStatus::kOutOfCapacity,
                         u8"远端容量不足")
                  .ok(),
              "peer sends UTF-8 OutOfCapacity");

        WireFrame third = ReadRequest(connection, Opcode::kPut);
        CHECK(WriteError(connection, third, WireStatus::kInvalidArgument,
                         "remote rejected metadata")
                  .ok(),
              "peer sends InvalidArgument");

        WireFrame fourth = ReadRequest(connection, Opcode::kPing);
        CHECK(WriteEmptySuccess(connection, fourth).ok(),
              "connection survives legal Put business errors");
    });

    TcpConnector connector(Endpoint(listener));
    CHECK(connector.Put(key, block).ok(), "Put success returns OK");
    Status capacity = connector.Put(key, block);
    CHECK(capacity.code() == StatusCode::kOutOfCapacity &&
              capacity.message() == u8"远端容量不足",
          "Put maps OutOfCapacity and preserves UTF-8 message");
    Status invalid = connector.Put(key, block);
    CHECK(invalid.code() == StatusCode::kInvalidArgument &&
              invalid.message() == "remote rejected metadata",
          "Put maps InvalidArgument explicitly");
    CHECK(connector.IsConnected(),
          "legal Put business errors preserve connection");
    CHECK(connector.Ping().ok(),
          "same connection remains usable after Put errors");
    connector.Close();
    peer.join();
    CHECK(accepted.load() == 1,
          "Put success and business errors reuse one connection");
}

void RunCorruptBlockCase(std::string corrupt_blob,
                         const char* case_message) {
    Listener listener = MakeListener();
    const BlockKey key = Key();
    std::vector<std::uint64_t> ids;
    std::atomic<int> accepted{0};

    std::thread peer([&] {
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            accepted.fetch_add(1);
            WireFrame request = ReadRequest(connection, Opcode::kGet);
            ids.push_back(request.header.request_id);
            auto payload = EncodeGetSuccessPayload(corrupt_blob);
            CHECK(payload.ok(), "opaque corrupt Block blob fits Wire payload");
            CHECK(WriteResponse(connection, request, WireStatus::kOk,
                                payload.value())
                      .ok(),
                  "peer sends structurally valid outer Get response");
        }
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            accepted.fetch_add(1);
            WireFrame request = ReadRequest(connection, Opcode::kPing);
            ids.push_back(request.header.request_id);
            CHECK(WriteEmptySuccess(connection, request).ok(),
                  "new connection serves Ping after corrupt Block");
        }
    });

    TcpConnector connector(Endpoint(listener));
    auto get = connector.Get(key);
    CHECK(!get.ok() && get.status().code() == StatusCode::kCorruption,
          case_message);
    CHECK(!connector.IsConnected(),
          "corrupt Get Block response drops connection");
    CHECK(connector.Ping().ok(),
          "next independent request reconnects after corrupt Block");
    connector.Close();
    peer.join();
    CHECK(accepted.load() == 2,
          "corrupt Block causes exactly one replacement connection");
    CHECK(ids == std::vector<std::uint64_t>({1, 2}),
          "reconnect after corrupt Block does not reset request IDs");
}

void TestCorruptGetBlocks() {
    const std::string valid = SerializeBlock(MakeBlock());

    std::string bad_magic = valid;
    bad_magic[0] ^= 0x7f;
    RunCorruptBlockCase(std::move(bad_magic),
                        "Get rejects bad Block magic as corruption");

    std::string bad_length = valid;
    bad_length[28] = static_cast<char>(
        static_cast<unsigned char>(bad_length[28]) + 1U);
    RunCorruptBlockCase(std::move(bad_length),
                        "Get rejects inconsistent Block payload length");

    std::string truncated = valid;
    truncated.pop_back();
    RunCorruptBlockCase(std::move(truncated),
                        "Get rejects truncated Block blob");
}

enum class MatchError {
    kRequestKind,
    kWrongOpcode,
    kWrongRequestId,
    kZeroRequestId,
    kFlags,
    kUnknownStatus,
};

void RunMatchError(MatchError error) {
    Listener listener = MakeListener();
    std::vector<std::uint64_t> ids;
    std::atomic<int> accepted{0};

    std::thread peer([&] {
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            accepted.fetch_add(1);
            WireFrame request = ReadRequest(connection, Opcode::kPing);
            ids.push_back(request.header.request_id);

            FrameHeader response{
                MessageKind::kResponse, Opcode::kPing, WireStatus::kOk, 0,
                request.header.request_id, 0};
            if (error == MatchError::kRequestKind) {
                response.kind = MessageKind::kRequest;
            } else if (error == MatchError::kWrongOpcode) {
                response.opcode = Opcode::kPut;
            } else if (error == MatchError::kWrongRequestId) {
                ++response.request_id;
            } else if (error == MatchError::kZeroRequestId) {
                response.request_id = 0;
            }

            auto encoded = EncodeHeader(response);
            CHECK(encoded.ok(), "baseline mismatched response Header encodes");
            if (error == MatchError::kFlags) {
                PutU32(&encoded.value(), 12, 1);
            } else if (error == MatchError::kUnknownStatus) {
                PutU16(&encoded.value(), 10, 0xffffU);
            }
            CHECK(WriteRaw(connection, encoded.value()),
                  "peer writes mismatched or malformed response Header");
        }
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            accepted.fetch_add(1);
            WireFrame request = ReadRequest(connection, Opcode::kPing);
            ids.push_back(request.header.request_id);
            CHECK(WriteEmptySuccess(connection, request).ok(),
                  "replacement connection sends valid Ping");
        }
    });

    TcpConnector connector(Endpoint(listener));
    Status first = connector.Ping();
    CHECK(first.code() == StatusCode::kCorruption,
          "response Header/matching error returns protocol corruption");
    CHECK(!connector.IsConnected(),
          "response Header/matching error drops connection");
    CHECK(connector.Ping().ok(),
          "next request reconnects after matching error");
    connector.Close();
    peer.join();
    CHECK(accepted.load() == 2,
          "matching error triggers one replacement connection");
    CHECK(ids == std::vector<std::uint64_t>({1, 2}),
          "matching-error reconnect keeps request ID sequence");
}

void TestResponseMatchingErrors() {
    RunMatchError(MatchError::kRequestKind);
    RunMatchError(MatchError::kWrongOpcode);
    RunMatchError(MatchError::kWrongRequestId);
    RunMatchError(MatchError::kZeroRequestId);
    RunMatchError(MatchError::kFlags);
    RunMatchError(MatchError::kUnknownStatus);
}

enum class PayloadError {
    kPutSuccessExtra,
    kPingSuccessExtra,
    kGetSuccessEmpty,
    kMalformedErrorLength,
    kTrailingErrorByte,
    kSuccessWithErrorPayload,
};

void RunPayloadError(PayloadError error) {
    Listener listener = MakeListener();
    const Block block = MakeBlock();
    const BlockKey key = Key();
    std::atomic<int> accepted{0};
    Opcode opcode =
        error == PayloadError::kPutSuccessExtra
            ? Opcode::kPut
            : (error == PayloadError::kGetSuccessEmpty ||
                       error == PayloadError::kSuccessWithErrorPayload
                   ? Opcode::kGet
                   : Opcode::kPing);

    std::thread peer([&] {
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            accepted.fetch_add(1);
            WireFrame request = ReadRequest(connection, opcode);

            if (error == PayloadError::kMalformedErrorLength ||
                error == PayloadError::kTrailingErrorByte) {
                const std::string payload =
                    error == PayloadError::kMalformedErrorLength
                        ? std::string("\0\0\0\1", 4)
                        : std::string("\0\0\0\0x", 5);
                const FrameHeader header{
                    MessageKind::kResponse, opcode, WireStatus::kNotFound, 0,
                    request.header.request_id, payload.size()};
                CHECK(WriteWireFrame(connection, header, payload).ok(),
                      "peer writes malformed ErrorPayload in valid frame");
            } else if (error == PayloadError::kSuccessWithErrorPayload) {
                auto payload = EncodeErrorPayload("not a Block blob");
                CHECK(payload.ok() &&
                          WriteResponse(connection, request, WireStatus::kOk,
                                        payload.value())
                              .ok(),
                      "peer labels ErrorPayload bytes as successful Get");
            } else {
                FrameHeader valid{
                    MessageKind::kResponse, opcode, WireStatus::kOk, 0,
                    request.header.request_id,
                    error == PayloadError::kGetSuccessEmpty ? 1U : 0U};
                auto encoded = EncodeHeader(valid);
                CHECK(encoded.ok(), "payload-error baseline Header encodes");
                if (error == PayloadError::kGetSuccessEmpty) {
                    PutU64(&encoded.value(), 24, 0);
                    CHECK(WriteRaw(connection, encoded.value()),
                          "peer writes Get success with empty payload");
                } else {
                    PutU64(&encoded.value(), 24, 1);
                    CHECK(WriteRaw(connection, encoded.value(), "x"),
                          "peer writes empty-success operation with extra byte");
                }
            }
        }
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            accepted.fetch_add(1);
            WireFrame request = ReadRequest(connection, Opcode::kPing);
            CHECK(WriteEmptySuccess(connection, request).ok(),
                  "replacement connection serves Ping after payload error");
        }
    });

    TcpConnector connector(Endpoint(listener));
    Status status;
    if (opcode == Opcode::kPut) {
        status = connector.Put(key, block);
    } else if (opcode == Opcode::kGet) {
        auto result = connector.Get(key);
        status = result.ok() ? Status::Ok() : result.status();
    } else {
        status = connector.Ping();
    }
    CHECK(!status.ok() &&
              (status.code() == StatusCode::kCorruption ||
               status.code() == StatusCode::kNetworkError),
          "payload semantic error fails current RPC");
    CHECK(!connector.IsConnected(),
          "payload semantic error drops connection");
    CHECK(connector.Ping().ok(),
          "next request reconnects after payload semantic error");
    connector.Close();
    peer.join();
    CHECK(accepted.load() == 2,
          "payload error triggers exactly one replacement connection");
}

void TestPayloadSemanticErrors() {
    RunPayloadError(PayloadError::kPutSuccessExtra);
    RunPayloadError(PayloadError::kPingSuccessExtra);
    RunPayloadError(PayloadError::kGetSuccessEmpty);
    RunPayloadError(PayloadError::kMalformedErrorLength);
    RunPayloadError(PayloadError::kTrailingErrorByte);
    RunPayloadError(PayloadError::kSuccessWithErrorPayload);
}

void TestPutTimeoutDoesNotReplayAndNextRequestReconnects() {
    Listener listener = MakeListener();
    const Block block = MakeBlock();
    const BlockKey key = Key();
    std::atomic<int> put_count{0};
    std::atomic<bool> retry_checked{false};
    std::atomic<bool> unexpected_retry{false};
    std::vector<std::uint64_t> ids;

    std::thread peer([&] {
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            WireFrame request = ReadRequest(connection, Opcode::kPut);
            ids.push_back(request.header.request_id);
            put_count.fetch_add(1);
            std::this_thread::sleep_for(250ms);
        }

        pollfd descriptor{};
        descriptor.fd = listener.fd;
        descriptor.events = POLLIN;
        const int poll_result = ::poll(&descriptor, 1, 80);
        unexpected_retry.store(
            poll_result > 0 && (descriptor.revents & POLLIN) != 0,
            std::memory_order_release);
        retry_checked.store(true, std::memory_order_release);

        TcpConnection connection = AcceptConnection(listener.fd);
        WireFrame request = ReadRequest(connection, Opcode::kPing);
        ids.push_back(request.header.request_id);
        CHECK(WriteEmptySuccess(connection, request).ok(),
              "explicit next Ping succeeds on new connection");
    });

    TcpConnector connector(Endpoint(listener), ClientOptions(100ms));
    Status put = connector.Put(key, block);
    CHECK(!put.ok(), "Put without response fails");
    CHECK(!connector.IsConnected(), "timed-out Put drops connection");
    WaitFor(retry_checked);
    CHECK(!unexpected_retry.load(),
          "failed Put is not automatically reconnected or replayed");
    CHECK(put_count.load() == 1, "peer receives failed Put exactly once");
    CHECK(connector.Ping().ok(),
          "explicit next request reconnects after failed Put");
    connector.Close();
    peer.join();
    CHECK(ids == std::vector<std::uint64_t>({1, 2}),
          "next request uses a new ID after failed Put");
}

void TestGetDisconnectDoesNotReplay() {
    Listener listener = MakeListener();
    const BlockKey key = Key();
    std::atomic<int> get_count{0};
    std::atomic<bool> retry_checked{false};
    std::atomic<bool> unexpected_retry{false};

    std::thread peer([&] {
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            WireFrame request = ReadRequest(connection, Opcode::kGet);
            (void)request;
            get_count.fetch_add(1);
        }

        pollfd descriptor{};
        descriptor.fd = listener.fd;
        descriptor.events = POLLIN;
        const int poll_result = ::poll(&descriptor, 1, 80);
        unexpected_retry.store(
            poll_result > 0 && (descriptor.revents & POLLIN) != 0);
        retry_checked.store(true, std::memory_order_release);

        TcpConnection connection = AcceptConnection(listener.fd);
        WireFrame request = ReadRequest(connection, Opcode::kPing);
        CHECK(request.header.request_id == 2,
              "explicit request after failed Get advances request ID");
        CHECK(WriteEmptySuccess(connection, request).ok(),
              "explicit Ping succeeds after failed Get");
    });

    TcpConnector connector(Endpoint(listener));
    auto get = connector.Get(key);
    CHECK(!get.ok(), "Get fails when peer closes without response");
    WaitFor(retry_checked);
    CHECK(get_count.load() == 1 && !unexpected_retry.load(),
          "failed Get is received once and not replayed");
    CHECK(connector.Ping().ok(),
          "next explicit request reconnects after failed Get");
    connector.Close();
    peer.join();
}

void TestPeerCloseBeforeRequestAndMidWrite() {
    {
        Listener listener = MakeListener();
        std::vector<std::uint64_t> ids;
        std::thread peer([&] {
            {
                const int accepted = AcceptRaw(listener.fd);
                linger reset{1, 0};
                CHECK(::setsockopt(accepted, SOL_SOCKET, SO_LINGER, &reset,
                                   sizeof(reset)) == 0,
                      "peer configures reset before first request");
                ::close(accepted);
            }
            {
                TcpConnection connection = AcceptConnection(listener.fd);
                WireFrame request = ReadRequest(connection, Opcode::kPing);
                ids.push_back(request.header.request_id);
                CHECK(WriteEmptySuccess(connection, request).ok(),
                      "Ping after pre-request close reconnects");
            }
        });

        TcpConnector connector(Endpoint(listener));
        Status first = connector.Ping();
        CHECK(!first.ok(), "peer close before request completion fails Ping");
        CHECK(!connector.IsConnected(),
              "pre-request peer close drops connection");
        CHECK(connector.Ping().ok(),
              "next Ping reconnects after pre-request close");
        connector.Close();
        peer.join();
        CHECK(ids == std::vector<std::uint64_t>({2}),
              "failed pre-request Ping is not replayed");
    }

    {
        Listener listener = MakeListener();
        Block large = MakeBlock();
        large.data.assign(8U * 1024U * 1024U, 0x5a);
        large.data[0] = 0;
        large.data[large.data.size() - 1] = 0xff;
        std::uint64_t first_id = 0;
        std::uint64_t second_id = 0;

        std::thread peer([&] {
            {
                const int accepted = AcceptRaw(listener.fd);
                std::array<std::uint8_t, wire::kHeaderBytes> header{};
                CHECK(ReceiveAll(accepted, header.data(), header.size()),
                      "peer receives full Put Header before mid-write close");
                auto decoded =
                    DecodeHeader(header.data(), header.size());
                CHECK(decoded.ok() &&
                          decoded.value().opcode == Opcode::kPut,
                      "large request begins with valid Put Header");
                first_id = decoded.value().request_id;

                std::array<std::uint8_t, 4096> partial{};
                ssize_t received = -1;
                do {
                    received =
                        ::recv(accepted, partial.data(), partial.size(), 0);
                } while (received < 0 && errno == EINTR);
                CHECK(received > 0,
                      "peer receives part of large Put Payload");
                linger reset{1, 0};
                CHECK(::setsockopt(accepted, SOL_SOCKET, SO_LINGER, &reset,
                                   sizeof(reset)) == 0,
                      "peer configures reset during Put write");
                ::close(accepted);
            }
            {
                TcpConnection connection = AcceptConnection(listener.fd);
                WireFrame request = ReadRequest(connection, Opcode::kPing);
                second_id = request.header.request_id;
                CHECK(WriteEmptySuccess(connection, request).ok(),
                      "Ping after mid-write failure reconnects");
            }
        });

        TcpConnectorOptions options = ClientOptions(2s, 2s);
        TcpConnector connector(Endpoint(listener), options);
        Status put = connector.Put(Key(), large);
        CHECK(!put.ok(), "peer reset during Put write fails current Put");
        CHECK(!connector.IsConnected(),
              "mid-write failure drops connection");
        CHECK(connector.Ping().ok(),
              "next request reconnects after mid-write failure");
        connector.Close();
        peer.join();
        CHECK(first_id == 1 && second_id == 2,
              "mid-write Put is sent once and never replayed");
    }
}

enum class Truncation {
    kHeader,
    kPayload,
};

void RunTruncationCase(Truncation truncation) {
    Listener listener = MakeListener();
    std::atomic<int> accepted{0};

    std::thread peer([&] {
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            accepted.fetch_add(1);
            WireFrame request = ReadRequest(connection, Opcode::kPing);
            auto error = EncodeErrorPayload("truncated-response");
            CHECK(error.ok(), "truncation error payload encodes");
            const FrameHeader header{
                MessageKind::kResponse, Opcode::kPing,
                WireStatus::kUnavailable, 0, request.header.request_id,
                static_cast<std::uint64_t>(error.value().size())};
            auto encoded = EncodeHeader(header);
            CHECK(encoded.ok(), "truncation response Header encodes");
            if (truncation == Truncation::kHeader) {
                CHECK(connection.WriteExact(encoded.value().data(), 11).ok(),
                      "peer writes truncated response Header");
            } else {
                CHECK(connection
                          .WriteExact(encoded.value().data(),
                                      encoded.value().size())
                          .ok(),
                      "peer writes full response Header");
                CHECK(connection.WriteExact(error.value().data(), 3).ok(),
                      "peer writes truncated response Payload");
            }
        }
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            accepted.fetch_add(1);
            WireFrame request = ReadRequest(connection, Opcode::kPing);
            CHECK(WriteEmptySuccess(connection, request).ok(),
                  "next Ping succeeds after truncated response");
        }
    });

    TcpConnector connector(Endpoint(listener));
    Status first = connector.Ping();
    CHECK(first.code() == StatusCode::kNetworkError,
          "truncated response returns NetworkError");
    CHECK(!connector.IsConnected(),
          "truncated response drops connection");
    CHECK(connector.Ping().ok(),
          "next request reconnects after truncated response");
    connector.Close();
    peer.join();
    CHECK(accepted.load() == 2,
          "truncated response creates one replacement connection");
}

void TestTruncatedResponses() {
    RunTruncationCase(Truncation::kHeader);
    RunTruncationCase(Truncation::kPayload);
}

void TestCompleteResponseThenPeerClose() {
    Listener listener = MakeListener();
    std::vector<std::uint64_t> ids;
    std::atomic<int> accepted{0};

    std::thread peer([&] {
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            accepted.fetch_add(1);
            WireFrame request = ReadRequest(connection, Opcode::kPing);
            ids.push_back(request.header.request_id);
            CHECK(WriteEmptySuccess(connection, request).ok(),
                  "peer writes complete Ping response before close");
        }
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            accepted.fetch_add(1);
            WireFrame request = ReadRequest(connection, Opcode::kPing);
            ids.push_back(request.header.request_id);
            CHECK(WriteEmptySuccess(connection, request).ok(),
                  "request after detecting stale connection succeeds");
        }
    });

    TcpConnector connector(Endpoint(listener));
    CHECK(connector.Ping().ok(),
          "complete response succeeds even when peer immediately closes");
    Status detection = connector.Ping();
    CHECK(!detection.ok(),
          "next request detects peer-closed persistent connection");
    CHECK(connector.Ping().ok(),
          "request after stale-connection detection reconnects");
    connector.Close();
    peer.join();
    CHECK(accepted.load() == 2,
          "peer close causes one later replacement connection");
    CHECK(ids == std::vector<std::uint64_t>({1, 3}),
          "failed stale-connection request consumes ID without replay");
}

void TestRequestIdWrap() {
    Listener listener = MakeListener();
    std::vector<std::uint64_t> ids;

    std::thread peer([&] {
        TcpConnection connection = AcceptConnection(listener.fd);
        for (int i = 0; i < 2; ++i) {
            WireFrame request = ReadRequest(connection, Opcode::kPing);
            ids.push_back(request.header.request_id);
            CHECK(WriteEmptySuccess(connection, request).ok(),
                  "wrap test Ping response succeeds");
        }
    });

    TcpConnector connector(Endpoint(listener));
    TcpConnectorTestAccess::SetNextRequestId(
        &connector, std::numeric_limits<std::uint64_t>::max());
    CHECK(connector.Ping().ok(), "UINT64_MAX request ID is valid");
    CHECK(connector.Ping().ok(), "request ID wraps to one");
    connector.Close();
    peer.join();
    CHECK(ids ==
              std::vector<std::uint64_t>({
                  std::numeric_limits<std::uint64_t>::max(), 1}),
          "request ID overflow skips reserved zero");
}

void TestConcurrentCallsAreSerialized() {
    constexpr int kCalls = 8;
    Listener listener = MakeListener();
    std::vector<std::uint64_t> ids;
    std::atomic<bool> saw_pipelined_request{false};

    std::thread peer([&] {
        int accepted_fd = -1;
        TcpConnection connection =
            AcceptConnection(listener.fd, &accepted_fd);
        for (int i = 0; i < kCalls; ++i) {
            WireFrame request = ReadRequest(connection, Opcode::kPing);
            ids.push_back(request.header.request_id);

            pollfd descriptor{};
            descriptor.fd = accepted_fd;
            descriptor.events = POLLIN;
            const int ready = ::poll(&descriptor, 1, 25);
            if (ready > 0 && (descriptor.revents & POLLIN) != 0) {
                saw_pipelined_request.store(true);
            }
            CHECK(WriteEmptySuccess(connection, request).ok(),
                  "concurrent-call Ping response succeeds");
        }
    });

    TcpConnector connector(Endpoint(listener));
    std::array<Status, kCalls> statuses;
    std::vector<std::thread> clients;
    for (int i = 0; i < kCalls; ++i) {
        clients.emplace_back([&, i] { statuses[i] = connector.Ping(); });
    }
    for (std::thread& client : clients) client.join();
    connector.Close();
    peer.join();

    for (const Status& status : statuses) {
        CHECK(status.ok(), "every concurrent Ping gets its own response");
    }
    CHECK(!saw_pipelined_request.load(),
          "single connector never has two in-flight requests");
    std::sort(ids.begin(), ids.end());
    CHECK(ids == std::vector<std::uint64_t>({1, 2, 3, 4, 5, 6, 7, 8}),
          "concurrent callers receive unique request IDs");
}

void TestConcurrentMixedCallsAreSerialized() {
    constexpr int kCalls = 6;
    Listener listener = MakeListener();
    const Block block = MakeBlock(0x77);
    const BlockKey key = Key(0x7777777777777777ULL);
    std::vector<std::uint64_t> ids;
    std::array<int, 3> opcode_counts{{0, 0, 0}};
    std::atomic<bool> saw_pipelined_request{false};

    std::thread peer([&] {
        int accepted_fd = -1;
        TcpConnection connection =
            AcceptConnection(listener.fd, &accepted_fd);
        for (int i = 0; i < kCalls; ++i) {
            auto frame = ReadWireFrame(connection);
            CHECK(frame.ok(), "mixed concurrent request frame is complete");
            CHECK(ValidateRequest(frame.value().header,
                                  DataOrNull(frame.value().payload),
                                  frame.value().payload.size())
                      .ok(),
                  "mixed concurrent request validates");
            ids.push_back(frame.value().header.request_id);

            pollfd descriptor{};
            descriptor.fd = accepted_fd;
            descriptor.events = POLLIN;
            const int ready = ::poll(&descriptor, 1, 25);
            if (ready > 0 && (descriptor.revents & POLLIN) != 0) {
                saw_pipelined_request.store(true);
            }

            if (frame.value().header.opcode == Opcode::kPing) {
                ++opcode_counts[0];
                CHECK(WriteEmptySuccess(connection, frame.value()).ok(),
                      "mixed concurrent Ping responds");
            } else if (frame.value().header.opcode == Opcode::kGet) {
                ++opcode_counts[1];
                auto decoded = DecodeGetRequest(
                    DataOrNull(frame.value().payload),
                    frame.value().payload.size());
                CHECK(decoded.ok() && decoded.value() == key,
                      "mixed concurrent Get key matches");
                ByteBuffer payload = BlockPayload(block);
                CHECK(WriteResponse(connection, frame.value(),
                                    WireStatus::kOk, payload)
                          .ok(),
                      "mixed concurrent Get responds");
            } else {
                ++opcode_counts[2];
                auto decoded = DecodePutRequest(
                    DataOrNull(frame.value().payload),
                    frame.value().payload.size());
                CHECK(decoded.ok() && decoded.value().key == key &&
                          decoded.value().block_blob == SerializeBlock(block),
                      "mixed concurrent Put blob matches");
                CHECK(WriteEmptySuccess(connection, frame.value()).ok(),
                      "mixed concurrent Put responds");
            }
        }
    });

    TcpConnector connector(Endpoint(listener));
    std::array<Status, kCalls> statuses;
    std::vector<std::thread> clients;
    for (int i = 0; i < kCalls; ++i) {
        clients.emplace_back([&, i] {
            if (i % 3 == 0) {
                statuses[i] = connector.Ping();
            } else if (i % 3 == 1) {
                auto result = connector.Get(key);
                statuses[i] =
                    result.ok() && SameBlock(result.value(), block)
                        ? Status::Ok()
                        : (result.ok()
                               ? Status::Internal("mixed Get data mismatch")
                               : result.status());
            } else {
                statuses[i] = connector.Put(key, block);
            }
        });
    }
    for (std::thread& client : clients) client.join();
    connector.Close();
    peer.join();

    for (const Status& status : statuses) {
        CHECK(status.ok(), "every mixed concurrent RPC succeeds");
    }
    CHECK(!saw_pipelined_request.load(),
          "mixed Get/Put/Ping calls remain strictly serialized");
    std::sort(ids.begin(), ids.end());
    CHECK(ids == std::vector<std::uint64_t>({1, 2, 3, 4, 5, 6}),
          "mixed concurrent RPCs use unique IDs");
    const std::array<int, 3> expected_counts{{2, 2, 2}};
    CHECK(opcode_counts == expected_counts,
          "mixed concurrency executes every RPC operation twice");
}

void TestCloseWaitsAndReconnectsWithoutReset() {
    Listener listener = MakeListener();
    std::atomic<bool> first_read{false};
    std::vector<std::uint64_t> ids;

    std::thread peer([&] {
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            WireFrame request = ReadRequest(connection, Opcode::kPing);
            ids.push_back(request.header.request_id);
            first_read.store(true, std::memory_order_release);
            std::this_thread::sleep_for(180ms);
            CHECK(WriteEmptySuccess(connection, request).ok(),
                  "delayed Ping response succeeds before Close");
            std::this_thread::sleep_for(20ms);
        }
        {
            TcpConnection connection = AcceptConnection(listener.fd);
            WireFrame request = ReadRequest(connection, Opcode::kPing);
            ids.push_back(request.header.request_id);
            CHECK(WriteEmptySuccess(connection, request).ok(),
                  "Ping after Close reconnects");
        }
    });

    TcpConnector connector(Endpoint(listener));
    connector.Close();
    connector.Close();
    CHECK(!connector.IsConnected(),
          "Close is idempotent before first connection");

    Status first_status;
    std::thread caller([&] { first_status = connector.Ping(); });
    WaitFor(first_read);
    const auto close_start = std::chrono::steady_clock::now();
    connector.Close();
    const auto close_elapsed =
        std::chrono::steady_clock::now() - close_start;
    caller.join();

    CHECK(first_status.ok(), "in-progress Ping completes before Close");
    CHECK(close_elapsed >= 100ms,
          "Close waits on the same mutex as complete RPC");
    CHECK(!connector.IsConnected(), "Close drops persistent connection");
    connector.Close();
    CHECK(connector.Ping().ok(), "Close does not permanently disable connector");
    connector.Close();
    peer.join();
    CHECK(ids == std::vector<std::uint64_t>({1, 2}),
          "Close and reconnect do not reset request ID");
}

}  // namespace

int main() {
    TestConfigurationValidation();
    TestOversizedPutFailsBeforeConnect();
    TestPersistentMixedRpcAndRequestIds();
    TestGetSuccessNotFoundAndReuse();
    TestPutSuccessAndBusinessErrors();
    TestCorruptGetBlocks();
    TestResponseMatchingErrors();
    TestPayloadSemanticErrors();
    TestPutTimeoutDoesNotReplayAndNextRequestReconnects();
    TestGetDisconnectDoesNotReplay();
    TestPeerCloseBeforeRequestAndMidWrite();
    TestTruncatedResponses();
    TestCompleteResponseThenPeerClose();
    TestRequestIdWrap();
    TestConcurrentCallsAreSerialized();
    TestConcurrentMixedCallsAreSerialized();
    TestCloseWaitsAndReconnectsWithoutReset();

    std::printf("tidepool tcp connector test: %d checks passed\n",
                g_checks.load());
    return 0;
}
