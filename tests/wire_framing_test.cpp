#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "tidepool/api/block.h"
#include "tidepool/api/block_codec.h"
#include "tidepool/protocol/wire_codec.h"
#include "tidepool/transport/tcp_connection.h"
#include "tidepool/transport/wire_framing.h"

using namespace tidepool;
using namespace tidepool::wire;
using namespace std::chrono_literals;

namespace {

int g_checks = 0;

#define CHECK(cond, msg)                                                                   \
    do {                                                                                   \
        ++g_checks;                                                                        \
        if (!(cond)) {                                                                     \
            std::fprintf(stderr, "CHECK failed: %s (%s:%d)\n", (msg), __FILE__, __LINE__); \
            std::abort();                                                                  \
        }                                                                                  \
    } while (0)

TcpConnectionOptions Options() {
    TcpConnectionOptions options;
    options.connect_timeout = 500ms;
    options.read_timeout = 1000ms;
    options.write_timeout = 1000ms;
    return options;
}

std::array<int, 2> MakeSocketPair() {
    std::array<int, 2> sockets{{-1, -1}};
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0,
                       sockets.data()) == 0,
          "wire framing socketpair succeeds");
    return sockets;
}

TcpConnection Adopt(int* fd) {
    CHECK(fd != nullptr && *fd >= 0, "wire framing Adopt helper receives fd");
    const int transferred = *fd;
    *fd = -1;
    auto result = TcpConnection::AdoptConnectedSocket(transferred, Options());
    CHECK(result.ok(), "wire framing connection adoption succeeds");
    return std::move(result.value());
}

bool SendAll(int fd, const void* input, std::size_t bytes) {
    const auto* source = static_cast<const std::uint8_t*>(input);
    std::size_t completed = 0;
    while (completed < bytes) {
        const ssize_t result =
            ::send(fd, source + completed, bytes - completed, MSG_NOSIGNAL);
        if (result > 0) {
            completed += static_cast<std::size_t>(result);
            continue;
        }
        if (result < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
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

FrameHeader Request(Opcode opcode, std::uint64_t request_id,
                    std::uint64_t payload_bytes) {
    return FrameHeader{MessageKind::kRequest, opcode, WireStatus::kOk, 0,
                       request_id, payload_bytes};
}

FrameHeader Response(Opcode opcode, WireStatus status,
                     std::uint64_t request_id,
                     std::uint64_t payload_bytes) {
    return FrameHeader{MessageKind::kResponse, opcode, status, 0, request_id,
                       payload_bytes};
}

void CheckHeader(const FrameHeader& actual, const FrameHeader& expected,
                 const char* message) {
    CHECK(actual.kind == expected.kind && actual.opcode == expected.opcode &&
              actual.status == expected.status && actual.flags == expected.flags &&
              actual.request_id == expected.request_id &&
              actual.payload_bytes == expected.payload_bytes,
          message);
}

void PutU64(EncodedHeader* bytes, std::size_t offset, std::uint64_t value) {
    for (std::size_t i = 0; i < 8; ++i) {
        (*bytes)[offset + i] = static_cast<std::uint8_t>(
            value >> static_cast<unsigned>((7U - i) * 8U));
    }
}

Block MakeBlock() {
    Block block;
    block.metadata.num_tokens = 3;
    block.metadata.num_layers = 4;
    block.metadata.dtype_size = 2;
    block.metadata.kv_heads = 8;
    block.metadata.created_unix_ns = 0x0102030405060708ULL;
    block.metadata.model_fingerprint = 0x11223344U;
    block.data = {0x00, 0x7f, 0x80, 0xff, 0x31, 0x00, 0x42};
    return block;
}

void TestReadCompleteAndEmptyFrames() {
    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        const std::string payload("key-and\0blob-data", 17);
        const FrameHeader header = Request(Opcode::kPut, 11, payload.size());
        auto encoded = EncodeHeader(header);
        CHECK(encoded.ok(), "complete raw frame header encodes");
        CHECK(SendAll(sockets[1], encoded.value().data(), encoded.value().size()),
              "peer sends complete header");
        CHECK(SendAll(sockets[1], payload.data(), payload.size()),
              "peer sends complete binary payload");

        auto frame = ReadWireFrame(connection);
        CHECK(frame.ok(), "ReadWireFrame reads complete frame");
        CheckHeader(frame.value().header, header,
                    "ReadWireFrame preserves complete header");
        CHECK(frame.value().payload == payload,
              "ReadWireFrame owns exact binary payload");
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        // Raw framing preserves request_id zero; RPC semantic validation rejects
        // it later. This keeps transport lossless and business-agnostic.
        const FrameHeader header = Request(Opcode::kPing, 0, 0);
        auto encoded = EncodeHeader(header);
        CHECK(encoded.ok(), "raw zero-id Ping header encodes");
        CHECK(SendAll(sockets[1], encoded.value().data(), encoded.value().size()),
              "peer sends empty-payload header");
        auto frame = ReadWireFrame(connection);
        CHECK(frame.ok() && frame.value().payload.empty(),
              "ReadWireFrame handles empty payload without extra read");
        CheckHeader(frame.value().header, header,
                    "empty frame preserves raw request_id zero");
        CHECK(!ValidateRequest(frame.value().header, nullptr, 0).ok(),
              "RPC semantic validator rejects reserved zero request id");
        ::close(sockets[1]);
    }
}

void TestFragmentedRead() {
    auto sockets = MakeSocketPair();
    TcpConnection connection = Adopt(&sockets[0]);
    const std::string payload("fragmented\0payload", 18);
    const FrameHeader header =
        Response(Opcode::kGet, WireStatus::kOk, 22, payload.size());
    auto encoded = EncodeHeader(header);
    CHECK(encoded.ok(), "fragmented frame header encodes");

    std::thread writer([&] {
        std::size_t offset = 0;
        const std::array<std::size_t, 6> header_chunks{{1, 2, 3, 5, 8, 13}};
        for (std::size_t requested : header_chunks) {
            if (offset >= encoded.value().size()) break;
            const std::size_t chunk =
                std::min(requested, encoded.value().size() - offset);
            if (!SendAll(sockets[1], encoded.value().data() + offset, chunk)) return;
            offset += chunk;
            std::this_thread::sleep_for(3ms);
        }
        if (offset < encoded.value().size() &&
            !SendAll(sockets[1], encoded.value().data() + offset,
                     encoded.value().size() - offset)) {
            return;
        }
        for (char byte : payload) {
            if (!SendAll(sockets[1], &byte, 1)) return;
            std::this_thread::sleep_for(2ms);
        }
    });

    auto frame = ReadWireFrame(connection);
    writer.join();
    CHECK(frame.ok(), "ReadWireFrame combines fragmented header and payload");
    CheckHeader(frame.value().header, header,
                "fragmented frame preserves header");
    CHECK(frame.value().payload == payload,
          "fragmented frame preserves binary payload");
    ::close(sockets[1]);
}

void TestCompleteFrameThenHalfClose() {
    auto sockets = MakeSocketPair();
    TcpConnection connection = Adopt(&sockets[0]);
    const std::string payload("complete-frame\0then-close", 25);
    const FrameHeader header =
        Response(Opcode::kGet, WireStatus::kOk, 23, payload.size());
    auto encoded = EncodeHeader(header);
    CHECK(encoded.ok(), "complete-before-close frame header encodes");

    bool header_sent = false;
    bool payload_sent = false;
    std::thread writer([&] {
        std::this_thread::sleep_for(30ms);
        header_sent =
            SendAll(sockets[1], encoded.value().data(), encoded.value().size());
        if (header_sent) {
            payload_sent =
                SendAll(sockets[1], payload.data(), payload.size());
        }
        ::shutdown(sockets[1], SHUT_WR);
    });

    auto frame = ReadWireFrame(connection);
    writer.join();
    CHECK(header_sent && payload_sent && frame.ok(),
          "ReadWireFrame consumes complete frame before peer EOF");
    CHECK(frame.value().payload == payload,
          "complete frame before close preserves binary payload");
    CheckHeader(frame.value().header, header,
                "complete frame before close preserves header");

    auto next = ReadWireFrame(connection);
    CHECK(!next.ok() && next.status().code() == StatusCode::kNetworkError,
          "next frame read observes EOF after complete prior frame");
    ::close(sockets[1]);
}

void TestMalformedAndTruncatedReads() {
    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        auto encoded = EncodeHeader(Request(Opcode::kPut, 1, 17));
        CHECK(encoded.ok(), "bad-magic baseline encodes");
        encoded.value()[0] ^= 0xffU;
        CHECK(SendAll(sockets[1], encoded.value().data(), encoded.value().size()),
              "peer sends malformed magic header");
        const auto start = std::chrono::steady_clock::now();
        auto frame = ReadWireFrame(connection);
        const auto elapsed = std::chrono::steady_clock::now() - start;
        CHECK(!frame.ok() && frame.status().code() == StatusCode::kCorruption,
              "malformed magic returns Codec corruption");
        CHECK(!connection.IsOpen(), "malformed magic invalidates connection");
        CHECK(elapsed < 500ms,
              "malformed header returns without reading declared payload");
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        auto encoded = EncodeHeader(Request(Opcode::kPut, 1, 17));
        encoded.value()[9] = 0xffU;
        CHECK(SendAll(sockets[1], encoded.value().data(), encoded.value().size()),
              "peer sends unknown opcode header");
        auto frame = ReadWireFrame(connection);
        CHECK(!frame.ok() && frame.status().code() == StatusCode::kCorruption,
              "unknown opcode returns corruption");
        CHECK(!connection.IsOpen(), "unknown opcode invalidates connection");
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        auto encoded = EncodeHeader(Request(Opcode::kPut, 1, 17));
        PutU64(&encoded.value(), 24, kDefaultMaxFrameBytes);
        CHECK(SendAll(sockets[1], encoded.value().data(), encoded.value().size()),
              "peer sends oversized payload declaration");
        auto frame = ReadWireFrame(connection);
        CHECK(!frame.ok() && frame.status().code() == StatusCode::kCorruption,
              "oversized payload declaration fails before allocation");
        CHECK(!connection.IsOpen(), "oversized frame invalidates connection");
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        auto encoded = EncodeHeader(Request(Opcode::kPut, 1, 17));
        CHECK(SendAll(sockets[1], encoded.value().data(), 16),
              "peer sends partial fixed header");
        ::close(sockets[1]);
        auto frame = ReadWireFrame(connection);
        CHECK(!frame.ok() && frame.status().code() == StatusCode::kNetworkError,
              "truncated fixed header is a network error");
        CHECK(!connection.IsOpen(), "truncated header invalidates connection");
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        const FrameHeader header = Request(Opcode::kPut, 1, 20);
        auto encoded = EncodeHeader(header);
        CHECK(SendAll(sockets[1], encoded.value().data(), encoded.value().size()),
              "peer sends valid header for truncated body");
        const std::array<std::uint8_t, 5> partial{{1, 2, 3, 4, 5}};
        CHECK(SendAll(sockets[1], partial.data(), partial.size()),
              "peer sends partial payload");
        ::close(sockets[1]);
        auto frame = ReadWireFrame(connection);
        CHECK(!frame.ok() && frame.status().code() == StatusCode::kNetworkError,
              "truncated payload is a network error");
        CHECK(!connection.IsOpen(), "truncated payload invalidates connection");
    }
}

void TestWriteFrames() {
    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        const std::string payload("0123456789abcdef\0", 17);
        const FrameHeader header = Request(Opcode::kPut, 33, payload.size());
        auto expected_header = EncodeHeader(header);
        CHECK(WriteWireFrame(connection, header, payload).ok(),
              "WriteWireFrame writes binary frame");
        EncodedHeader actual_header{};
        CHECK(ReceiveAll(sockets[1], actual_header.data(), actual_header.size()),
              "peer receives written header");
        CHECK(actual_header == expected_header.value(),
              "written header exactly matches protocol EncodeHeader");
        std::string actual_payload(payload.size(), '\0');
        CHECK(ReceiveAll(sockets[1], actual_payload.data(), actual_payload.size()),
              "peer receives written payload");
        CHECK(actual_payload == payload,
              "WriteWireFrame preserves binary payload exactly");
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        FrameHeader header = Request(Opcode::kPing, 44, 0);
        CHECK(WriteWireFrame(connection, header, "unexpected").code() ==
                  StatusCode::kInvalidArgument,
              "payload/header size mismatch fails before writing");
        CHECK(connection.IsOpen(), "local frame mismatch does not break connection");
        CHECK(WriteWireFrame(connection, header, "").ok(),
              "connection remains reusable after pre-write validation failure");
        EncodedHeader actual{};
        auto expected = EncodeHeader(header);
        CHECK(ReceiveAll(sockets[1], actual.data(), actual.size()) &&
                  actual == expected.value(),
              "empty frame writes only exact header");
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        FrameHeader invalid = Request(Opcode::kPing, 55, 0);
        invalid.flags = 1;
        CHECK(!WriteWireFrame(connection, invalid, "").ok(),
              "invalid local header is rejected by EncodeHeader");
        CHECK(connection.IsOpen(),
              "invalid local header does not consume or break connection");
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        ::close(sockets[1]);
        const std::string payload(17, 'x');
        const FrameHeader header = Request(Opcode::kPut, 66, payload.size());
        Status status = WriteWireFrame(connection, header, payload);
        CHECK(status.code() == StatusCode::kNetworkError,
              "peer close during frame write is a network error");
        CHECK(!connection.IsOpen(), "frame write failure invalidates connection");
    }

    {
        auto sockets = MakeSocketPair();
        int send_buffer = 4096;
        CHECK(::setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &send_buffer,
                           sizeof(send_buffer)) == 0,
              "frame partial-write test sets a small send buffer");
        TcpConnection connection = Adopt(&sockets[0]);
        const std::string payload(4 * 1024 * 1024, 'p');
        const FrameHeader header =
            Response(Opcode::kGet, WireStatus::kOk, 67, payload.size());
        std::thread closing_reader([&] {
            std::array<std::uint8_t, kHeaderBytes + 8192> prefix{};
            ReceiveAll(sockets[1], prefix.data(), prefix.size());
            ::close(sockets[1]);
        });
        Status status = WriteWireFrame(connection, header, payload);
        closing_reader.join();
        CHECK(!status.ok(),
              "peer close after Header causes in-progress Payload write failure");
        CHECK(!connection.IsOpen(),
              "mid-frame Payload write failure invalidates connection");
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        const std::string blob = SerializeBlock(MakeBlock());
        const FrameHeader header =
            Response(Opcode::kGet, WireStatus::kOk, 77, blob.size());
        CHECK(WriteWireFrame(connection, header, blob).ok(),
              "WriteWireFrame accepts real Block blob");
        EncodedHeader raw_header{};
        std::string raw_blob(blob.size(), '\0');
        CHECK(ReceiveAll(sockets[1], raw_header.data(), raw_header.size()) &&
                  ReceiveAll(sockets[1], raw_blob.data(), raw_blob.size()),
              "peer receives Block frame bytes");
        CHECK(raw_blob == blob,
              "Wire framing does not copy-convert or alter Block blob");
        ::close(sockets[1]);
    }
}

void TestConsecutiveFrames() {
    auto sockets = MakeSocketPair();
    TcpConnection writer = Adopt(&sockets[0]);
    TcpConnection reader = Adopt(&sockets[1]);

    const FrameHeader first = Request(Opcode::kPing, 101, 0);
    const std::string second_payload("0123456789abcdefZ", 17);
    const FrameHeader second =
        Request(Opcode::kPut, 102, second_payload.size());

    CHECK(WriteWireFrame(writer, first, "").ok(),
          "first consecutive empty frame writes");
    CHECK(WriteWireFrame(writer, second, second_payload).ok(),
          "second consecutive nonempty frame writes");

    auto first_read = ReadWireFrame(reader);
    CHECK(first_read.ok() && first_read.value().payload.empty(),
          "first consecutive frame reads without consuming second");
    CheckHeader(first_read.value().header, first,
                "first consecutive header matches");

    auto second_read = ReadWireFrame(reader);
    CHECK(second_read.ok() && second_read.value().payload == second_payload,
          "second consecutive frame begins at exact boundary");
    CheckHeader(second_read.value().header, second,
                "second consecutive header matches");

    const std::string reverse_payload("reverse\0binary", 14);
    const FrameHeader reverse =
        Response(Opcode::kGet, WireStatus::kOk, 101,
                 reverse_payload.size());
    CHECK(WriteWireFrame(reader, reverse, reverse_payload).ok(),
          "same connection supports reverse-direction frame");
    auto reverse_read = ReadWireFrame(writer);
    CHECK(reverse_read.ok() && reverse_read.value().payload == reverse_payload,
          "reverse-direction frame reads exactly");
    CheckHeader(reverse_read.value().header, reverse,
                "reverse-direction header matches");
}

}  // namespace

int main() {
    TestReadCompleteAndEmptyFrames();
    TestFragmentedRead();
    TestCompleteFrameThenHalfClose();
    TestMalformedAndTruncatedReads();
    TestWriteFrames();
    TestConsecutiveFrames();

    std::printf("tidepool wire framing test: %d checks passed\n", g_checks);
    return 0;
}
