#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tidepool/transport/tcp_connection.h"

using namespace tidepool;
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

TcpConnectionOptions Options(std::chrono::milliseconds read_timeout = 500ms,
                             std::chrono::milliseconds write_timeout = 500ms,
                             std::chrono::milliseconds connect_timeout = 500ms) {
    TcpConnectionOptions options;
    options.connect_timeout = connect_timeout;
    options.read_timeout = read_timeout;
    options.write_timeout = write_timeout;
    return options;
}

std::array<int, 2> MakeSocketPair() {
    std::array<int, 2> sockets{{-1, -1}};
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0,
                       sockets.data()) == 0,
          "socketpair succeeds");
    return sockets;
}

TcpConnection Adopt(int* fd,
                    const TcpConnectionOptions& options = Options()) {
    CHECK(fd != nullptr && *fd >= 0, "Adopt helper receives an fd");
    const int transferred = *fd;
    *fd = -1;
    auto result = TcpConnection::AdoptConnectedSocket(transferred, options);
    CHECK(result.ok(), "AdoptConnectedSocket succeeds");
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
        const ssize_t result = ::recv(fd, destination + completed,
                                      bytes - completed, 0);
        if (result > 0) {
            completed += static_cast<std::size_t>(result);
            continue;
        }
        if (result < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}

bool IsClosedFd(int fd) {
    errno = 0;
    return ::fcntl(fd, F_GETFD) == -1 && errno == EBADF;
}

int CountOpenFds() {
    DIR* directory = ::opendir("/proc/self/fd");
    if (directory == nullptr) return -1;
    int count = 0;
    while (::readdir(directory) != nullptr) ++count;
    ::closedir(directory);
    return count;
}

struct Listener {
    int fd = -1;
    std::uint16_t port = 0;
};

Listener MakeListener(int family) {
    Listener listener;
    listener.fd = ::socket(family, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
    if (listener.fd < 0) return listener;

    int one = 1;
    ::setsockopt(listener.fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (family == AF_INET) {
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (::bind(listener.fd, reinterpret_cast<sockaddr*>(&address),
                   sizeof(address)) != 0) {
            ::close(listener.fd);
            return Listener{};
        }
        socklen_t address_size = sizeof(address);
        if (::getsockname(listener.fd, reinterpret_cast<sockaddr*>(&address),
                          &address_size) != 0) {
            ::close(listener.fd);
            return Listener{};
        }
        listener.port = ntohs(address.sin_port);
    } else {
        sockaddr_in6 address{};
        address.sin6_family = AF_INET6;
        address.sin6_addr = in6addr_loopback;
        address.sin6_port = 0;
        if (::bind(listener.fd, reinterpret_cast<sockaddr*>(&address),
                   sizeof(address)) != 0) {
            ::close(listener.fd);
            return Listener{};
        }
        socklen_t address_size = sizeof(address);
        if (::getsockname(listener.fd, reinterpret_cast<sockaddr*>(&address),
                          &address_size) != 0) {
            ::close(listener.fd);
            return Listener{};
        }
        listener.port = ntohs(address.sin6_port);
    }
    if (::listen(listener.fd, 8) != 0) {
        ::close(listener.fd);
        return Listener{};
    }
    return listener;
}

void TestDefaultAndAdoptionLifecycle() {
    TcpConnection closed;
    CHECK(!closed.IsOpen(), "default connection is closed");
    CHECK(closed.ReadExact(nullptr, 0).ok(), "zero-byte read succeeds closed");
    CHECK(closed.WriteExact(nullptr, 0).ok(), "zero-byte write succeeds closed");
    CHECK(closed.Shutdown().ok() && closed.Shutdown().ok(),
          "Shutdown is idempotent on default object");
    closed.Close();
    closed.Close();

    CHECK(!TcpConnection::AdoptConnectedSocket(-1).ok(),
          "negative fd adoption fails");

    std::array<int, 2> datagrams{{-1, -1}};
    CHECK(::socketpair(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0,
                       datagrams.data()) == 0,
          "datagram socketpair succeeds");
    const int rejected_datagram = datagrams[0];
    auto datagram_result =
        TcpConnection::AdoptConnectedSocket(datagrams[0], Options());
    datagrams[0] = -1;
    CHECK(!datagram_result.ok(), "non-stream socket adoption fails");
    CHECK(IsClosedFd(rejected_datagram),
          "failed adoption closes transferred datagram fd");
    ::close(datagrams[1]);

    auto invalid_options_pair = MakeSocketPair();
    TcpConnectionOptions invalid_options = Options();
    invalid_options.read_timeout = -1ms;
    const int rejected_for_options = invalid_options_pair[0];
    auto invalid_options_result = TcpConnection::AdoptConnectedSocket(
        invalid_options_pair[0], invalid_options);
    invalid_options_pair[0] = -1;
    CHECK(!invalid_options_result.ok(), "negative read timeout is invalid");
    CHECK(IsClosedFd(rejected_for_options),
          "invalid-option adoption closes transferred fd");
    ::close(invalid_options_pair[1]);

    const int unconnected =
        ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
    CHECK(unconnected >= 0, "unconnected TCP socket creation succeeds");
    auto unconnected_result =
        TcpConnection::AdoptConnectedSocket(unconnected, Options());
    CHECK(!unconnected_result.ok(), "unconnected stream socket is rejected");
    CHECK(IsClosedFd(unconnected), "rejected unconnected socket is closed");

    auto sockets = MakeSocketPair();
    const int owned_fd = sockets[0];
    TcpConnection connection = Adopt(&sockets[0]);
    CHECK(connection.IsOpen(), "adopted stream connection is open");
    const int status_flags = ::fcntl(owned_fd, F_GETFL, 0);
    const int descriptor_flags = ::fcntl(owned_fd, F_GETFD, 0);
    CHECK(status_flags >= 0 && (status_flags & O_NONBLOCK) != 0,
          "adopted socket is non-blocking");
    CHECK(descriptor_flags >= 0 && (descriptor_flags & FD_CLOEXEC) != 0,
          "adopted socket is close-on-exec");
    std::uint8_t byte = 0;
    CHECK(connection.ReadExact(nullptr, 1).code() ==
              StatusCode::kInvalidArgument,
          "null nonzero read is invalid");
    CHECK(connection.WriteExact(nullptr, 1).code() ==
              StatusCode::kInvalidArgument,
          "null nonzero write is invalid");
    CHECK(connection.IsOpen(), "invalid arguments do not break connection");

    TcpConnection moved(std::move(connection));
    CHECK(!connection.IsOpen() && moved.IsOpen(),
          "move construction transfers fd ownership");
    byte = 0x41;
    CHECK(SendAll(sockets[1], &byte, 1), "peer sends after move construction");
    std::uint8_t received = 0;
    CHECK(moved.ReadExact(&received, 1).ok() && received == byte,
          "moved connection remains usable");

    auto replacement_sockets = MakeSocketPair();
    const int replaced_fd = replacement_sockets[0];
    TcpConnection replacement = Adopt(&replacement_sockets[0]);
    replacement = std::move(moved);
    CHECK(!moved.IsOpen() && replacement.IsOpen(),
          "move assignment transfers fd ownership");
    CHECK(IsClosedFd(replaced_fd),
          "move assignment closes previously owned destination fd");
    byte = 0x52;
    CHECK(SendAll(sockets[1], &byte, 1), "peer sends after move assignment");
    CHECK(replacement.ReadExact(&received, 1).ok() && received == byte,
          "move-assigned connection remains usable");

    CHECK(replacement.Shutdown().ok() && replacement.Shutdown().ok(),
          "Shutdown is idempotent on open object");
    CHECK(!replacement.IsOpen(), "Shutdown makes connection unusable");
    CHECK(replacement.ReadExact(&received, 1).code() ==
              StatusCode::kUnavailable,
          "I/O after Shutdown is unavailable");
    replacement.Close();
    replacement.Close();
    CHECK(replacement.Shutdown().ok(),
          "Shutdown remains safe after repeated Close");
    CHECK(IsClosedFd(owned_fd), "Close releases adopted fd once");
    ::close(sockets[1]);
    ::close(replacement_sockets[1]);
}

void TestDestructorAndRepeatedLifecycle() {
    auto sockets = MakeSocketPair();
    const int owned_fd = sockets[0];
    {
        TcpConnection connection = Adopt(&sockets[0]);
        CHECK(connection.IsOpen(), "scoped connection opens");
    }
    CHECK(IsClosedFd(owned_fd), "destructor closes owned fd");
    ::close(sockets[1]);

    const int before = CountOpenFds();
    for (int i = 0; i < 128; ++i) {
        auto pair = MakeSocketPair();
        const int current_fd = pair[0];
        {
            TcpConnection connection = Adopt(&pair[0]);
            CHECK(connection.IsOpen(), "loop adoption opens");
        }
        CHECK(IsClosedFd(current_fd), "loop destructor closes connection fd");
        ::close(pair[1]);
    }
    const int after = CountOpenFds();
    if (before >= 0 && after >= 0) {
        CHECK(after <= before + 1,
              "repeated create/destroy has no obvious fd leak");
    }
}

void ExerciseConnectedClient(const std::string& host, Listener* listener) {
    CHECK(listener != nullptr && listener->fd >= 0 && listener->port != 0,
          "listener is ready");
    auto client_result =
        TcpConnection::Connect(host, listener->port, Options());
    CHECK(client_result.ok(), "loopback Connect succeeds");
    TcpConnection client = std::move(client_result.value());
    CHECK(client.IsOpen(), "connected client is open");

    const int accepted = ::accept4(listener->fd, nullptr, nullptr, SOCK_CLOEXEC);
    CHECK(accepted >= 0, "listener accepts connected client");
    const std::string outbound = "client-to-server";
    CHECK(client.WriteExact(outbound.data(), outbound.size()).ok(),
          "connected client writes");
    std::string server_received(outbound.size(), '\0');
    CHECK(ReceiveAll(accepted, server_received.data(), server_received.size()) &&
              server_received == outbound,
          "accepted peer receives client data");

    const std::string inbound = "server-to-client";
    CHECK(SendAll(accepted, inbound.data(), inbound.size()),
          "accepted peer writes client data");
    std::string client_received(inbound.size(), '\0');
    CHECK(client.ReadExact(client_received.data(), client_received.size()).ok() &&
              client_received == inbound,
          "connected client reads peer data");
    ::close(accepted);
    client.Close();
}

void TestConnect() {
    CHECK(TcpConnection::Connect("", 12345, Options()).status().code() ==
              StatusCode::kInvalidArgument,
          "empty host is invalid");
    CHECK(TcpConnection::Connect("127.0.0.1", 0, Options()).status().code() ==
              StatusCode::kInvalidArgument,
          "zero port is invalid");
    TcpConnectionOptions invalid_connect = Options();
    invalid_connect.connect_timeout = -1ms;
    CHECK(TcpConnection::Connect("127.0.0.1", 1, invalid_connect)
              .status()
              .code() == StatusCode::kInvalidArgument,
          "negative connect timeout is invalid");
    TcpConnectionOptions invalid_write = Options();
    invalid_write.write_timeout = -1ms;
    CHECK(TcpConnection::Connect("127.0.0.1", 1, invalid_write)
              .status()
              .code() == StatusCode::kInvalidArgument,
          "negative write timeout is invalid");

    Listener ipv4 = MakeListener(AF_INET);
    CHECK(ipv4.fd >= 0, "IPv4 loopback listener starts");
    ExerciseConnectedClient("127.0.0.1", &ipv4);
    ::close(ipv4.fd);

    Listener localhost = MakeListener(AF_INET);
    CHECK(localhost.fd >= 0, "localhost candidate listener starts");
    ExerciseConnectedClient("localhost", &localhost);
    ::close(localhost.fd);

    Listener ipv6 = MakeListener(AF_INET6);
    if (ipv6.fd >= 0) {
        ExerciseConnectedClient("::1", &ipv6);
        ::close(ipv6.fd);
    } else {
        std::printf("tcp connection test: IPv6 loopback unavailable, skipped\n");
    }

    Listener closed_port = MakeListener(AF_INET);
    CHECK(closed_port.fd >= 0, "closed-port listener obtains an ephemeral port");
    const std::uint16_t port = closed_port.port;
    ::close(closed_port.fd);
    const int before = CountOpenFds();
    for (int i = 0; i < 16; ++i) {
        auto failed = TcpConnection::Connect("127.0.0.1", port, Options());
        CHECK(!failed.ok(), "closed loopback port connection fails");
        CHECK(failed.status().code() == StatusCode::kNetworkError ||
                  failed.status().code() == StatusCode::kUnavailable,
              "closed-port failure has network/timeout status");
    }
    const int after = CountOpenFds();
    if (before >= 0 && after >= 0) {
        CHECK(after <= before + 1,
              "failed Connect candidates do not leak descriptors");
    }
}

void TestReadExact() {
    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        const std::string message = "one complete read";
        CHECK(SendAll(sockets[1], message.data(), message.size()),
              "peer writes complete message");
        std::string output(message.size(), '\0');
        CHECK(connection.ReadExact(output.data(), output.size()).ok() &&
                  output == message,
              "ReadExact reads complete buffered message");
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0], Options(1000ms));
        const std::string message = "fragmented-read";
        std::thread writer([&] {
            for (char byte : message) {
                if (!SendAll(sockets[1], &byte, 1)) break;
                std::this_thread::sleep_for(5ms);
            }
        });
        std::string output(message.size(), '\0');
        Status status = connection.ReadExact(output.data(), output.size());
        writer.join();
        CHECK(status.ok() && output == message,
              "ReadExact combines many one-byte sends");
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        const char partial[] = {'a', 'b', 'c'};
        CHECK(SendAll(sockets[1], partial, sizeof(partial)),
              "peer sends partial read body");
        ::close(sockets[1]);
        char output[5]{};
        Status status = connection.ReadExact(output, sizeof(output));
        CHECK(status.code() == StatusCode::kNetworkError,
              "mid-read EOF is a network error");
        CHECK(!connection.IsOpen(), "mid-read EOF breaks connection");
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        ::close(sockets[1]);
        char output = 0;
        Status status = connection.ReadExact(&output, 1);
        CHECK(status.code() == StatusCode::kNetworkError,
              "EOF before any byte is a network error");
        CHECK(!connection.IsOpen(), "initial EOF breaks connection");
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0], Options(120ms));
        char output = 0;
        const auto start = std::chrono::steady_clock::now();
        Status status = connection.ReadExact(&output, 1);
        const auto elapsed = std::chrono::steady_clock::now() - start;
        CHECK(status.code() == StatusCode::kUnavailable,
              "read timeout maps to Unavailable");
        CHECK(elapsed >= 50ms && elapsed < 2s,
              "read timeout uses configured finite deadline");
        CHECK(!connection.IsOpen(), "read timeout breaks connection");
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0], Options(150ms));
        const char partial[] = {'x', 'y'};
        CHECK(SendAll(sockets[1], partial, sizeof(partial)),
              "peer sends data before partial-read timeout");
        char output[4]{};
        Status status = connection.ReadExact(output, sizeof(output));
        CHECK(status.code() == StatusCode::kUnavailable,
              "partial read timeout maps to Unavailable");
        CHECK(!connection.IsOpen(), "partial read timeout breaks connection");
        ::close(sockets[1]);
    }
}

void TestShutdownWakeupAndSingleDeadline() {
    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0], Options(5s));
        Status read_status;
        char output = 0;
        std::thread reader([&] { read_status = connection.ReadExact(&output, 1); });
        std::this_thread::sleep_for(100ms);
        const auto shutdown_start = std::chrono::steady_clock::now();
        CHECK(connection.Shutdown().ok(), "cross-thread Shutdown succeeds");
        reader.join();
        const auto shutdown_elapsed =
            std::chrono::steady_clock::now() - shutdown_start;
        CHECK(!read_status.ok(), "Shutdown wakes blocked ReadExact with error");
        CHECK(shutdown_elapsed < 1500ms,
              "Shutdown wakeup does not wait for original read timeout");
        CHECK(connection.Shutdown().ok(), "repeated Shutdown remains safe");
        connection.Close();
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0], Options(260ms));
        std::atomic<bool> stopped{false};
        std::thread slow_writer([&] {
            std::uint8_t value = 0;
            while (!stopped.load()) {
                std::this_thread::sleep_for(75ms);
                if (!SendAll(sockets[1], &value, 1)) break;
                ++value;
            }
        });
        std::array<std::uint8_t, 8> output{};
        const auto start = std::chrono::steady_clock::now();
        Status status = connection.ReadExact(output.data(), output.size());
        const auto elapsed = std::chrono::steady_clock::now() - start;
        stopped.store(true);
        connection.Shutdown();
        slow_writer.join();
        CHECK(status.code() == StatusCode::kUnavailable,
              "slow trickle exceeds one overall read deadline");
        CHECK(elapsed >= 150ms && elapsed < 1200ms,
              "partial progress does not reset read timeout");
        CHECK(!connection.IsOpen(), "deadline failure breaks connection");
        ::close(sockets[1]);
    }
}

void TestWriteExact() {
    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        const std::string message = "small-write";
        CHECK(connection.WriteExact(message.data(), message.size()).ok(),
              "WriteExact writes small message");
        std::string received(message.size(), '\0');
        CHECK(ReceiveAll(sockets[1], received.data(), received.size()) &&
                  received == message,
              "peer receives small WriteExact message");
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        int send_buffer = 4096;
        CHECK(::setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &send_buffer,
                           sizeof(send_buffer)) == 0,
              "small send buffer config succeeds");
        TcpConnection connection = Adopt(&sockets[0], Options(500ms, 5s));
        std::vector<std::uint8_t> payload(512 * 1024);
        for (std::size_t i = 0; i < payload.size(); ++i) {
            payload[i] = static_cast<std::uint8_t>(i % 251);
        }
        std::vector<std::uint8_t> received(payload.size());
        std::thread slow_reader([&] {
            std::size_t completed = 0;
            while (completed < received.size()) {
                const std::size_t chunk =
                    std::min<std::size_t>(1024, received.size() - completed);
                const ssize_t result =
                    ::recv(sockets[1], received.data() + completed, chunk, 0);
                if (result > 0) {
                    completed += static_cast<std::size_t>(result);
                    std::this_thread::sleep_for(1ms);
                    continue;
                }
                if (result < 0 && errno == EINTR) continue;
                break;
            }
        });
        Status status = connection.WriteExact(payload.data(), payload.size());
        slow_reader.join();
        CHECK(status.ok(), "WriteExact completes through repeated short writes");
        CHECK(received == payload, "slow peer receives exact large payload");
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        int send_buffer = 4096;
        ::setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &send_buffer,
                     sizeof(send_buffer));
        TcpConnection connection = Adopt(&sockets[0], Options(500ms, 150ms));
        std::vector<std::uint8_t> payload(4 * 1024 * 1024, 0x5a);
        const auto start = std::chrono::steady_clock::now();
        Status status = connection.WriteExact(payload.data(), payload.size());
        const auto elapsed = std::chrono::steady_clock::now() - start;
        CHECK(status.code() == StatusCode::kUnavailable,
              "write timeout maps to Unavailable");
        CHECK(elapsed >= 50ms && elapsed < 2s,
              "write timeout uses configured finite deadline");
        CHECK(!connection.IsOpen(), "write timeout breaks connection");
        ::close(sockets[1]);
    }

    {
        auto sockets = MakeSocketPair();
        TcpConnection connection = Adopt(&sockets[0]);
        ::close(sockets[1]);
        const char byte = 'x';
        Status status = connection.WriteExact(&byte, 1);
        CHECK(status.code() == StatusCode::kNetworkError,
              "write to closed peer is NetworkError without SIGPIPE");
        CHECK(!connection.IsOpen(), "closed-peer write breaks connection");
    }

    {
        auto sockets = MakeSocketPair();
        int send_buffer = 4096;
        ::setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &send_buffer,
                     sizeof(send_buffer));
        TcpConnection connection = Adopt(&sockets[0], Options(500ms, 3s));
        std::vector<std::uint8_t> payload(4 * 1024 * 1024, 0xa5);
        std::thread closing_reader([&] {
            std::array<std::uint8_t, 8192> received{};
            ReceiveAll(sockets[1], received.data(), received.size());
            ::close(sockets[1]);
        });
        Status status = connection.WriteExact(payload.data(), payload.size());
        closing_reader.join();
        CHECK(!status.ok(), "peer close during large write fails current write");
        CHECK(!connection.IsOpen(), "partial write failure breaks connection");
    }
}

}  // namespace

int main() {
    TestDefaultAndAdoptionLifecycle();
    TestDestructorAndRepeatedLifecycle();
    TestConnect();
    TestReadExact();
    TestShutdownWakeupAndSingleDeadline();
    TestWriteExact();

    std::printf("tidepool tcp connection test: %d checks passed\n", g_checks);
    return 0;
}
