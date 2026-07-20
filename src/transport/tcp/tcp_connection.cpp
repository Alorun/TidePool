#include "tidepool/transport/tcp_connection.h"

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <utility>

namespace tidepool {
namespace {

using Clock = std::chrono::steady_clock;

class ScopedFd {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) ::close(fd_);
    }

    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;

    int get() const noexcept { return fd_; }
    int release() noexcept {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }

private:
    int fd_;
};

class Deadline {
public:
    explicit Deadline(std::chrono::milliseconds timeout) {
        const Clock::time_point now = Clock::now();
        const Clock::duration max_remaining = Clock::time_point::max() - now;
        const auto max_milliseconds =
            std::chrono::duration_cast<std::chrono::milliseconds>(max_remaining);
        if (timeout >= max_milliseconds) {
            expires_at_ = Clock::time_point::max();
        } else {
            expires_at_ = now + std::chrono::duration_cast<Clock::duration>(timeout);
        }
    }

    bool Expired() const noexcept { return Clock::now() >= expires_at_; }

    int PollTimeoutMilliseconds() const noexcept {
        const Clock::time_point now = Clock::now();
        if (now >= expires_at_) return 0;

        const Clock::duration remaining = expires_at_ - now;
        auto milliseconds =
            std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
        if (std::chrono::duration_cast<Clock::duration>(milliseconds) < remaining) {
            milliseconds += std::chrono::milliseconds(1);
        }
        if (milliseconds.count() > INT_MAX) return INT_MAX;
        return static_cast<int>(milliseconds.count());
    }

private:
    Clock::time_point expires_at_;
};

std::string ErrnoText(int error_number) {
    return std::string(std::strerror(error_number)) + " (errno=" +
           std::to_string(error_number) + ")";
}

std::string Endpoint(const std::string& host, std::uint16_t port) {
    return host + ":" + std::to_string(port);
}

Status ValidateOptions(const TcpConnectionOptions& options) {
    if (options.connect_timeout.count() < 0) {
        return Status::InvalidArgument(
            "TcpConnection: connect_timeout must be non-negative");
    }
    if (options.read_timeout.count() < 0) {
        return Status::InvalidArgument(
            "TcpConnection: read_timeout must be non-negative");
    }
    if (options.write_timeout.count() < 0) {
        return Status::InvalidArgument(
            "TcpConnection: write_timeout must be non-negative");
    }
    return Status::Ok();
}

Status ConfigureSocket(int fd) {
    const int status_flags = ::fcntl(fd, F_GETFL, 0);
    if (status_flags < 0) {
        return Status::NetworkError(
            "TcpConnection: fcntl(F_GETFL) failed: " + ErrnoText(errno));
    }
    if ((status_flags & O_NONBLOCK) == 0 &&
        ::fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) < 0) {
        return Status::NetworkError(
            "TcpConnection: fcntl(F_SETFL O_NONBLOCK) failed: " +
            ErrnoText(errno));
    }

    const int descriptor_flags = ::fcntl(fd, F_GETFD, 0);
    if (descriptor_flags < 0) {
        return Status::NetworkError(
            "TcpConnection: fcntl(F_GETFD) failed: " + ErrnoText(errno));
    }
    if ((descriptor_flags & FD_CLOEXEC) == 0 &&
        ::fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) < 0) {
        return Status::NetworkError(
            "TcpConnection: fcntl(F_SETFD FD_CLOEXEC) failed: " +
            ErrnoText(errno));
    }
    return Status::Ok();
}

Status ValidateConnectedStreamSocket(int fd) {
    int socket_type = 0;
    socklen_t socket_type_size = sizeof(socket_type);
    if (::getsockopt(fd, SOL_SOCKET, SO_TYPE, &socket_type,
                     &socket_type_size) < 0) {
        return Status::InvalidArgument(
            "TcpConnection::AdoptConnectedSocket: fd is not a socket: " +
            ErrnoText(errno));
    }
    if (socket_type != SOCK_STREAM) {
        return Status::InvalidArgument(
            "TcpConnection::AdoptConnectedSocket: socket is not SOCK_STREAM");
    }

    sockaddr_storage peer{};
    socklen_t peer_size = sizeof(peer);
    if (::getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &peer_size) < 0) {
        return Status::InvalidArgument(
            "TcpConnection::AdoptConnectedSocket: socket is not connected: " +
            ErrnoText(errno));
    }
    return Status::Ok();
}

Status PollReady(int fd, short requested_events, const Deadline& deadline,
                 const char* operation, std::size_t completed,
                 std::size_t target,
                 const std::atomic<bool>* externally_broken = nullptr,
                 bool let_socket_error_caller_inspect = false) {
    while (true) {
        if (externally_broken != nullptr &&
            externally_broken->load(std::memory_order_acquire)) {
            return Status::Unavailable(
                std::string("TcpConnection::") + operation +
                " interrupted because the connection was shut down (" +
                std::to_string(completed) + "/" + std::to_string(target) +
                " bytes completed)");
        }

        pollfd descriptor{};
        descriptor.fd = fd;
        descriptor.events = requested_events;
        const int poll_result =
            ::poll(&descriptor, 1, deadline.PollTimeoutMilliseconds());
        if (poll_result == 0) {
            return Status::Unavailable(
                std::string("TcpConnection::") + operation + " timeout (" +
                std::to_string(completed) + "/" + std::to_string(target) +
                " bytes completed)");
        }
        if (poll_result < 0) {
            if (errno == EINTR) continue;
            return Status::NetworkError(
                std::string("TcpConnection::") + operation +
                " poll failed after " + std::to_string(completed) + "/" +
                std::to_string(target) + " bytes: " + ErrnoText(errno));
        }

        if (externally_broken != nullptr &&
            externally_broken->load(std::memory_order_acquire)) {
            return Status::Unavailable(
                std::string("TcpConnection::") + operation +
                " interrupted because the connection was shut down (" +
                std::to_string(completed) + "/" + std::to_string(target) +
                " bytes completed)");
        }
        if ((descriptor.revents & POLLNVAL) != 0) {
            return Status::NetworkError(
                std::string("TcpConnection::") + operation +
                " poll reported POLLNVAL after " +
                std::to_string(completed) + "/" + std::to_string(target) +
                " bytes");
        }
        if ((descriptor.revents & requested_events) != 0) {
            return Status::Ok();
        }
        if (let_socket_error_caller_inspect &&
            (descriptor.revents & (POLLERR | POLLHUP)) != 0) {
            return Status::Ok();
        }
        if ((descriptor.revents & (POLLERR | POLLHUP)) != 0) {
            return Status::NetworkError(
                std::string("TcpConnection::") + operation +
                " peer closed or poll reported an error after " +
                std::to_string(completed) + "/" + std::to_string(target) +
                " bytes");
        }
        return Status::NetworkError(
            std::string("TcpConnection::") + operation +
            " poll returned unexpected events after " +
            std::to_string(completed) + "/" + std::to_string(target) +
            " bytes");
    }
}

int CreateNonBlockingSocket(const addrinfo& address) {
#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
    int fd = ::socket(address.ai_family,
                      address.ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                      address.ai_protocol);
    if (fd >= 0 || errno != EINVAL) return fd;
#endif
    return ::socket(address.ai_family, address.ai_socktype,
                    address.ai_protocol);
}

}  // namespace

TcpConnection::TcpConnection(
    int socket_fd, const TcpConnectionOptions& options) noexcept
    : socket_fd_(socket_fd), options_(options), broken_(false) {}

TcpConnection::~TcpConnection() { Close(); }

TcpConnection::TcpConnection(TcpConnection&& other) noexcept
    : socket_fd_(other.socket_fd_),
      options_(other.options_),
      broken_(other.broken_.load(std::memory_order_acquire)) {
    other.socket_fd_ = -1;
    other.broken_.store(true, std::memory_order_release);
}

TcpConnection& TcpConnection::operator=(TcpConnection&& other) noexcept {
    if (this == &other) return *this;
    Close();
    socket_fd_ = other.socket_fd_;
    options_ = other.options_;
    broken_.store(other.broken_.load(std::memory_order_acquire),
                  std::memory_order_release);
    other.socket_fd_ = -1;
    other.broken_.store(true, std::memory_order_release);
    return *this;
}

Result<TcpConnection> TcpConnection::Connect(
    const std::string& host, std::uint16_t port,
    const TcpConnectionOptions& options) {
    if (host.empty()) {
        return Status::InvalidArgument("TcpConnection::Connect: host is empty");
    }
    if (port == 0) {
        return Status::InvalidArgument("TcpConnection::Connect: port is zero");
    }
    if (Status status = ValidateOptions(options); !status.ok()) return status;

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    addrinfo* raw_addresses = nullptr;
    const std::string service = std::to_string(port);
    // getaddrinfo is intentionally synchronous in v1. The connect deadline
    // begins after DNS resolution and covers all candidate socket attempts as
    // one operation; it cannot bound time spent inside the resolver itself.
    const int gai_result =
        ::getaddrinfo(host.c_str(), service.c_str(), &hints, &raw_addresses);
    if (gai_result != 0) {
        return Status::NetworkError(
            "TcpConnection::Connect getaddrinfo failed for " +
            Endpoint(host, port) + ": " + ::gai_strerror(gai_result));
    }
    const auto free_addresses = [](addrinfo* addresses) {
        ::freeaddrinfo(addresses);
    };
    std::unique_ptr<addrinfo, decltype(free_addresses)> addresses(
        raw_addresses, free_addresses);

    const Deadline deadline(options.connect_timeout);
    std::string last_error = "no address candidates";
    for (const addrinfo* address = addresses.get(); address != nullptr;
         address = address->ai_next) {
        if (deadline.Expired()) {
            return Status::Unavailable(
                "TcpConnection::Connect timeout for " + Endpoint(host, port) +
                ": " + last_error);
        }

        ScopedFd candidate(CreateNonBlockingSocket(*address));
        if (candidate.get() < 0) {
            last_error = "socket failed: " + ErrnoText(errno);
            continue;
        }
        if (Status status = ConfigureSocket(candidate.get()); !status.ok()) {
            last_error = status.message();
            continue;
        }

        const int connect_result =
            ::connect(candidate.get(), address->ai_addr, address->ai_addrlen);
        if (connect_result == 0) {
            return TcpConnection(candidate.release(), options);
        }

        const int connect_error = errno;
        if (connect_error != EINPROGRESS && connect_error != EALREADY &&
            connect_error != EWOULDBLOCK && connect_error != EINTR) {
            last_error = "connect failed: " + ErrnoText(connect_error);
            continue;
        }

        Status ready = PollReady(candidate.get(), POLLOUT, deadline,
                                 "Connect", 0, 0, nullptr, true);
        if (!ready.ok()) {
            last_error = ready.message();
            if (ready.code() == StatusCode::kUnavailable) {
                return Status::Unavailable(
                    "TcpConnection::Connect timeout for " +
                    Endpoint(host, port) + ": " + last_error);
            }
            continue;
        }

        int socket_error = 0;
        socklen_t socket_error_size = sizeof(socket_error);
        if (::getsockopt(candidate.get(), SOL_SOCKET, SO_ERROR, &socket_error,
                         &socket_error_size) < 0) {
            last_error = "getsockopt(SO_ERROR) failed: " + ErrnoText(errno);
            continue;
        }
        if (socket_error != 0) {
            last_error = "connect failed: " + ErrnoText(socket_error);
            continue;
        }
        return TcpConnection(candidate.release(), options);
    }

    if (deadline.Expired()) {
        return Status::Unavailable(
            "TcpConnection::Connect timeout for " + Endpoint(host, port) +
            ": " + last_error);
    }
    return Status::NetworkError(
        "TcpConnection::Connect failed for " + Endpoint(host, port) +
        ": " + last_error);
}

Result<TcpConnection> TcpConnection::AdoptConnectedSocket(
    int socket_fd, const TcpConnectionOptions& options) {
    if (socket_fd < 0) {
        return Status::InvalidArgument(
            "TcpConnection::AdoptConnectedSocket: fd is negative");
    }
    ScopedFd owned(socket_fd);
    if (Status status = ValidateOptions(options); !status.ok()) return status;
    if (Status status = ValidateConnectedStreamSocket(owned.get());
        !status.ok()) {
        return status;
    }
    if (Status status = ConfigureSocket(owned.get()); !status.ok()) {
        return status;
    }
    return TcpConnection(owned.release(), options);
}

Status TcpConnection::ReadExact(void* output, std::size_t bytes) {
    if (bytes == 0) return Status::Ok();
    if (output == nullptr) {
        return Status::InvalidArgument(
            "TcpConnection::ReadExact: output is null for non-zero read");
    }
    if (socket_fd_ < 0) {
        return Status::Unavailable("TcpConnection::ReadExact: connection is closed");
    }
    if (broken_.load(std::memory_order_acquire)) {
        return Status::Unavailable("TcpConnection::ReadExact: connection is broken");
    }

    auto* destination = static_cast<std::uint8_t*>(output);
    const Deadline deadline(options_.read_timeout);
    std::size_t completed = 0;
    bool first_attempt = true;
    while (completed < bytes) {
        if (!first_attempt && deadline.Expired()) {
            MarkBroken();
            return Status::Unavailable(
                "TcpConnection::ReadExact timeout (" +
                std::to_string(completed) + "/" + std::to_string(bytes) +
                " bytes completed)");
        }
        first_attempt = false;

        const std::size_t remaining = bytes - completed;
        const std::size_t chunk = std::min(
            remaining, static_cast<std::size_t>(
                           std::numeric_limits<ssize_t>::max()));
        const ssize_t read_result =
            ::recv(socket_fd_, destination + completed, chunk, 0);
        if (read_result > 0) {
            completed += static_cast<std::size_t>(read_result);
            continue;
        }
        if (read_result == 0) {
            MarkBroken();
            return Status::NetworkError(
                "TcpConnection::ReadExact peer EOF (" +
                std::to_string(completed) + "/" + std::to_string(bytes) +
                " bytes completed)");
        }

        const int read_error = errno;
        if (read_error == EINTR) continue;
        if (read_error == EAGAIN || read_error == EWOULDBLOCK) {
            Status ready = PollReady(socket_fd_, POLLIN, deadline, "ReadExact",
                                     completed, bytes, &broken_);
            if (ready.ok()) continue;
            MarkBroken();
            return ready;
        }

        MarkBroken();
        return Status::NetworkError(
            "TcpConnection::ReadExact recv failed after " +
            std::to_string(completed) + "/" + std::to_string(bytes) +
            " bytes: " + ErrnoText(read_error));
    }
    return Status::Ok();
}

Status TcpConnection::WriteExact(const void* input, std::size_t bytes) {
    if (bytes == 0) return Status::Ok();
    if (input == nullptr) {
        return Status::InvalidArgument(
            "TcpConnection::WriteExact: input is null for non-zero write");
    }
    if (socket_fd_ < 0) {
        return Status::Unavailable("TcpConnection::WriteExact: connection is closed");
    }
    if (broken_.load(std::memory_order_acquire)) {
        return Status::Unavailable("TcpConnection::WriteExact: connection is broken");
    }

    const auto* source = static_cast<const std::uint8_t*>(input);
    const Deadline deadline(options_.write_timeout);
    std::size_t completed = 0;
    bool first_attempt = true;
    while (completed < bytes) {
        if (!first_attempt && deadline.Expired()) {
            MarkBroken();
            return Status::Unavailable(
                "TcpConnection::WriteExact timeout (" +
                std::to_string(completed) + "/" + std::to_string(bytes) +
                " bytes completed)");
        }
        first_attempt = false;

        const std::size_t remaining = bytes - completed;
        const std::size_t chunk = std::min(
            remaining, static_cast<std::size_t>(
                           std::numeric_limits<ssize_t>::max()));
        const ssize_t write_result =
            ::send(socket_fd_, source + completed, chunk, MSG_NOSIGNAL);
        if (write_result > 0) {
            completed += static_cast<std::size_t>(write_result);
            continue;
        }
        if (write_result == 0) {
            MarkBroken();
            return Status::NetworkError(
                "TcpConnection::WriteExact send made no progress (" +
                std::to_string(completed) + "/" + std::to_string(bytes) +
                " bytes completed)");
        }

        const int write_error = errno;
        if (write_error == EINTR) continue;
        if (write_error == EAGAIN || write_error == EWOULDBLOCK) {
            Status ready = PollReady(socket_fd_, POLLOUT, deadline,
                                     "WriteExact", completed, bytes, &broken_);
            if (ready.ok()) continue;
            MarkBroken();
            return ready;
        }

        MarkBroken();
        return Status::NetworkError(
            "TcpConnection::WriteExact send failed after " +
            std::to_string(completed) + "/" + std::to_string(bytes) +
            " bytes: " + ErrnoText(write_error));
    }
    return Status::Ok();
}

Status TcpConnection::Shutdown() {
    if (socket_fd_ < 0) {
        broken_.store(true, std::memory_order_release);
        return Status::Ok();
    }
    if (broken_.exchange(true, std::memory_order_acq_rel)) {
        return Status::Ok();
    }
    if (::shutdown(socket_fd_, SHUT_RDWR) == 0) return Status::Ok();
    const int shutdown_error = errno;
    if (shutdown_error == ENOTCONN || shutdown_error == EINVAL) {
        return Status::Ok();
    }
    return Status::NetworkError(
        "TcpConnection::Shutdown failed: " + ErrnoText(shutdown_error));
}

void TcpConnection::Close() noexcept {
    broken_.store(true, std::memory_order_release);
    if (socket_fd_ < 0) return;
    const int fd = socket_fd_;
    socket_fd_ = -1;
    // On Linux an fd is released even if close reports EINTR; retrying could
    // accidentally close a newly reused descriptor.
    ::close(fd);
}

bool TcpConnection::IsOpen() const noexcept {
    return socket_fd_ >= 0 && !broken_.load(std::memory_order_acquire);
}

void TcpConnection::MarkBroken() noexcept {
    if (socket_fd_ < 0) {
        broken_.store(true, std::memory_order_release);
        return;
    }
    if (!broken_.exchange(true, std::memory_order_acq_rel)) {
        const int saved_errno = errno;
        ::shutdown(socket_fd_, SHUT_RDWR);
        errno = saved_errno;
    }
}

}  // namespace tidepool
