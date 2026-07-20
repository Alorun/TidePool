// tcp_connection.h — non-blocking TCP socket ownership and deadline-based I/O.
#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

#include "tidepool/api/status.h"

namespace tidepool {

struct TcpConnectionOptions {
    std::chrono::milliseconds connect_timeout{5000};
    std::chrono::milliseconds read_timeout{30000};
    std::chrono::milliseconds write_timeout{30000};
};

// Owns one connected, non-blocking stream socket. ReadExact, WriteExact,
// Connect, Close, and moves must not race with each other. Shutdown is the one
// operation allowed from another thread: it wakes a blocked poll/recv/send.
class TcpConnection {
public:
    static Result<TcpConnection> Connect(
        const std::string& host, std::uint16_t port,
        const TcpConnectionOptions& options = {});

    // Ownership transfers at function entry. On every failure path this
    // function closes socket_fd, so the caller must never close it afterward.
    static Result<TcpConnection> AdoptConnectedSocket(
        int socket_fd, const TcpConnectionOptions& options = {});

    TcpConnection() noexcept = default;
    ~TcpConnection();

    TcpConnection(TcpConnection&& other) noexcept;
    TcpConnection& operator=(TcpConnection&& other) noexcept;

    TcpConnection(const TcpConnection&) = delete;
    TcpConnection& operator=(const TcpConnection&) = delete;

    Status ReadExact(void* output, std::size_t bytes);
    Status WriteExact(const void* input, std::size_t bytes);

    // Idempotent and safe to call concurrently with ReadExact/WriteExact.
    // Shutdown marks the connection unusable but leaves final fd release to
    // Close or the destructor.
    Status Shutdown();

    // Idempotent. The caller must ensure no I/O or Shutdown call is active.
    void Close() noexcept;

    // False after a fatal I/O error or Shutdown, even before Close releases fd.
    bool IsOpen() const noexcept;

private:
    TcpConnection(int socket_fd, const TcpConnectionOptions& options) noexcept;

    void MarkBroken() noexcept;

    int socket_fd_ = -1;
    TcpConnectionOptions options_;
    std::atomic<bool> broken_{true};
};

}  // namespace tidepool
