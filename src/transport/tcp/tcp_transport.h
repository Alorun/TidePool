// tcp_transport.h — MVP Transfer Engine over plain TCP sockets.
// Plane: DATA. Concrete implementation of the Transport ABC.
//
// Emulates one-sided read/write by issuing request/response messages to a small
// data-plane server running on each storage node (see apps/tidepool_node). This
// is intentionally simple; the RDMA implementation replaces it for true
// one-sided, zero-copy transfers behind the same interface.
#pragma once

#include <cstddef>

#include "tidepool/transport/transport.h"

namespace tidepool {

class TcpTransport : public Transport {
public:
    TcpTransport() = default;
    ~TcpTransport() override = default;

    Status RegisterMem(void* addr, size_t length, MemRegion* out) override;
    Status DeregisterMem(const MemRegion& region) override;
    Status ReadRemote(const RemoteRef& remote, void* dst, size_t length) override;
    Status WriteRemote(const RemoteRef& remote, const void* src, size_t length) override;
    const char* name() const override { return "tcp"; }
};

}  // namespace tidepool
