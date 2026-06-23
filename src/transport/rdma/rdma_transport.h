// rdma_transport.h — RDMA Transfer Engine (libibverbs). Plane: DATA.
// STUB ONLY in this scaffold — implements the Transport ABC so it is drop-in
// interchangeable with TcpTransport, but every method returns NotImplemented
// until the verbs path is built (ROADMAP stage 3).
#pragma once

#include <cstddef>

#include "tidepool/transport/transport.h"

namespace tidepool {

class RdmaTransport : public Transport {
public:
    RdmaTransport() = default;
    ~RdmaTransport() override = default;

    Status RegisterMem(void* addr, size_t length, MemRegion* out) override;
    Status DeregisterMem(const MemRegion& region) override;
    Status ReadRemote(const RemoteRef& remote, void* dst, size_t length) override;
    Status WriteRemote(const RemoteRef& remote, const void* src, size_t length) override;
    const char* name() const override { return "rdma"; }
};

}  // namespace tidepool
