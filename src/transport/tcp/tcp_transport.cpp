// Concrete impl header lives next to this .cpp under src/ (kept out of the
// public include/ tree on purpose).
#include "tcp_transport.h"

#include "tidepool/transport/factory.h"

namespace tidepool {

std::unique_ptr<Transport> MakeTcpTransport() { return std::make_unique<TcpTransport>(); }

Status TcpTransport::RegisterMem(void* addr, size_t length, MemRegion* out) {
    // For TCP there is nothing to pin; just record the region so callers can
    // use a uniform API with the RDMA path.
    if (out == nullptr) return Status::InvalidArgument("out == nullptr");
    out->addr = addr;
    out->length = length;
    out->lkey = 0;
    out->rkey = 0;
    out->token = 0;
    // TODO: maintain a registry for validation / lifetime tracking.
    return Status::Ok();
}

Status TcpTransport::DeregisterMem(const MemRegion&) {
    // No-op for TCP.
    return Status::Ok();
}

Status TcpTransport::ReadRemote(const RemoteRef& /*remote*/, void* /*dst*/, size_t /*length*/) {
    // TODO: open/reuse a socket to remote.address, send a READ(handle, length)
    // request, stream the bytes into dst.
    return Status::NotImplemented("TcpTransport::ReadRemote");
}

Status TcpTransport::WriteRemote(const RemoteRef& /*remote*/, const void* /*src*/, size_t /*length*/) {
    // TODO: open/reuse a socket to remote.address, send a WRITE(handle, length)
    // header followed by the payload.
    return Status::NotImplemented("TcpTransport::WriteRemote");
}

}  // namespace tidepool
