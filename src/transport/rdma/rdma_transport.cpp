// RDMA transport stub. When built with -DTIDEPOOL_WITH_RDMA=ON the target links
// libibverbs; the actual verbs calls are still TODO. Without the flag this file
// compiles to pure NotImplemented stubs and needs no system dependency.
#include "rdma_transport.h"

#include "tidepool/transport/factory.h"

#ifdef TIDEPOOL_WITH_RDMA
#include <infiniband/verbs.h>  // provided by libibverbs
#endif

namespace tidepool {

std::unique_ptr<Transport> MakeRdmaTransport() { return std::make_unique<RdmaTransport>(); }

Status RdmaTransport::RegisterMem(void* /*addr*/, size_t /*length*/, MemRegion* /*out*/) {
    // TODO: ibv_reg_mr() the buffer, fill MemRegion.lkey/rkey from the MR.
    return Status::NotImplemented("RdmaTransport::RegisterMem");
}

Status RdmaTransport::DeregisterMem(const MemRegion& /*region*/) {
    // TODO: ibv_dereg_mr().
    return Status::NotImplemented("RdmaTransport::DeregisterMem");
}

Status RdmaTransport::ReadRemote(const RemoteRef& /*remote*/, void* /*dst*/, size_t /*length*/) {
    // TODO: post an RDMA READ work request (remote_addr + rkey), poll the CQ.
    return Status::NotImplemented("RdmaTransport::ReadRemote");
}

Status RdmaTransport::WriteRemote(const RemoteRef& /*remote*/, const void* /*src*/, size_t /*length*/) {
    // TODO: post an RDMA WRITE work request (remote_addr + rkey), poll the CQ.
    return Status::NotImplemented("RdmaTransport::WriteRemote");
}

}  // namespace tidepool
