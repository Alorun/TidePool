// transport.h — Transfer Engine abstraction. Plane: DATA (hot path).
//
// The Transfer Engine is the unified data-movement primitive for the pool. It
// hides *how* bytes cross the wire (or stay local) behind register / read /
// write of remote memory. TCP is the MVP implementation; an RDMA (libibverbs)
// implementation plugs in behind the SAME abstract base class and is left as a
// stub for now.
//
// This is a pluggable seam: Transport is a pure abstract base class so that
// TCP, RDMA, and future transports are drop-in interchangeable.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "tidepool/api/location.h"
#include "tidepool/api/status.h"

namespace tidepool {

// Handle to a locally-registered memory region. For RDMA this wraps an MR
// (lkey/rkey); for TCP it is a bookkeeping token only.
struct MemRegion {
    void* addr = nullptr;
    size_t length = 0;
    uint32_t lkey = 0;   // local key (RDMA); 0 for TCP
    uint32_t rkey = 0;   // remote key advertised to peers (RDMA); 0 for TCP
    uint64_t token = 0;  // implementation-private registration id
};

// Description of a remote buffer to read from / write to. The fields used
// depend on the transport: TCP uses {node address + handle}, RDMA additionally
// uses {remote_addr, rkey}.
struct RemoteRef {
    NodeId node_id;
    std::string address;       // "host:port"
    uint64_t handle = 0;       // tier-local handle on the remote (see Location)
    uint64_t remote_addr = 0;  // remote virtual address (RDMA)
    uint32_t rkey = 0;         // remote key (RDMA)
};

class Transport {
public:
    virtual ~Transport() = default;

    // Register a local buffer for transfer. For RDMA this pins + registers the
    // MR; for TCP it is a no-op that just records the region.
    // TODO: support partial / scatter-gather registration.
    virtual Status RegisterMem(void* addr, size_t length, MemRegion* out) = 0;

    // Release a previously registered region.
    virtual Status DeregisterMem(const MemRegion& region) = 0;

    // One-sided read: copy `length` bytes from `remote` into local `dst`.
    // TODO: async/completion-queue variant for pipelining on the hot path.
    virtual Status ReadRemote(const RemoteRef& remote, void* dst, size_t length) = 0;

    // One-sided write: copy `length` bytes from local `src` to `remote`.
    virtual Status WriteRemote(const RemoteRef& remote, const void* src, size_t length) = 0;

    // Human-readable transport name, e.g. "tcp" / "rdma".
    virtual const char* name() const = 0;
};

}  // namespace tidepool
