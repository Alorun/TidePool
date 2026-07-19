// tier.h — Abstract storage tier. Plane: DATA (node-internal).
//
// A Tier is one physical storage level *inside a single storage node*
// (DRAM, SSD, future GPU). This is the VERTICAL, intra-node dimension — exactly
// the "tiered/multi-level cache" concept, here scoped to a single node and
// reduced to a node-internal submodule. It is NOT the project's distributed
// sharing mechanism (that is the hash ring + coordinator, horizontal
// dimension).
//
// Pluggable seam: Tier is a pure abstract base class so DRAM, SSD, and future
// tiers are interchangeable and new levels can be inserted.
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>

#include "tidepool/api/block.h"
#include "tidepool/api/block_key.h"
#include "tidepool/api/buffer.h"
#include "tidepool/api/location.h"
#include "tidepool/api/status.h"

namespace tidepool {

struct TierStats {
    uint64_t num_blocks = 0;
    uint64_t used_bytes = 0;
    uint64_t capacity_bytes = 0;
    // Operation counters count successful completions only. NotFound,
    // kOutOfCapacity and I/O errors do not increment them.
    uint64_t put_count = 0;
    uint64_t get_count = 0;
    uint64_t evict_count = 0;
    uint64_t hits = 0;
    uint64_t misses = 0;
};

struct BlockInfo {
    BlockMetadata metadata;
    size_t payload_size = 0;
    uint64_t handle = 0;
};

class Tier {
public:
    virtual ~Tier() = default;

    virtual TierType type() const = 0;

    // Lifecycle hooks. In-memory tiers need no external initialization, so the
    // default implementation is always ready and Open/Close are idempotent
    // no-ops. Resource-owning tiers override all three methods.
    virtual Status Open() { return Status::Ok(); }
    virtual Status Close() { return Status::Ok(); }
    virtual bool IsReady() const { return true; }

    // Return metadata and payload size without copying payload bytes.
    virtual Result<BlockInfo> Probe(const BlockKey& key) = 0;

    // Store `block` under `key`. On success, `*out_handle` receives the
    // tier-local handle to embed in a Location.
    // TODO: support in-place / zero-copy ingestion from a registered MemRegion.
    virtual Status Put(const BlockKey& key, const Block& block, uint64_t* out_handle) = 0;

    // Fetch the block for `key` into caller-owned `dst`. `out` is required.
    // Success guarantees out->size <= dst.capacity. A short buffer returns
    // kOutOfCapacity; callers use Probe() to obtain the required size. Every
    // failure clears `out`. A zero-length payload succeeds with
    // {dst.data=nullptr, dst.capacity=0}.
    //
    // WHY this shape (changed from `Result<Block> Get`): the read path is the
    // hottest path and transfer efficiency is the point of the project.
    // Returning a Block by value bakes a copy into the public contract and
    // forecloses zero-copy / RDMA-into-registered-memory. Taking a caller-owned
    // destination keeps allocation+ownership with the caller and lets a remote
    // Get land bytes straight into a registered MutableBuffer via the
    // Transport.
    // TODO(zero-copy): add a local overload returning a BlockView directly over
    // tier-internal storage (no copy) for owner==self hits, under a
    // refcount/lock.
    virtual Status Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out) = 0;

    // Drop `key` from this tier (does not cascade to other tiers; the
    // StorageNode orchestrates demotion across tiers).
    virtual Status Evict(const BlockKey& key) = 0;

    // Preflight/commit pair used by StorageNode's persistent demotion commit.
    // ValidateEraseExisting may fail before the SSD write. Once it succeeds,
    // and while StorageNode retains exclusive node ownership, EraseExisting is
    // a no-throw, no-allocation commit operation. Violating its precondition is
    // an unrecoverable internal invariant failure.
    virtual Status ValidateEraseExisting(const BlockKey& key) const = 0;
    virtual void EraseExisting(const BlockKey& key) noexcept = 0;

    // Enumerate currently valid entries for node index reconstruction. Tiers
    // with no recoverable entries may use the empty default.
    virtual Status VisitEntries(
        const std::function<Status(const BlockKey&, const BlockInfo&)>& /*visitor*/) const {
        return Status::Ok();
    }

    virtual TierStats Stats() const = 0;
};

}  // namespace tidepool
