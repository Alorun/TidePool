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
    uint64_t put_count = 0;
    uint64_t get_count = 0;
    uint64_t evict_count = 0;
    uint64_t hits = 0;
    uint64_t misses = 0;
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

    // Store `block` under `key`. On success, `*out_handle` receives the
    // tier-local handle to embed in a Location.
    // TODO: support in-place / zero-copy ingestion from a registered MemRegion.
    virtual Status Put(const BlockKey& key, const Block& block, uint64_t* out_handle) = 0;

    // Fetch the block for `key` into the caller-owned `dst`. On success, `*out`
    // is a read-only view over the filled prefix of `dst` (out->data ==
    // dst.data, out->size == the block's byte size). Returns kNotFound if
    // absent in THIS tier, or kInvalidArgument if dst is null / smaller than
    // the block.
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

    virtual TierStats Stats() const = 0;
};

}  // namespace tidepool
