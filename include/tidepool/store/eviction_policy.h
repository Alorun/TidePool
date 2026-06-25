// eviction_policy.h — Pluggable cache replacement policy. Plane: DATA
// (node-internal). Decides, when a tier is full, which block to demote/drop.
//
// The policy only tracks *keys and sizes*; it never touches block bytes. The
// StorageNode/Tier asks Victim() for a candidate, then performs the actual
// demotion (DRAM->SSD) or eviction. LRU is provided first; a cost-aware policy
// (recompute cost vs. size vs. recency) shares this interface as a stub.
//
// Pluggable seam: EvictionPolicy is a pure abstract base class.
#pragma once

#include <cstddef>
#include <optional>

#include "tidepool/api/block_key.h"

namespace tidepool {

class EvictionPolicy {
public:
    virtual ~EvictionPolicy() = default;

    // Record that `key` was just read/used (recency/frequency signal).
    virtual void OnAccess(const BlockKey& key) = 0;

    // Record that `key` (of `size_bytes`) was just inserted into the tier.
    virtual void OnInsert(const BlockKey& key, size_t size_bytes) = 0;

    // Record that `key` left the tier (already evicted/demoted elsewhere) so
    // the policy can forget its bookkeeping. Default: treat as no-op-able.
    virtual void OnRemove(const BlockKey& key) = 0;

    // Return the next block to evict, or std::nullopt if the policy has no
    // candidate (e.g. empty). Does not mutate the tier itself.
    // TODO(cost-aware): factor in recompute cost and block size, not just
    // recency.
    virtual std::optional<BlockKey> Victim() = 0;

    virtual const char* name() const = 0;
};

}  // namespace tidepool
