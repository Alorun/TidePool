// dram_tier.h — In-process DRAM storage tier. Plane: DATA (node-internal).
// The hot, in-memory level. Concrete implementation of the Tier ABC.
#pragma once

#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "tidepool/store/tier.h"

namespace tidepool {

class DramTier : public Tier {
public:
    explicit DramTier(uint64_t capacity_bytes) { stats_.capacity_bytes = capacity_bytes; }
    ~DramTier() override = default;

    TierType type() const override { return TierType::kDram; }
    Status Put(const BlockKey& key, const Block& block, uint64_t* out_handle) override;
    Status Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out) override;
    Status Evict(const BlockKey& key) override;
    TierStats Stats() const override;

private:
    mutable std::mutex mu_;
    // Owning store keyed by block key. `handle` is a monotonically increasing,
    // STABLE opaque id (see location.h handle contract) — NOT a pointer into
    // this map. We never expose iterators/pointers into store_, so a rehash or
    // eviction can't dangle a Location held by some other node/client; reads
    // re-look-up by key under the lock. The tier owns the byte buffers.
    std::unordered_map<BlockKey, Block> store_;
    uint64_t next_handle_ = 1;
    TierStats stats_;
};

}  // namespace tidepool
