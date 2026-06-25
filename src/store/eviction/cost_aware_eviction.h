// cost_aware_eviction.h — Cost-aware replacement policy. Plane: DATA
// (node-internal). STUB: implements the EvictionPolicy ABC but defers victim
// selection logic (ROADMAP stage 3).
//
// Intended to weigh recompute cost vs. block size vs. recency so that expensive
// (e.g. long-prefix) blocks survive longer than cheap ones — the distributed
// analogue of GDSF/cost-aware caching.
#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>

#include "tidepool/store/eviction_policy.h"

namespace tidepool {

struct CostStats {
    size_t size_bytes = 0;
    double recompute_cost = 0.0;  // estimated cost to regenerate this block
    uint64_t last_access_seq = 0;
};

class CostAwareEviction : public EvictionPolicy {
public:
    void OnAccess(const BlockKey& key) override;
    void OnInsert(const BlockKey& key, size_t size_bytes) override;
    void OnRemove(const BlockKey& key) override;
    std::optional<BlockKey> Victim() override;
    const char* name() const override { return "cost-aware(stub)"; }

    // TODO: let the StorageNode feed recompute-cost estimates per key.
    void SetRecomputeCost(const BlockKey& key, double cost);

private:
    std::unordered_map<BlockKey, CostStats> stats_;
    uint64_t seq_ = 0;
};

}  // namespace tidepool
