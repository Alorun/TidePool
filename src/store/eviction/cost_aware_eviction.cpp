#include "cost_aware_eviction.h"

#include "tidepool/store/factory.h"

namespace tidepool {

std::unique_ptr<EvictionPolicy> MakeCostAwareEviction() { return std::make_unique<CostAwareEviction>(); }

void CostAwareEviction::OnAccess(const BlockKey& key) {
    auto it = stats_.find(key);
    if (it != stats_.end()) it->second.last_access_seq = ++seq_;
}

void CostAwareEviction::OnInsert(const BlockKey& key, size_t size_bytes) {
    CostStats& s = stats_[key];
    s.size_bytes = size_bytes;
    s.last_access_seq = ++seq_;
}

void CostAwareEviction::OnRemove(const BlockKey& key) { stats_.erase(key); }

void CostAwareEviction::SetRecomputeCost(const BlockKey& key, double cost) { stats_[key].recompute_cost = cost; }

Result<BlockKey> CostAwareEviction::SelectVictim() {
    // TODO(stage 3): pick argmin over a cost function such as
    //   score = recompute_cost / size_bytes  (favor evicting cheap, large
    //   blocks)
    // combined with an aging term on last_access_seq. Stubbed for now.
    return Status::NotFound("cost-aware victim selection is not implemented");
}

Status CostAwareEviction::CommitVictim(const BlockKey&) {
    return Status::InvalidArgument("cost-aware policy has no reserved victim");
}

Status CostAwareEviction::CancelVictim(const BlockKey&) {
    return Status::InvalidArgument("cost-aware policy has no reserved victim");
}

}  // namespace tidepool
