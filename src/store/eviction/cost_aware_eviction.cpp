#include "cost_aware_eviction.h"

#include <exception>

#include "tidepool/store/factory.h"

namespace tidepool {

std::unique_ptr<EvictionPolicy> MakeCostAwareEviction() { return std::make_unique<CostAwareEviction>(); }

void CostAwareEviction::OnAccess(const BlockKey& key) noexcept {
    auto it = stats_.find(key);
    if (it != stats_.end()) it->second.last_access_seq = ++seq_;
}

Status CostAwareEviction::OnInsert(const BlockKey&, size_t) {
    return Status::NotImplemented("cost-aware policy is not implemented");
}

Status CostAwareEviction::OnPromotionCommitted(const BlockKey&, size_t) {
    return Status::NotImplemented("cost-aware policy is not implemented");
}

void CostAwareEviction::OnRemove(const BlockKey& key) noexcept { stats_.erase(key); }

void CostAwareEviction::SetRecomputeCost(const BlockKey& key, double cost) { stats_[key].recompute_cost = cost; }

Result<BlockKey> CostAwareEviction::SelectVictim(const std::optional<BlockKey>&) {
    // TODO(stage 3): pick argmin over a cost function such as
    //   score = recompute_cost / size_bytes  (favor evicting cheap, large
    //   blocks)
    // combined with an aging term on last_access_seq. Stubbed for now.
    return Status::NotFound("cost-aware victim selection is not implemented");
}

Status CostAwareEviction::ValidateVictimCommit(const BlockKey&) const {
    return Status::InvalidArgument("cost-aware policy has no reserved victim");
}

void CostAwareEviction::CommitVictim(const BlockKey&) noexcept { std::terminate(); }

Status CostAwareEviction::CancelVictim(const BlockKey&) {
    return Status::InvalidArgument("cost-aware policy has no reserved victim");
}

}  // namespace tidepool
