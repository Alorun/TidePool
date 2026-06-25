#include "lru_eviction.h"

#include "tidepool/store/factory.h"

namespace tidepool {

std::unique_ptr<EvictionPolicy> MakeLruEviction() { return std::make_unique<LruEviction>(); }

void LruEviction::OnAccess(const BlockKey& key) {
    auto it = pos_.find(key);
    if (it == pos_.end()) return;
    // Move to front (most-recently-used).
    order_.splice(order_.begin(), order_, it->second);
}

void LruEviction::OnInsert(const BlockKey& key, size_t /*size_bytes*/) {
    // TODO: track size_bytes for byte-budget-aware victim selection.
    auto it = pos_.find(key);
    if (it != pos_.end()) {
        order_.splice(order_.begin(), order_, it->second);
        return;
    }
    order_.push_front(key);
    pos_[key] = order_.begin();
}

void LruEviction::OnRemove(const BlockKey& key) {
    auto it = pos_.find(key);
    if (it == pos_.end()) return;
    order_.erase(it->second);
    pos_.erase(it);
}

std::optional<BlockKey> LruEviction::Victim() {
    if (order_.empty()) return std::nullopt;
    return order_.back();
}

}  // namespace tidepool
