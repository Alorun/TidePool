#include "lru_eviction.h"

#include "tidepool/store/factory.h"

namespace tidepool {

std::unique_ptr<EvictionPolicy> MakeLruEviction() { return std::make_unique<LruEviction>(); }

void LruEviction::OnAccess(const BlockKey& key) {
    auto it = pos_.find(key);
    if (it == pos_.end()) return;
    if (reserved_ && *reserved_ == key) return;
    // Move to front (most-recently-used).
    order_.splice(order_.begin(), order_, it->second);
}

void LruEviction::OnInsert(const BlockKey& key, size_t /*size_bytes*/) {
    // TODO: track size_bytes for byte-budget-aware victim selection.
    auto it = pos_.find(key);
    if (it != pos_.end()) {
        if (reserved_ && *reserved_ == key) return;
        order_.splice(order_.begin(), order_, it->second);
        return;
    }
    order_.push_front(key);
    pos_[key] = order_.begin();
}

void LruEviction::OnRemove(const BlockKey& key) {
    auto it = pos_.find(key);
    if (it == pos_.end()) return;
    if (reserved_ && *reserved_ == key) reserved_.reset();
    order_.erase(it->second);
    pos_.erase(it);
}

Result<BlockKey> LruEviction::SelectVictim() {
    if (reserved_) {
        return Status(StatusCode::kAlreadyExists, "LRU already has a reserved victim");
    }
    if (order_.empty()) return Status::NotFound("LRU has no resident victim");
    reserved_ = order_.back();
    return *reserved_;
}

Status LruEviction::CommitVictim(const BlockKey& key) {
    if (!reserved_ || *reserved_ != key) {
        return Status::InvalidArgument("LRU commit does not match the reserved victim");
    }
    auto it = pos_.find(key);
    if (it == pos_.end()) {
        return Status::Internal("LRU reserved victim is not resident");
    }
    order_.erase(it->second);
    pos_.erase(it);
    reserved_.reset();
    return Status::Ok();
}

Status LruEviction::CancelVictim(const BlockKey& key) {
    if (!reserved_ || *reserved_ != key) {
        return Status::InvalidArgument("LRU cancel does not match the reserved victim");
    }
    reserved_.reset();
    return Status::Ok();
}

}  // namespace tidepool
