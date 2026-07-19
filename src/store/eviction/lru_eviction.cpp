#include "lru_eviction.h"

#include <exception>
#include <new>

#include "tidepool/store/factory.h"

namespace tidepool {

std::unique_ptr<EvictionPolicy> MakeLruEviction() { return std::make_unique<LruEviction>(); }

void LruEviction::OnAccess(const BlockKey& key) noexcept {
    auto it = pos_.find(key);
    if (it == pos_.end()) return;
    if (reserved_ && *reserved_ == key) return;
    // Move to front (most-recently-used).
    order_.splice(order_.begin(), order_, it->second);
}

Status LruEviction::OnInsert(const BlockKey& key, size_t /*size_bytes*/) {
    // TODO: track size_bytes for byte-budget-aware victim selection.
    auto it = pos_.find(key);
    if (it != pos_.end()) {
        if (reserved_ && *reserved_ == key) {
            return Status::InvalidArgument("LRU cannot insert its reserved victim");
        }
        order_.splice(order_.begin(), order_, it->second);
        return Status::Ok();
    }
    try {
        order_.push_front(key);
        try {
            auto [inserted, ok] = pos_.emplace(key, order_.begin());
            (void)inserted;
            if (!ok) {
                order_.pop_front();
                return Status::Internal("LRU insert found an unexpected duplicate");
            }
        } catch (...) {
            order_.pop_front();
            throw;
        }
    } catch (const std::bad_alloc&) {
        return Status(StatusCode::kOutOfCapacity, "LRU insert allocation failed");
    }
    return Status::Ok();
}

Status LruEviction::OnPromotionCommitted(const BlockKey& key, size_t size_bytes) {
    return OnInsert(key, size_bytes);
}

void LruEviction::OnRemove(const BlockKey& key) noexcept {
    auto it = pos_.find(key);
    if (it == pos_.end()) return;
    if (reserved_ && *reserved_ == key) reserved_.reset();
    order_.erase(it->second);
    pos_.erase(it);
}

Result<BlockKey> LruEviction::SelectVictim(const std::optional<BlockKey>& excluded) {
    if (reserved_) {
        return Status(StatusCode::kAlreadyExists, "LRU already has a reserved victim");
    }
    if (order_.empty()) return Status::NotFound("LRU has no resident victim");
    auto candidate = order_.rbegin();
    while (candidate != order_.rend() && excluded && *candidate == *excluded) ++candidate;
    if (candidate == order_.rend()) return Status::NotFound("LRU has no unpinned resident victim");
    reserved_ = *candidate;
    return *reserved_;
}

Status LruEviction::ValidateVictimCommit(const BlockKey& key) const {
    if (!reserved_ || *reserved_ != key) {
        return Status::InvalidArgument("LRU commit does not match the reserved victim");
    }
    auto it = pos_.find(key);
    if (it == pos_.end()) {
        return Status::Internal("LRU reserved victim is not resident");
    }
    return Status::Ok();
}

void LruEviction::CommitVictim(const BlockKey& key) noexcept {
    if (!ValidateVictimCommit(key).ok()) std::terminate();
    auto it = pos_.find(key);
    order_.erase(it->second);
    pos_.erase(it);
    reserved_.reset();
}

Status LruEviction::CancelVictim(const BlockKey& key) {
    if (!reserved_ || *reserved_ != key) {
        return Status::InvalidArgument("LRU cancel does not match the reserved victim");
    }
    reserved_.reset();
    return Status::Ok();
}

}  // namespace tidepool
