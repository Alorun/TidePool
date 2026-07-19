#include "tidepool/store/local_index.h"

#include <exception>
#include <new>  // std::bad_alloc
#include <utility>

namespace tidepool {

Status LocalIndex::Upsert(const BlockKey& key, const Location& loc) {
    Location replacement;
    try {
        replacement = loc;
        auto it = table_.find(key);
        if (it == table_.end()) {
            table_.emplace(key, std::move(replacement));
        } else {
            it->second = std::move(replacement);
        }
    } catch (const std::bad_alloc&) {
        return Status(StatusCode::kOutOfCapacity, "LocalIndex::Upsert allocation failed");
    }
    return Status::Ok();
}

Status LocalIndex::InsertIfAbsent(const BlockKey& key, const Location& loc) {
    if (table_.find(key) != table_.end()) return Status::Ok();
    try {
        table_.emplace(key, loc);
    } catch (const std::bad_alloc&) {
        return Status(StatusCode::kOutOfCapacity, "LocalIndex::InsertIfAbsent allocation failed");
    }
    return Status::Ok();
}

Status LocalIndex::ValidateRelocate(const BlockKey& key, TierType expected_tier) const {
    auto it = table_.find(key);
    if (it == table_.end()) return Status::NotFound(key.ToString());
    if (it->second.tier != expected_tier) {
        return Status::Internal("LocalIndex::ValidateRelocate: current tier does not match expected tier");
    }
    return Status::Ok();
}

void LocalIndex::RelocateExisting(const BlockKey& key, TierType expected_tier, TierType new_tier,
                                  uint64_t new_handle) noexcept {
    auto it = table_.find(key);
    if (it == table_.end() || it->second.tier != expected_tier) std::terminate();
    it->second.tier = new_tier;
    it->second.handle = new_handle;
}

Result<Location> LocalIndex::Find(const BlockKey& key) const {
    auto it = table_.find(key);
    if (it == table_.end()) return Status::NotFound(key.ToString());
    return it->second;
}

Status LocalIndex::Remove(const BlockKey& key) {
    auto it = table_.find(key);
    if (it == table_.end()) return Status::NotFound(key.ToString());
    table_.erase(it);
    return Status::Ok();
}

void LocalIndex::RemoveExisting(const BlockKey& key) noexcept {
    auto it = table_.find(key);
    if (it == table_.end()) std::terminate();
    table_.erase(it);
}

bool LocalIndex::Contains(const BlockKey& key) const { return table_.find(key) != table_.end(); }

}  // namespace tidepool
