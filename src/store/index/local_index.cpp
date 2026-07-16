#include "tidepool/store/local_index.h"

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

Status LocalIndex::Relocate(const BlockKey& key, TierType expected_tier, TierType new_tier, uint64_t new_handle) {
    auto it = table_.find(key);
    if (it == table_.end()) return Status::NotFound(key.ToString());
    if (it->second.tier != expected_tier) {
        return Status::Internal("LocalIndex::Relocate: current tier does not match expected tier");
    }
    it->second.tier = new_tier;
    it->second.handle = new_handle;
    return Status::Ok();
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

bool LocalIndex::Contains(const BlockKey& key) const { return table_.find(key) != table_.end(); }

}  // namespace tidepool
