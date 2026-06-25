#include "tidepool/store/local_index.h"

namespace tidepool {

Status LocalIndex::Upsert(const BlockKey& key, const Location& loc) {
    table_[key] = loc;
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
