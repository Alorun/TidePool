// local_index.h — Node-local block directory. Plane: DATA (node-internal).
//
// Maps BlockKey -> Location for blocks resident on THIS node, recording which
// tier currently holds each block and its tier-local handle. It is a plain
// in-memory hash table: PURELY LOCAL, NO CONSENSUS, no cross-node coordination.
// Global placement is decided by the hash ring; this index only answers
// "which of my tiers has this block, if any?".
//
// Concrete (not abstract): the in-memory hash table is the design, not a
// pluggable seam. Thread-safety is the caller/StorageNode's concern for now
// (TODO: internal sharded locking).
#pragma once

#include <cstddef>
#include <unordered_map>

#include "tidepool/api/block_key.h"
#include "tidepool/api/location.h"
#include "tidepool/api/status.h"

namespace tidepool {

class LocalIndex {
public:
    // Insert or overwrite the location for `key`.
    Status Upsert(const BlockKey& key, const Location& loc);

    // Look up the current location of `key` on this node.
    Result<Location> Find(const BlockKey& key) const;

    // Remove `key` from the directory. Returns kNotFound if absent.
    Status Remove(const BlockKey& key);

    bool Contains(const BlockKey& key) const;
    size_t size() const { return table_.size(); }

private:
    // TODO: shard this map + per-shard mutex for concurrent get/put.
    std::unordered_map<BlockKey, Location> table_;
};

}  // namespace tidepool
