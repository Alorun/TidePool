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
    // Insert only when absent. Used while rebuilding the index in hot-to-cold
    // tier order so an inclusive SSD copy never overrides a DRAM primary.
    Status InsertIfAbsent(const BlockKey& key, const Location& loc);

    // Validate before a persistence commit, then relocate without allocation.
    Status ValidateRelocate(const BlockKey& key, TierType expected_tier) const;
    void RelocateExisting(const BlockKey& key, TierType expected_tier, TierType new_tier,
                          uint64_t new_handle) noexcept;

    // Look up the current location of `key` on this node.
    Result<Location> Find(const BlockKey& key) const;

    // Remove `key` from the directory. Returns kNotFound if absent.
    Status Remove(const BlockKey& key);
    void RemoveExisting(const BlockKey& key) noexcept;

    bool Contains(const BlockKey& key) const;
    size_t size() const { return table_.size(); }
    void Swap(LocalIndex& other) noexcept { table_.swap(other.table_); }

private:
    // TODO: shard this map + per-shard mutex for concurrent get/put.
    std::unordered_map<BlockKey, Location> table_;
};

}  // namespace tidepool
