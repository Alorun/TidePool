// ssd_tier.h — SSD storage tier backed by LevelDB (or RocksDB). Plane: DATA
// (node-internal). The cold, capacity level. Concrete impl of the Tier ABC.
//
// The byte payload is stored under BlockKey::ToString(); metadata is serialized
// alongside it. Built only when -DTIDEPOOL_WITH_LEVELDB=ON; otherwise every
// method is a NotImplemented stub so the scaffold builds with no dependency.
// (Continuing the prior project's LevelDB-based technology stack.)
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "tidepool/store/tier.h"

namespace tidepool {

class SsdTier : public Tier {
public:
    // `db_path` is the on-disk directory for the LevelDB instance.
    explicit SsdTier(std::string db_path);
    ~SsdTier() override;

    // Open/create the backing store. Returns NotImplemented when built without
    // LevelDB support. Open and Close are idempotent; a successful Close may be
    // followed by another Open.
    Status Open() override;
    Status Close() override;
    bool IsReady() const override;

    TierType type() const override { return TierType::kSsd; }
    Status Put(const BlockKey& key, const Block& block, uint64_t* out_handle) override;
    Status Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out) override;
    Status Evict(const BlockKey& key) override;
    TierStats Stats() const override;

private:
    std::string db_path_;
    // Guards the complete lifetime and use of db_, plus stats_. Holding this lock
    // for each LevelDB operation makes Close wait for in-flight calls before it
    // deletes the database object.
    mutable std::mutex mu_;
    TierStats stats_;
    // Opaque handle to the LevelDB instance (kept type-erased so the public
    // header pulls in no leveldb headers). nullptr when unopened/stubbed.
    void* db_ = nullptr;
};

}  // namespace tidepool
