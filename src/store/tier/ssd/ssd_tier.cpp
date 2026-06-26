#include "ssd_tier.h"

#include "tidepool/store/factory.h"

#ifdef TIDEPOOL_WITH_LEVELDB
#include <leveldb/db.h>
#endif

namespace tidepool {

std::unique_ptr<Tier> MakeSsdTier(std::string db_path) {
    // TODO: open lazily / propagate Open() errors to the caller. For the
    // scaffold we just construct; methods report NotImplemented without LevelDB
    // support.
    return std::make_unique<SsdTier>(std::move(db_path));
}

SsdTier::SsdTier(std::string db_path) : db_path_(std::move(db_path)) {}

SsdTier::~SsdTier() {
#ifdef TIDEPOOL_WITH_LEVELDB
    delete reinterpret_cast<leveldb::DB*>(db_);
    db_ = nullptr;
#endif
}

Status SsdTier::Open() {
#ifdef TIDEPOOL_WITH_LEVELDB
    // TODO: wire options (block cache, compression) and reopen semantics.
    leveldb::DB* db = nullptr;
    leveldb::Options options;
    options.create_if_missing = true;
    const leveldb::Status s = leveldb::DB::Open(options, db_path_, &db);
    if (!s.ok()) return Status::IoError("leveldb open: " + s.ToString());
    db_ = db;
    return Status::Ok();
#else
    return Status::NotImplemented("SsdTier requires -DTIDEPOOL_WITH_LEVELDB=ON");
#endif
}

Status SsdTier::Put(const BlockKey& /*key*/, const Block& /*block*/, uint64_t* /*out_handle*/) {
    // TODO: serialize {metadata, data} and Put under key.ToString();
    // update stats_; set *out_handle to a stable record id.
    return Status::NotImplemented("SsdTier::Put");
}

Status SsdTier::Get(const BlockKey& /*key*/, const MutableBuffer& /*dst*/, BlockView* /*out*/) {
    // TODO: Get key.ToString(), deserialize the metadata, copy the payload into
    // dst (bounds-check against dst.capacity), and fill *out. When LevelDB
    // hands back a Slice we may expose a BlockView over it to avoid the extra
    // copy.
    return Status::NotImplemented("SsdTier::Get");
}

Status SsdTier::Evict(const BlockKey& /*key*/) {
    // TODO: Delete key.ToString(); update stats_.
    return Status::NotImplemented("SsdTier::Evict");
}

TierStats SsdTier::Stats() const { return stats_; }

}  // namespace tidepool
