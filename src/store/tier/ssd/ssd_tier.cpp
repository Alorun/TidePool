// ssd_tier.cpp — SSD tier: Block <-> self-describing blob (via block_codec) on
// top of LevelDB. Plane: DATA (node-internal).
//
// The stored LevelDB value is exactly the block_codec blob (header + payload):
// self-describing, so a node that restarts and lost its volatile LocalIndex can
// still read shape metadata straight off disk. See api/block_codec.h.
//
// SCOPE / stage-3 TODO: no Mooncake-style persistence policy (Eager/Lazy/None),
// no striping / parallel I/O, no per-key size index. Those are production-grade
// directions, out of scope here.
#include "ssd_tier.h"

#include <functional>  // std::hash

#include "tidepool/api/block_codec.h"
#include "tidepool/store/factory.h"

#ifdef TIDEPOOL_WITH_LEVELDB
#include <cstring>  // std::memcpy

#include <leveldb/db.h>
#include <leveldb/iterator.h>
#endif

namespace tidepool {

std::unique_ptr<Tier> MakeSsdTier(std::string db_path) {
    return std::make_unique<SsdTier>(std::move(db_path));
}

SsdTier::SsdTier(std::string db_path) : db_path_(std::move(db_path)) {}

SsdTier::~SsdTier() { (void)Close(); }

Status SsdTier::Open() {
#ifdef TIDEPOOL_WITH_LEVELDB
    std::lock_guard<std::mutex> lock(mu_);
    if (db_ != nullptr) return Status::Ok();

    // TODO: wire options (block cache, compression).
    leveldb::DB* db = nullptr;
    leveldb::Options options;
    options.create_if_missing = true;
    const leveldb::Status s = leveldb::DB::Open(options, db_path_, &db);
    if (!s.ok()) return Status::IoError("leveldb open: " + s.ToString());

    // LevelDB may already contain records from a previous process. Rebuild the
    // physical occupancy counters now so a later overwrite/eviction cannot
    // subtract an old record from zero and underflow used_bytes.
    uint64_t num_blocks = 0;
    uint64_t used_bytes = 0;
    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ++num_blocks;
        used_bytes += it->value().size();
    }
    if (!it->status().ok()) {
        const std::string message = it->status().ToString();
        delete db;
        return Status::IoError("leveldb scan: " + message);
    }

    db_ = db;
    stats_.num_blocks = num_blocks;
    stats_.used_bytes = used_bytes;
    stats_.hits = 0;
    stats_.misses = 0;
    return Status::Ok();
#else
    return Status::NotImplemented("SsdTier requires -DTIDEPOOL_WITH_LEVELDB=ON");
#endif
}

Status SsdTier::Close() {
#ifdef TIDEPOOL_WITH_LEVELDB
    std::lock_guard<std::mutex> lock(mu_);
    auto* db = reinterpret_cast<leveldb::DB*>(db_);
    if (db == nullptr) return Status::Ok();
    delete db;
    db_ = nullptr;
#endif
    return Status::Ok();
}

bool SsdTier::IsReady() const {
    std::lock_guard<std::mutex> lock(mu_);
    return db_ != nullptr;
}

Status SsdTier::Put([[maybe_unused]] const BlockKey& key, [[maybe_unused]] const Block& block,
                    [[maybe_unused]] uint64_t* out_handle) {
#ifdef TIDEPOOL_WITH_LEVELDB
    std::lock_guard<std::mutex> lock(mu_);
    auto* db = reinterpret_cast<leveldb::DB*>(db_);
    if (db == nullptr) return Status::Unavailable("SsdTier::Put: tier is not open");

    // The value we persist is the whole self-describing blob.
    const std::string blob = SerializeBlock(block);
    const std::string k = key.ToString();

    // Probe for an existing record so used_bytes/num_blocks stay accurate across
    // overwrites (SSD keeps no per-key size index of its own). This costs one
    // read on the write path, which is acceptable: Put is not the hot zero-copy
    // path.
    std::string existing;
    const leveldb::Status g = db->Get(leveldb::ReadOptions(), k, &existing);
    const bool overwrite = g.ok();
    if (!g.ok() && !g.IsNotFound()) return Status::IoError("leveldb get(probe): " + g.ToString());

    const leveldb::Status s = db->Put(leveldb::WriteOptions(), k, blob);
    if (!s.ok()) return Status::IoError("leveldb put: " + s.ToString());

    if (overwrite) {
        stats_.used_bytes -= existing.size();  // drop the old blob's footprint
    } else {
        stats_.num_blocks++;
    }
    stats_.used_bytes += blob.size();

    // SSD has no native handle: the LevelDB key IS the record id, so we return a
    // stable hash of the key rather than a pointer/offset. Re-lookups go by key;
    // the handle carries no semantics (matches Location's stable-id contract).
    if (out_handle) *out_handle = static_cast<uint64_t>(std::hash<BlockKey>{}(key));
    return Status::Ok();
#else
    return Status::NotImplemented("SsdTier::Put requires -DTIDEPOOL_WITH_LEVELDB=ON");
#endif
}

Status SsdTier::Get([[maybe_unused]] const BlockKey& key, [[maybe_unused]] const MutableBuffer& dst,
                    [[maybe_unused]] BlockView* out) {
#ifdef TIDEPOOL_WITH_LEVELDB
    std::lock_guard<std::mutex> lock(mu_);
    auto* db = reinterpret_cast<leveldb::DB*>(db_);
    if (db == nullptr) return Status::Unavailable("SsdTier::Get: tier is not open");

    // NOTE: SSD Get is NOT zero-copy. LevelDB copies the record into `value`
    // (copy #1), then we copy the payload out of `value` into the caller's dst
    // (copy #2). This double copy is inherent to the disk tier and is left as-is
    // — real zero-copy belongs only to the DRAM/RDMA path, never SSD.
    std::string value;
    const leveldb::Status s = db->Get(leveldb::ReadOptions(), key.ToString(), &value);
    if (s.IsNotFound()) {
        stats_.misses++;
        return Status::NotFound(key.ToString());
    }
    if (!s.ok()) return Status::IoError("leveldb get: " + s.ToString());

    // Parse the header only — this locates the payload without copying it, which
    // is exactly what the size-probe protocol below needs.
    BlockMetadata meta;
    size_t payload_len = 0;
    size_t payload_off = 0;
    const Status ds = DeserializeHeader(value, &meta, &payload_len, &payload_off);
    if (!ds.ok()) return ds;  // corrupt/unknown blob -> propagate the reason

    // Size-probe: if the caller's buffer is too small, report the required size
    // and DO NOT copy, so the caller can resize and retry.
    if (payload_len > dst.capacity) {
        if (out) {
            out->data = nullptr;
            out->size = payload_len;  // tell the caller how big dst must be
            out->metadata = meta;
        }
        // kOutOfCapacity has no Status:: factory; construct it directly.
        return Status(StatusCode::kOutOfCapacity, "SsdTier::Get: dst too small, resize to out->size");
    }
    if (dst.data == nullptr) return Status::InvalidArgument("dst buffer is null");

    std::memcpy(dst.data, value.data() + payload_off, payload_len);
    stats_.hits++;
    if (out) {
        out->data = dst.data;
        out->size = payload_len;
        out->metadata = meta;
    }
    return Status::Ok();
#else
    return Status::NotImplemented("SsdTier::Get requires -DTIDEPOOL_WITH_LEVELDB=ON");
#endif
}

Status SsdTier::Evict([[maybe_unused]] const BlockKey& key) {
#ifdef TIDEPOOL_WITH_LEVELDB
    std::lock_guard<std::mutex> lock(mu_);
    auto* db = reinterpret_cast<leveldb::DB*>(db_);
    if (db == nullptr) return Status::Unavailable("SsdTier::Evict: tier is not open");

    const std::string k = key.ToString();
    // Read the record first so we can adjust used_bytes by its true footprint;
    // absent -> NotFound (nothing to evict here).
    std::string existing;
    const leveldb::Status g = db->Get(leveldb::ReadOptions(), k, &existing);
    if (g.IsNotFound()) return Status::NotFound(k);
    if (!g.ok()) return Status::IoError("leveldb get(evict): " + g.ToString());

    const leveldb::Status s = db->Delete(leveldb::WriteOptions(), k);
    if (!s.ok()) return Status::IoError("leveldb delete: " + s.ToString());

    if (stats_.num_blocks > 0) stats_.num_blocks--;
    stats_.used_bytes -= existing.size();
    return Status::Ok();
#else
    return Status::NotImplemented("SsdTier::Evict requires -DTIDEPOOL_WITH_LEVELDB=ON");
#endif
}

TierStats SsdTier::Stats() const {
    std::lock_guard<std::mutex> lock(mu_);
    return stats_;
}

}  // namespace tidepool
