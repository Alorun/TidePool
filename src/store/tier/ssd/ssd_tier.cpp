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

#include <exception>
#include <limits>
#include <new>
#include <utility>
#include <vector>

#include "tidepool/api/block_codec.h"
#include "tidepool/store/factory.h"

#ifdef TIDEPOOL_WITH_LEVELDB
#include <cstring>  // std::memcpy

#include <leveldb/db.h>
#include <leveldb/iterator.h>
#endif

namespace tidepool {
namespace {

#ifdef TIDEPOOL_WITH_LEVELDB
uint64_t StableHandle(const BlockKey& key) {
    uint64_t hash = 1469598103934665603ULL;
    auto mix = [&hash](uint64_t value, int bytes) {
        for (int i = 0; i < bytes; ++i) {
            hash ^= static_cast<uint8_t>((value >> (i * 8)) & 0xff);
            hash *= 1099511628211ULL;
        }
    };
    mix(key.prefix_hash, 8);
    mix(key.prefix_len, 4);
    mix(key.model_fingerprint, 4);
    return hash;
}

Status ValidateMetadata(const BlockKey& key, const BlockMetadata& metadata) {
    if (metadata.model_fingerprint != key.model_fingerprint) {
        return Status::Corruption("SSD record model_fingerprint does not match BlockKey");
    }
    if (metadata.num_tokens != 0 && metadata.num_tokens != key.prefix_len) {
        return Status::Corruption("SSD record num_tokens does not match BlockKey prefix_len");
    }
    return Status::Ok();
}

leveldb::ReadOptions CheckedReadOptions() {
    leveldb::ReadOptions options;
    options.verify_checksums = true;
    return options;
}

Status DecodeRecord(const BlockKey& key, std::string_view value, BlockInfo* info,
                    size_t* payload_offset = nullptr) {
    BlockMetadata metadata;
    size_t payload_size = 0;
    size_t offset = 0;
    if (Status s = DeserializeHeader(value, &metadata, &payload_size, &offset); !s.ok()) {
        return Status::Corruption("SSD record value is invalid: " + s.ToString());
    }
    if (Status s = ValidateMetadata(key, metadata); !s.ok()) return s;
    if (info) *info = BlockInfo{metadata, payload_size, StableHandle(key)};
    if (payload_offset) *payload_offset = offset;
    return Status::Ok();
}
#endif

}  // namespace

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
    if (!s.ok()) {
        delete db;
        return Status::IoError("leveldb open: " + s.ToString());
    }

    // LevelDB may already contain records from a previous process. Rebuild the
    // physical occupancy counters now so a later overwrite/eviction cannot
    // subtract an old record from zero and underflow used_bytes.
    uint64_t num_blocks = 0;
    uint64_t used_bytes = 0;
    Status scan_status = Status::Ok();
    try {
        std::unique_ptr<leveldb::Iterator> it(db->NewIterator(CheckedReadOptions()));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            const std::string_view encoded_key(it->key().data(), it->key().size());
            auto key = BlockKey::FromString(encoded_key);
            if (!key.ok()) {
                scan_status = Status::Corruption("leveldb scan key: " + key.status().ToString());
                break;
            }
            const std::string_view encoded_value(it->value().data(), it->value().size());
            if (Status valid = DecodeRecord(key.value(), encoded_value, nullptr); !valid.ok()) {
                scan_status = Status::Corruption("leveldb scan value: " + valid.ToString());
                break;
            }
            if (used_bytes > std::numeric_limits<uint64_t>::max() - it->value().size()) {
                scan_status = Status::Corruption("leveldb scan size overflow");
                break;
            }
            ++num_blocks;
            used_bytes += it->value().size();
        }
        if (scan_status.ok() && !it->status().ok()) {
            scan_status = Status::IoError("leveldb scan: " + it->status().ToString());
        }
    } catch (const std::bad_alloc&) {
        scan_status = Status(StatusCode::kOutOfCapacity, "leveldb scan allocation failed");
    }
    if (!scan_status.ok()) {
        delete db;
        return scan_status;
    }

    db_ = db;
    stats_.num_blocks = num_blocks;
    stats_.used_bytes = used_bytes;
    stats_.put_count = 0;
    stats_.get_count = 0;
    stats_.evict_count = 0;
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

Result<BlockInfo> SsdTier::Probe([[maybe_unused]] const BlockKey& key) {
#ifdef TIDEPOOL_WITH_LEVELDB
    std::lock_guard<std::mutex> lock(mu_);
    auto* db = reinterpret_cast<leveldb::DB*>(db_);
    if (db == nullptr) return Status::Unavailable("SsdTier::Probe: tier is not open");
    try {
        const std::string encoded_key = key.ToString();
        std::string value;
        const leveldb::Status s = db->Get(CheckedReadOptions(), encoded_key, &value);
        if (s.IsNotFound()) return Status::NotFound(encoded_key);
        if (!s.ok()) return Status::IoError("leveldb probe: " + s.ToString());
        BlockInfo info;
        if (Status decoded = DecodeRecord(key, value, &info); !decoded.ok()) return decoded;
        return info;
    } catch (const std::bad_alloc&) {
        return Status(StatusCode::kOutOfCapacity, "SsdTier::Probe allocation failed");
    }
#else
    return Status::NotImplemented("SsdTier::Probe requires -DTIDEPOOL_WITH_LEVELDB=ON");
#endif
}

Status SsdTier::Put([[maybe_unused]] const BlockKey& key, [[maybe_unused]] const Block& block,
                    [[maybe_unused]] uint64_t* out_handle) {
#ifdef TIDEPOOL_WITH_LEVELDB
    std::lock_guard<std::mutex> lock(mu_);
    auto* db = reinterpret_cast<leveldb::DB*>(db_);
    if (db == nullptr) return Status::Unavailable("SsdTier::Put: tier is not open");

    if (Status valid = ValidateMetadata(key, block.metadata); !valid.ok()) {
        return Status::InvalidArgument(valid.message());
    }

    std::string blob;
    std::string k;
    try {
        blob = SerializeBlock(block);
        k = key.ToString();
    } catch (const std::bad_alloc&) {
        return Status(StatusCode::kOutOfCapacity, "SsdTier::Put allocation failed");
    }

    // Probe for an existing record so used_bytes/num_blocks stay accurate across
    // overwrites (SSD keeps no per-key size index of its own). This costs one
    // read on the write path, which is acceptable: Put is not the hot zero-copy
    // path.
    std::string existing;
    leveldb::Status g;
    leveldb::Status s;
    try {
        g = db->Get(CheckedReadOptions(), k, &existing);
        if (!g.ok() && !g.IsNotFound()) {
            return Status::IoError("leveldb get(probe): " + g.ToString());
        }
        s = db->Put(leveldb::WriteOptions(), k, blob);
        if (!s.ok()) return Status::IoError("leveldb put: " + s.ToString());
    } catch (const std::bad_alloc&) {
        return Status(StatusCode::kOutOfCapacity, "SsdTier::Put backend allocation failed");
    }
    const bool overwrite = g.ok();

    if (overwrite) {
        stats_.used_bytes -= existing.size();  // drop the old blob's footprint
    } else {
        stats_.num_blocks++;
    }
    stats_.used_bytes += blob.size();
    stats_.put_count++;

    // Re-lookups use the fully parsed BlockKey. The deterministic handle is an
    // opaque diagnostic token only and is never used as the physical address.
    if (out_handle) *out_handle = StableHandle(key);
    return Status::Ok();
#else
    return Status::NotImplemented("SsdTier::Put requires -DTIDEPOOL_WITH_LEVELDB=ON");
#endif
}

Status SsdTier::Get([[maybe_unused]] const BlockKey& key, [[maybe_unused]] const MutableBuffer& dst,
                    [[maybe_unused]] BlockView* out) {
    if (out) *out = BlockView{};
    if (out == nullptr) return Status::InvalidArgument("SsdTier::Get: out is null");
    if (dst.data == nullptr && dst.capacity != 0) {
        return Status::InvalidArgument("SsdTier::Get: dst.data is null with non-zero capacity");
    }
#ifdef TIDEPOOL_WITH_LEVELDB
    std::lock_guard<std::mutex> lock(mu_);
    auto* db = reinterpret_cast<leveldb::DB*>(db_);
    if (db == nullptr) return Status::Unavailable("SsdTier::Get: tier is not open");

    // NOTE: SSD Get is NOT zero-copy. LevelDB copies the record into `value`
    // (copy #1), then we copy the payload out of `value` into the caller's dst
    // (copy #2). This double copy is inherent to the disk tier and is left as-is
    // — real zero-copy belongs only to the DRAM/RDMA path, never SSD.
    try {
        const std::string encoded_key = key.ToString();
        std::string value;
        const leveldb::Status s = db->Get(CheckedReadOptions(), encoded_key, &value);
        if (s.IsNotFound()) {
            stats_.misses++;
            return Status::NotFound(encoded_key);
        }
        if (!s.ok()) return Status::IoError("leveldb get: " + s.ToString());

        BlockInfo info;
        size_t payload_off = 0;
        if (Status decoded = DecodeRecord(key, value, &info, &payload_off);
            !decoded.ok()) {
            return decoded;
        }
        if (info.payload_size > dst.capacity) {
            return Status(StatusCode::kOutOfCapacity, "SsdTier::Get: dst buffer too small");
        }
        if (info.payload_size != 0) {
            std::memcpy(dst.data, value.data() + payload_off, info.payload_size);
        }
        stats_.get_count++;
        stats_.hits++;
        out->data = dst.data;
        out->size = info.payload_size;
        out->metadata = info.metadata;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status(StatusCode::kOutOfCapacity, "SsdTier::Get allocation failed");
    }
#else
    return Status::NotImplemented("SsdTier::Get requires -DTIDEPOOL_WITH_LEVELDB=ON");
#endif
}

Status SsdTier::Evict([[maybe_unused]] const BlockKey& key) {
#ifdef TIDEPOOL_WITH_LEVELDB
    std::lock_guard<std::mutex> lock(mu_);
    auto* db = reinterpret_cast<leveldb::DB*>(db_);
    if (db == nullptr) return Status::Unavailable("SsdTier::Evict: tier is not open");

    std::string k;
    std::string existing;
    try {
        k = key.ToString();
        const leveldb::Status g = db->Get(CheckedReadOptions(), k, &existing);
        if (g.IsNotFound()) return Status::NotFound(k);
        if (!g.ok()) return Status::IoError("leveldb get(evict): " + g.ToString());

        const leveldb::Status s = db->Delete(leveldb::WriteOptions(), k);
        if (!s.ok()) return Status::IoError("leveldb delete: " + s.ToString());
    } catch (const std::bad_alloc&) {
        return Status(StatusCode::kOutOfCapacity, "SsdTier::Evict allocation failed");
    }

    if (stats_.num_blocks > 0) stats_.num_blocks--;
    stats_.used_bytes -= existing.size();
    stats_.evict_count++;
    return Status::Ok();
#else
    return Status::NotImplemented("SsdTier::Evict requires -DTIDEPOOL_WITH_LEVELDB=ON");
#endif
}

Status SsdTier::ValidateEraseExisting(const BlockKey&) const {
    return Status::NotImplemented("SsdTier does not support volatile commit erasure");
}

void SsdTier::EraseExisting(const BlockKey&) noexcept { std::terminate(); }

Status SsdTier::VisitEntries(
    const std::function<Status(const BlockKey&, const BlockInfo&)>& visitor) const {
#ifdef TIDEPOOL_WITH_LEVELDB
    std::vector<std::pair<BlockKey, BlockInfo>> entries;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto* db = reinterpret_cast<leveldb::DB*>(db_);
        if (db == nullptr) return Status::Unavailable("SsdTier::VisitEntries: tier is not open");
        try {
            std::unique_ptr<leveldb::Iterator> it(db->NewIterator(CheckedReadOptions()));
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                auto key = BlockKey::FromString(it->key().ToString());
                if (!key.ok()) return key.status();
                BlockInfo info;
                if (Status valid = DecodeRecord(key.value(), it->value().ToString(), &info); !valid.ok()) {
                    return valid;
                }
                entries.emplace_back(key.value(), info);
            }
            if (!it->status().ok()) return Status::IoError("leveldb visit: " + it->status().ToString());
        } catch (const std::bad_alloc&) {
            return Status(StatusCode::kOutOfCapacity, "SsdTier::VisitEntries allocation failed");
        }
    }
    for (const auto& [key, info] : entries) {
        if (Status s = visitor(key, info); !s.ok()) return s;
    }
    return Status::Ok();
#else
    (void)visitor;
    return Status::NotImplemented("SsdTier::VisitEntries requires -DTIDEPOOL_WITH_LEVELDB=ON");
#endif
}

TierStats SsdTier::Stats() const {
    std::lock_guard<std::mutex> lock(mu_);
    return stats_;
}

}  // namespace tidepool
