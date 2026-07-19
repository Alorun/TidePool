#include "dram_tier.h"

#include <exception>
#include <cstring>  // std::memcpy
#include <new>      // std::bad_alloc
#include <utility>
#include <vector>

#include "tidepool/store/factory.h"

namespace tidepool {

std::unique_ptr<Tier> MakeDramTier(uint64_t capacity_bytes) { return std::make_unique<DramTier>(capacity_bytes); }

Result<BlockInfo> DramTier::Probe(const BlockKey& key) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = store_.find(key);
    if (it == store_.end()) return Status::NotFound(key.ToString());
    return BlockInfo{it->second.block.metadata, it->second.block.size_bytes(), it->second.handle};
}

// A minimal but functional DRAM tier so the single-node Put/Get path (ROADMAP
// stage 1) is exercisable end-to-end. Capacity enforcement / eviction wiring is
// orchestrated by the StorageNode + EvictionPolicy and is left as TODO here.
Status DramTier::Put(const BlockKey& key, const Block& block, uint64_t* out_handle) {
    Block replacement;
    try {
        replacement = block;
    } catch (const std::bad_alloc&) {
        return Status(StatusCode::kOutOfCapacity, "DramTier::Put: block allocation failed");
    }

    std::lock_guard<std::mutex> lock(mu_);
    const size_t new_size = replacement.size_bytes();
    auto existing = store_.find(key);
    const uint64_t handle = next_handle_;
    if (existing != store_.end()) {
        const size_t old_size = existing->second.block.size_bytes();
        existing->second.block = std::move(replacement);
        existing->second.handle = handle;
        if (new_size >= old_size) {
            stats_.used_bytes += new_size - old_size;
        } else {
            stats_.used_bytes -= old_size - new_size;
        }
    } else {
        try {
            store_.emplace(key, Entry{std::move(replacement), handle});
        } catch (const std::bad_alloc&) {
            return Status(StatusCode::kOutOfCapacity, "DramTier::Put: map allocation failed");
        }
        stats_.num_blocks++;
        stats_.used_bytes += new_size;
    }

    next_handle_++;
    stats_.put_count++;
    if (out_handle) *out_handle = handle;
    return Status::Ok();
}

Status DramTier::Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out) {
    std::lock_guard<std::mutex> lock(mu_);
    if (out) *out = BlockView{};
    if (out == nullptr) return Status::InvalidArgument("DramTier::Get: out is null");
    if (dst.data == nullptr && dst.capacity != 0) {
        return Status::InvalidArgument("DramTier::Get: dst.data is null with non-zero capacity");
    }
    auto it = store_.find(key);
    if (it == store_.end()) {
        stats_.misses++;
        return Status::NotFound(key.ToString());
    }
    const Block& blk = it->second.block;
    if (dst.capacity < blk.size_bytes()) {
        return Status(StatusCode::kOutOfCapacity, "DramTier::Get: dst buffer too small");
    }
    // Copy into the caller-owned buffer. We deliberately do NOT hand out a
    // pointer into store_ (e.g. via Location.handle or a BlockView over store_
    // memory): a rehash or eviction would dangle it. handle stays a stable
    // opaque id and reads always copy out under the lock. See location.h handle
    // contract.
    // TODO(zero-copy): add a local view path that returns a BlockView over
    // store_ memory under a read-lock/refcount instead of copying, for
    // owner==self hits.
    if (!blk.data.empty()) std::memcpy(dst.data, blk.data.data(), blk.size_bytes());
    stats_.get_count++;
    stats_.hits++;
    out->data = dst.data;
    out->size = blk.size_bytes();
    out->metadata = blk.metadata;
    return Status::Ok();
}

Status DramTier::Evict(const BlockKey& key) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = store_.find(key);
    if (it == store_.end()) return Status::NotFound(key.ToString());
    stats_.used_bytes -= it->second.block.size_bytes();
    stats_.num_blocks--;
    store_.erase(it);
    stats_.evict_count++;
    return Status::Ok();
}

Status DramTier::ValidateEraseExisting(const BlockKey& key) const {
    std::lock_guard<std::mutex> lock(mu_);
    if (store_.find(key) == store_.end()) {
        return Status::Internal("DramTier::ValidateEraseExisting: key is absent");
    }
    return Status::Ok();
}

void DramTier::EraseExisting(const BlockKey& key) noexcept {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = store_.find(key);
    if (it == store_.end()) std::terminate();
    stats_.used_bytes -= it->second.block.size_bytes();
    stats_.num_blocks--;
    store_.erase(it);
    stats_.evict_count++;
}

Status DramTier::VisitEntries(
    const std::function<Status(const BlockKey&, const BlockInfo&)>& visitor) const {
    std::vector<std::pair<BlockKey, BlockInfo>> entries;
    try {
        std::lock_guard<std::mutex> lock(mu_);
        entries.reserve(store_.size());
        for (const auto& [key, entry] : store_) {
            entries.emplace_back(
                key, BlockInfo{entry.block.metadata, entry.block.size_bytes(), entry.handle});
        }
    } catch (const std::bad_alloc&) {
        return Status(StatusCode::kOutOfCapacity, "DramTier::VisitEntries allocation failed");
    }
    for (const auto& [key, info] : entries) {
        if (Status s = visitor(key, info); !s.ok()) return s;
    }
    return Status::Ok();
}

TierStats DramTier::Stats() const {
    std::lock_guard<std::mutex> lock(mu_);
    return stats_;
}

}  // namespace tidepool
