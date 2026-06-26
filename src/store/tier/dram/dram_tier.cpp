#include "dram_tier.h"

#include <cstring>  // std::memcpy

#include "tidepool/store/factory.h"

namespace tidepool {

std::unique_ptr<Tier> MakeDramTier(uint64_t capacity_bytes) { return std::make_unique<DramTier>(capacity_bytes); }

// A minimal but functional DRAM tier so the single-node Put/Get path (ROADMAP
// stage 1) is exercisable end-to-end. Capacity enforcement / eviction wiring is
// orchestrated by the StorageNode + EvictionPolicy and is left as TODO here.
Status DramTier::Put(const BlockKey& key, const Block& block, uint64_t* out_handle) {
    std::lock_guard<std::mutex> lock(mu_);
    const uint64_t handle = next_handle_++;
    auto [it, inserted] = store_.insert_or_assign(key, block);
    if (inserted) {
        stats_.num_blocks++;
        stats_.used_bytes += it->second.size_bytes();
    }
    // TODO: reject / trigger demotion when used_bytes would exceed capacity.
    if (out_handle) *out_handle = handle;
    return Status::Ok();
}

Status DramTier::Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = store_.find(key);
    if (it == store_.end()) {
        stats_.misses++;
        return Status::NotFound(key.ToString());
    }
    const Block& blk = it->second;
    if (dst.data == nullptr || dst.capacity < blk.size_bytes()) {
        return Status::InvalidArgument("dst buffer too small for block");
    }
    // Copy into the caller-owned buffer. We deliberately do NOT hand out a
    // pointer into store_ (e.g. via Location.handle or a BlockView over store_
    // memory): a rehash or eviction would dangle it. handle stays a stable
    // opaque id and reads always copy out under the lock. See location.h handle
    // contract.
    // TODO(zero-copy): add a local view path that returns a BlockView over
    // store_ memory under a read-lock/refcount instead of copying, for
    // owner==self hits.
    std::memcpy(dst.data, blk.data.data(), blk.size_bytes());
    stats_.hits++;
    if (out) {
        out->data = dst.data;
        out->size = blk.size_bytes();
        out->metadata = blk.metadata;
    }
    return Status::Ok();
}

Status DramTier::Evict(const BlockKey& key) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = store_.find(key);
    if (it == store_.end()) return Status::NotFound(key.ToString());
    stats_.used_bytes -= it->second.size_bytes();
    stats_.num_blocks--;
    store_.erase(it);
    return Status::Ok();
}

TierStats DramTier::Stats() const {
    std::lock_guard<std::mutex> lock(mu_);
    return stats_;
}

}  // namespace tidepool
