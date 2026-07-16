#include "tidepool/store/storage_node.h"

#include <algorithm>
#include <limits>
#include <new>
#include <utility>

namespace tidepool {

StorageNode::StorageNode(NodeId id, std::vector<std::unique_ptr<Tier>> tiers, std::unique_ptr<EvictionPolicy> eviction)
    : id_(std::move(id)), tiers_(std::move(tiers)), eviction_(std::move(eviction)) {}

StorageNode::~StorageNode() { (void)Close(); }

Status StorageNode::Open() {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    if (ready_) return Status::Ok();
    if (tiers_.empty()) return Status::Internal("StorageNode::Open: node has no tiers");

    size_t opened = 0;
    Status failure = Status::Ok();
    size_t failed_index = 0;
    for (size_t i = 0; i < tiers_.size(); ++i) {
        Tier* tier = tiers_[i].get();
        if (tier == nullptr) {
            failure = Status::Internal("configured tier is null");
            failed_index = i;
            break;
        }

        failure = tier->Open();
        if (!failure.ok()) {
            failed_index = i;
            break;
        }
        opened = i + 1;
        if (!tier->IsReady()) {
            failure = Status::Internal("Open returned OK but tier is not ready");
            failed_index = i;
            break;
        }
    }

    if (!failure.ok()) {
        std::string message = "StorageNode::Open: tier[" + std::to_string(failed_index) + "]";
        if (tiers_[failed_index]) message += " (" + std::string(TierTypeName(tiers_[failed_index]->type())) + ")";
        message += " failed: " + failure.ToString();

        while (opened > 0) {
            --opened;
            Tier* tier = tiers_[opened].get();
            if (tier == nullptr) continue;
            if (Status rollback = tier->Close(); !rollback.ok()) {
                message += "; rollback close tier[" + std::to_string(opened) + "] (" +
                           TierTypeName(tier->type()) + ") failed: " + rollback.ToString();
            }
        }
        ready_ = false;
        return Status(failure.code(), std::move(message));
    }

    ready_ = true;
    return Status::Ok();
}

Status StorageNode::Close() {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    Status first_error = Status::Ok();
    for (size_t i = tiers_.size(); i > 0; --i) {
        Tier* tier = tiers_[i - 1].get();
        if (tier == nullptr) continue;
        if (Status s = tier->Close(); !s.ok() && first_error.ok()) {
            first_error = Status(s.code(), "StorageNode::Close: tier[" + std::to_string(i - 1) + "] (" +
                                                   TierTypeName(tier->type()) + ") failed: " + s.ToString());
        }
    }
    ready_ = false;
    return first_error;
}

bool StorageNode::IsReady() const {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    return ready_;
}

Tier* StorageNode::TierOfLocked(TierType type) {
    for (size_t i = 0; i < tiers_.size(); i++) {
        Tier* t = tiers_[i].get();
        if (t && t->type() == type) return t;
    }
    return nullptr;
}

Tier* StorageNode::ColderTierLocked() {
    // Tiers are hottest-first; the demotion sink for DRAM is the next tier down.
    // In the MVP that is the SSD tier when present.
    return TierOfLocked(TierType::kSsd);
}

Status StorageNode::ReadBlockLocked(Tier* tier, const BlockKey& key, Block* out) {
    if (tier == nullptr || out == nullptr) return Status::InvalidArgument("ReadBlockLocked received a null argument");
    const TierStats stats = tier->Stats();
    if (stats.used_bytes > std::numeric_limits<size_t>::max()) {
        return Status(StatusCode::kOutOfCapacity, "tier byte count exceeds addressable memory");
    }

    try {
        out->data.resize(std::max<size_t>(1, static_cast<size_t>(stats.used_bytes)));
    } catch (const std::bad_alloc&) {
        return Status(StatusCode::kOutOfCapacity, "failed to allocate victim read buffer");
    }
    MutableBuffer dst{out->data.data(), out->data.size()};
    BlockView view;
    if (Status s = tier->Get(key, dst, &view); !s.ok()) return s;
    out->data.resize(view.size);
    out->metadata = view.metadata;
    return Status::Ok();
}

Status StorageNode::DemoteOneVictimLocked(Tier* dram, Tier* sink) {
    Result<BlockKey> selected = eviction_->SelectVictim();
    if (!selected.ok()) return selected.status();
    const BlockKey victim = selected.value();

    auto cancel = [&]() { return eviction_->CancelVictim(victim); };
    auto location = index_.Find(victim);
    if (!location.ok()) {
        Status cancelled = cancel();
        if (!cancelled.ok()) return cancelled;
        return Status::Internal("selected victim is missing from LocalIndex");
    }
    if (location.value().tier != TierType::kDram) {
        Status cancelled = cancel();
        if (!cancelled.ok()) return cancelled;
        return Status::Internal("selected victim is not located in DRAM");
    }

    Block block;
    if (Status s = ReadBlockLocked(dram, victim, &block); !s.ok()) {
        Status cancelled = cancel();
        if (!cancelled.ok()) return cancelled;
        return s;
    }

    uint64_t sink_handle = 0;
    if (Status s = sink->Put(victim, block, &sink_handle); !s.ok()) {
        Status cancelled = cancel();
        if (!cancelled.ok()) return cancelled;
        return s;
    }

    // If DRAM deletion fails, the SSD copy is an acceptable orphan/inclusive
    // duplicate. LocalIndex remains on DRAM and the policy reservation is
    // cancelled, so the readable primary copy is unchanged.
    if (Status s = dram->Evict(victim); !s.ok()) {
        Status cancelled = cancel();
        if (!cancelled.ok()) return cancelled;
        return s;
    }

    if (Status s = index_.Relocate(victim, TierType::kDram, sink->type(), sink_handle); !s.ok()) {
        uint64_t restored_handle = 0;
        Status restored = dram->Put(victim, block, &restored_handle);
        if (restored.ok()) {
            restored = index_.Relocate(victim, TierType::kDram, TierType::kDram, restored_handle);
        }
        Status cancelled = cancel();
        if (!restored.ok()) return restored;
        if (!cancelled.ok()) return cancelled;
        return s;
    }

    if (Status s = eviction_->CommitVictim(victim); !s.ok()) {
        uint64_t restored_handle = 0;
        Status restored = dram->Put(victim, block, &restored_handle);
        if (restored.ok()) {
            restored = index_.Relocate(victim, sink->type(), TierType::kDram, restored_handle);
        }
        Status cancelled = cancel();
        if (!restored.ok()) return restored;
        if (!cancelled.ok()) return cancelled;
        return s;
    }
    return Status::Ok();
}

Status StorageNode::MakeDramRoomLocked() {
    Tier* dram = TierOfLocked(TierType::kDram);
    if (dram == nullptr) return Status::Ok();

    while (true) {
        const TierStats before = dram->Stats();
        if (before.capacity_bytes == 0 || before.used_bytes <= before.capacity_bytes) return Status::Ok();
        if (eviction_ == nullptr) {
            return Status(StatusCode::kOutOfCapacity, "DRAM is over capacity and has no eviction policy");
        }
        Tier* sink = ColderTierLocked();
        if (sink == nullptr) {
            return Status(StatusCode::kOutOfCapacity, "DRAM is over capacity and has no SSD sink");
        }

        Status demoted = DemoteOneVictimLocked(dram, sink);
        if (!demoted.ok()) {
            if (demoted.code() == StatusCode::kNotFound) {
                return Status(StatusCode::kOutOfCapacity, "DRAM is over capacity but no victim is available");
            }
            return demoted;
        }

        const TierStats after = dram->Stats();
        if (after.used_bytes >= before.used_bytes && after.num_blocks >= before.num_blocks) {
            return Status::Internal("DRAM demotion committed without making progress");
        }
    }
}

Status StorageNode::RollbackPutLocked(const BlockKey& key, Tier* hot, const std::optional<Location>& old_location,
                                      const std::optional<Block>& old_dram_block, bool policy_inserted) {
    if (old_dram_block) {
        uint64_t restored_handle = 0;
        if (Status s = hot->Put(key, *old_dram_block, &restored_handle); !s.ok()) return s;
        return index_.Relocate(key, hot->type(), hot->type(), restored_handle);
    }

    if (Status s = hot->Evict(key); !s.ok() && s.code() != StatusCode::kNotFound) return s;
    if (policy_inserted && eviction_) eviction_->OnRemove(key);

    if (old_location) return index_.Upsert(key, *old_location);
    Status removed = index_.Remove(key);
    if (!removed.ok() && removed.code() != StatusCode::kNotFound) return removed;
    return Status::Ok();
}

void StorageNode::PromoteToDramLocked(const BlockKey& key, const BlockView& view) {
    Tier* dram = TierOfLocked(TierType::kDram);
    if (dram == nullptr) return;  // no DRAM tier: nothing to promote into.

    // Reconstruct an owning block from the bytes already in the caller buffer.
    Block blk;
    blk.metadata = view.metadata;
    blk.data.assign(view.data, view.data + view.size);

    if (Status s = MakeDramRoomLocked(); !s.ok()) return;

    uint64_t handle = 0;
    if (Status s = dram->Put(key, blk, &handle); !s.ok()) return;  // best-effort.
    index_.Upsert(key, Location{id_, dram->type(), handle});
    // Cold SSD-hit -> ARC Case IV (T1). If OnAccess already promoted the key out
    // of a ghost list, OnInsert is a no-op (it stays in T2). Either way correct.
    if (eviction_) eviction_->OnInsert(key, blk.size_bytes());
}

Status StorageNode::Put(const BlockKey& key, const Block& block) {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    if (!ready_) return Status::Unavailable("StorageNode::Put: node is not open");
    if (tiers_.empty()) return Status::Internal("storage node has no tiers");
    // Place into the hottest tier (front). Cross-tier demotion on capacity
    // pressure is orchestrated here.
    Tier* hot = tiers_.front().get();
    std::optional<Location> old_location;
    auto existing = index_.Find(key);
    if (existing.ok()) {
        old_location = existing.value();
    } else if (existing.status().code() != StatusCode::kNotFound) {
        return existing.status();
    }

    std::optional<Block> old_dram_block;
    const bool was_in_hot_dram = old_location && old_location->tier == TierType::kDram &&
                                 hot->type() == TierType::kDram;
    if (was_in_hot_dram) {
        Block old;
        if (Status s = ReadBlockLocked(hot, key, &old); !s.ok()) return s;
        old_dram_block = std::move(old);
    }

    uint64_t handle = 0;
    if (Status s = hot->Put(key, block, &handle); !s.ok()) return s;
    if (Status s = index_.Upsert(key, Location{id_, hot->type(), handle}); !s.ok()) {
        Status rollback = RollbackPutLocked(key, hot, old_location, old_dram_block, false);
        return rollback.ok() ? s : rollback;
    }

    const bool policy_inserted = !was_in_hot_dram;
    if (policy_inserted && eviction_) eviction_->OnInsert(key, block.size_bytes());

    if (Status s = MakeDramRoomLocked(); !s.ok()) {
        Status rollback = RollbackPutLocked(key, hot, old_location, old_dram_block, policy_inserted);
        return rollback.ok() ? s : rollback;
    }

    // An overwrite of an existing DRAM key is recorded as an access only after
    // the write and any required demotions have committed. A failed Put
    // therefore cannot permanently change the old key's LRU/ARC state.
    if (!policy_inserted && eviction_) {
        auto current = index_.Find(key);
        if (current.ok() && current.value().tier == TierType::kDram) eviction_->OnAccess(key);
    }
    return Status::Ok();
}

Status StorageNode::Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out) {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    if (!ready_) return Status::Unavailable("StorageNode::Get: node is not open");
    auto loc = index_.Find(key);
    if (!loc.ok()) {
        // Full miss: not resident in any tier on this node. The upper layer
        // recomputes and Puts (which calls OnInsert).
        return loc.status();
    }

    Tier* owner = TierOfLocked(loc.value().tier);
    if (owner == nullptr) return Status::Internal("index points at an unknown tier");

    // DRAM hit: update recency/frequency only after the physical read succeeds.
    if (loc.value().tier == TierType::kDram) {
        Status s = owner->Get(key, dst, out);
        if (s.ok() && eviction_) eviction_->OnAccess(key);
        return s;
    }

    // DRAM miss, but resident on a colder tier (SSD hit). Read it out, then
    // promote it back into DRAM. After the SSD read succeeds, OnAccess performs
    // ARC ghost-hit adaptation and list migration when the key was in B1/B2;
    // PromoteToDram's OnInsert handles the cold (key not in B1/B2) sub-case.
    BlockView local_view;
    BlockView* view = out ? out : &local_view;
    if (Status s = owner->Get(key, dst, view); !s.ok()) return s;
    if (eviction_) eviction_->OnAccess(key);
    PromoteToDramLocked(key, *view);
    return Status::Ok();
}

Result<bool> StorageNode::Contains(const BlockKey& key) const {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    if (!ready_) return Status::Unavailable("StorageNode::Contains: node is not open");
    return index_.Contains(key);
}

Result<Location> StorageNode::Locate(const BlockKey& key) const {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    if (!ready_) return Status::Unavailable("StorageNode::Locate: node is not open");
    return index_.Find(key);
}

}  // namespace tidepool
