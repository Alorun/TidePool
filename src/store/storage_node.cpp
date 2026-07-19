#include "tidepool/store/storage_node.h"

#include <limits>
#include <new>
#include <string>
#include <utility>

namespace tidepool {
namespace {

Status CancelVictimOrInternal(EvictionPolicy* policy, const BlockKey& key, const Status& cause,
                              const char* context) {
    Status cancelled = policy->CancelVictim(key);
    if (cancelled.ok()) return cause;
    return Status::Internal(std::string(context) + ": " + cause.ToString() +
                            "; victim cancellation failed: " + cancelled.ToString());
}

bool ProjectedBytes(uint64_t used, uint64_t replaced, size_t incoming, uint64_t* projected) {
    if (used < replaced) return false;
    const uint64_t base = used - replaced;
    if (incoming > std::numeric_limits<uint64_t>::max() - base) return false;
    *projected = base + static_cast<uint64_t>(incoming);
    return true;
}

}  // namespace

StorageNode::StorageNode(NodeId id, std::vector<std::unique_ptr<Tier>> tiers,
                         std::unique_ptr<EvictionPolicy> eviction)
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

    LocalIndex recovered;
    if (failure.ok()) {
        for (size_t i = 0; i < tiers_.size(); ++i) {
            Tier* tier = tiers_[i].get();
            failure = tier->VisitEntries([&](const BlockKey& key, const BlockInfo& info) {
                try {
                    return recovered.InsertIfAbsent(
                        key, Location{id_, tier->type(), info.handle});
                } catch (const std::bad_alloc&) {
                    return Status(StatusCode::kOutOfCapacity,
                                  "StorageNode::Open index recovery allocation failed");
                }
            });
            if (!failure.ok()) {
                failed_index = i;
                break;
            }
        }
    }

    if (!failure.ok()) {
        std::string message = "StorageNode::Open: tier[" + std::to_string(failed_index) + "]";
        if (failed_index < tiers_.size() && tiers_[failed_index]) {
            message += " (" + std::string(TierTypeName(tiers_[failed_index]->type())) + ")";
        }
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

    index_.Swap(recovered);
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
            first_error = Status(s.code(), "StorageNode::Close: tier[" + std::to_string(i - 1) +
                                                   "] (" + TierTypeName(tier->type()) +
                                                   ") failed: " + s.ToString());
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
    for (const auto& tier : tiers_) {
        if (tier && tier->type() == type) return tier.get();
    }
    return nullptr;
}

Tier* StorageNode::ColderTierLocked() { return TierOfLocked(TierType::kSsd); }

Status StorageNode::ReadBlockLocked(Tier* tier, const BlockKey& key, Block* out) {
    if (tier == nullptr || out == nullptr) {
        return Status::InvalidArgument("ReadBlockLocked received a null argument");
    }
    auto info = tier->Probe(key);
    if (!info.ok()) return info.status();
    try {
        out->data.resize(info.value().payload_size);
    } catch (const std::bad_alloc&) {
        return Status(StatusCode::kOutOfCapacity, "failed to allocate tier read buffer");
    }

    MutableBuffer dst{out->data.empty() ? nullptr : out->data.data(), out->data.size()};
    BlockView view;
    if (Status s = tier->Get(key, dst, &view); !s.ok()) return s;
    if (view.size != info.value().payload_size) {
        return Status::Internal("Tier Probe/Get size changed while StorageNode lock was held");
    }
    out->metadata = view.metadata;
    return Status::Ok();
}

Status StorageNode::DemoteOneVictimLocked(Tier* dram, Tier* sink,
                                          const std::optional<BlockKey>& excluded) {
    if (eviction_ == nullptr) return Status::Internal("demotion requires an eviction policy");
    Result<BlockKey> selected = eviction_->SelectVictim(excluded);
    if (!selected.ok()) return selected.status();
    const BlockKey victim = selected.value();

    if (Status s = index_.ValidateRelocate(victim, TierType::kDram); !s.ok()) {
        return CancelVictimOrInternal(eviction_.get(), victim, s, "demotion index preflight");
    }

    Block block;
    if (Status s = ReadBlockLocked(dram, victim, &block); !s.ok()) {
        return CancelVictimOrInternal(eviction_.get(), victim, s, "demotion read preflight");
    }
    if (Status s = dram->ValidateEraseExisting(victim); !s.ok()) {
        return CancelVictimOrInternal(eviction_.get(), victim, s, "demotion DRAM preflight");
    }
    if (Status s = eviction_->ValidateVictimCommit(victim); !s.ok()) {
        return CancelVictimOrInternal(eviction_.get(), victim, s, "demotion policy preflight");
    }

    uint64_t sink_handle = 0;
    if (Status s = sink->Put(victim, block, &sink_handle); !s.ok()) {
        return CancelVictimOrInternal(eviction_.get(), victim, s, "demotion SSD Put");
    }

    // Persistence commit point. Every remaining operation is allocation-free
    // and cannot fail under the preflight invariants plus the node-wide lock.
    dram->EraseExisting(victim);
    index_.RelocateExisting(victim, TierType::kDram, sink->type(), sink_handle);
    eviction_->CommitVictim(victim);
    stats_.demotions++;
    stats_.bytes_read += block.size_bytes();
    stats_.bytes_written += block.size_bytes();
    return Status::Ok();
}

Status StorageNode::MakeDramRoomForLocked(
    size_t incoming_bytes, const std::optional<BlockKey>& replacing_dram_key) {
    Tier* dram = TierOfLocked(TierType::kDram);
    if (dram == nullptr) return Status::Ok();

    uint64_t replaced_bytes = 0;
    if (replacing_dram_key) {
        auto old = dram->Probe(*replacing_dram_key);
        if (!old.ok()) return old.status();
        replaced_bytes = old.value().payload_size;
    }

    while (true) {
        const TierStats before = dram->Stats();
        if (before.capacity_bytes == 0) return Status::Ok();
        uint64_t projected = 0;
        if (!ProjectedBytes(before.used_bytes, replaced_bytes, incoming_bytes, &projected)) {
            return Status(StatusCode::kOutOfCapacity, "projected DRAM footprint overflow");
        }
        if (projected <= before.capacity_bytes) return Status::Ok();
        if (eviction_ == nullptr) {
            return Status(StatusCode::kOutOfCapacity,
                          "DRAM needs space and has no eviction policy");
        }
        Tier* sink = ColderTierLocked();
        if (sink == nullptr) {
            return Status(StatusCode::kOutOfCapacity, "DRAM needs space and has no SSD sink");
        }

        Status demoted = DemoteOneVictimLocked(dram, sink, replacing_dram_key);
        if (!demoted.ok()) {
            if (demoted.code() == StatusCode::kNotFound) {
                return Status(StatusCode::kOutOfCapacity,
                              "DRAM needs space but no unpinned victim is available");
            }
            return demoted;
        }
        const TierStats after = dram->Stats();
        if (after.used_bytes >= before.used_bytes && after.num_blocks >= before.num_blocks) {
            return Status::Internal("DRAM demotion committed without making progress");
        }
    }
}

Status StorageNode::PutWithoutDramLocked(const BlockKey& key, const Block& block,
                                         const std::optional<Location>& old_location) {
    Tier* hot = tiers_.front().get();
    bool inserted_index = false;
    if (old_location) {
        if (Status s = index_.ValidateRelocate(key, old_location->tier); !s.ok()) return s;
        index_.RelocateExisting(key, old_location->tier, hot->type(), 0);
    } else {
        if (Status s = index_.Upsert(key, Location{id_, hot->type(), 0}); !s.ok()) return s;
        inserted_index = true;
    }

    uint64_t handle = 0;
    Status stored = hot->Put(key, block, &handle);
    if (!stored.ok()) {
        if (inserted_index) {
            index_.RemoveExisting(key);
        } else {
            index_.RelocateExisting(key, hot->type(), old_location->tier,
                                    old_location->handle);
        }
        return stored;
    }
    index_.RelocateExisting(key, hot->type(), hot->type(), handle);
    stats_.bytes_written += block.size_bytes();
    return Status::Ok();
}

Status StorageNode::PutOversizedLocked(const BlockKey& key, const Block& block, Tier* dram,
                                       Tier* sink,
                                       const std::optional<Location>& old_location) {
    bool inserted_index = false;
    if (old_location) {
        if (Status s = index_.ValidateRelocate(key, old_location->tier); !s.ok()) return s;
        if (old_location->tier == TierType::kDram) {
            if (Status s = dram->ValidateEraseExisting(key); !s.ok()) return s;
        }
    } else {
        if (Status s = index_.Upsert(key, Location{id_, sink->type(), 0}); !s.ok()) return s;
        inserted_index = true;
    }

    uint64_t handle = 0;
    Status stored = sink->Put(key, block, &handle);
    if (!stored.ok()) {
        if (inserted_index) index_.RemoveExisting(key);
        return stored;
    }

    if (old_location && old_location->tier == TierType::kDram) {
        dram->EraseExisting(key);
        if (eviction_) eviction_->OnRemove(key);
    }
    if (old_location) {
        index_.RelocateExisting(key, old_location->tier, sink->type(), handle);
    } else {
        index_.RelocateExisting(key, sink->type(), sink->type(), handle);
    }
    stats_.bytes_written += block.size_bytes();
    return Status::Ok();
}

void StorageNode::TryPromoteToDramLocked(const BlockKey& key, const BlockView& view) {
    Tier* dram = TierOfLocked(TierType::kDram);
    if (dram == nullptr || eviction_ == nullptr) return;
    const TierStats dram_stats = dram->Stats();
    if (dram_stats.capacity_bytes != 0 && view.size > dram_stats.capacity_bytes) return;
    if (!index_.ValidateRelocate(key, TierType::kSsd).ok()) return;
    auto already = dram->Probe(key);
    if (already.ok() || already.status().code() != StatusCode::kNotFound) return;

    Block block;
    block.metadata = view.metadata;
    try {
        if (view.size != 0) block.data.assign(view.data, view.data + view.size);
    } catch (const std::bad_alloc&) {
        return;
    }

    if (!MakeDramRoomForLocked(block.size_bytes()).ok()) return;
    uint64_t handle = 0;
    if (!dram->Put(key, block, &handle).ok()) return;

    Status policy = eviction_->OnPromotionCommitted(key, block.size_bytes());
    if (!policy.ok()) {
        if (!dram->ValidateEraseExisting(key).ok()) std::terminate();
        dram->EraseExisting(key);
        return;
    }

    // Inclusive cache: the SSD record remains. LocalIndex names DRAM as the
    // primary only after both DRAM and policy resident state have committed.
    index_.RelocateExisting(key, TierType::kSsd, TierType::kDram, handle);
    stats_.promotions++;
    stats_.bytes_written += block.size_bytes();
}

Status StorageNode::Put(const BlockKey& key, const Block& block) {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    if (!ready_) return Status::Unavailable("StorageNode::Put: node is not open");
    if (tiers_.empty() || tiers_.front() == nullptr) {
        return Status::Internal("storage node has no hot tier");
    }

    std::optional<Location> old_location;
    auto existing = index_.Find(key);
    if (existing.ok()) {
        old_location = existing.value();
    } else if (existing.status().code() != StatusCode::kNotFound) {
        return existing.status();
    }

    Tier* hot = tiers_.front().get();
    if (hot->type() != TierType::kDram) {
        return PutWithoutDramLocked(key, block, old_location);
    }

    Tier* sink = ColderTierLocked();
    const TierStats hot_stats = hot->Stats();
    if (hot_stats.capacity_bytes != 0 && block.size_bytes() > hot_stats.capacity_bytes) {
        if (sink == nullptr) {
            return Status(StatusCode::kOutOfCapacity,
                          "block is larger than DRAM capacity and no SSD sink exists");
        }
        return PutOversizedLocked(key, block, hot, sink, old_location);
    }

    std::optional<BlockKey> replacing_dram;
    if (old_location && old_location->tier == TierType::kDram) replacing_dram = key;
    if (Status s = MakeDramRoomForLocked(block.size_bytes(), replacing_dram); !s.ok()) {
        return s;
    }

    if (old_location) {
        if (Status s = index_.ValidateRelocate(key, old_location->tier); !s.ok()) return s;
    }

    uint64_t handle = 0;
    if (Status s = hot->Put(key, block, &handle); !s.ok()) return s;

    const bool needs_policy_insert =
        !old_location || old_location->tier != TierType::kDram;
    if (needs_policy_insert && eviction_) {
        if (Status s = eviction_->OnInsert(key, block.size_bytes()); !s.ok()) {
            if (!hot->ValidateEraseExisting(key).ok()) std::terminate();
            hot->EraseExisting(key);
            return s;
        }
    }

    if (old_location) {
        index_.RelocateExisting(key, old_location->tier, TierType::kDram, handle);
    } else {
        Status indexed = index_.Upsert(key, Location{id_, TierType::kDram, handle});
        if (!indexed.ok()) {
            if (eviction_) eviction_->OnRemove(key);
            if (!hot->ValidateEraseExisting(key).ok()) std::terminate();
            hot->EraseExisting(key);
            return indexed;
        }
    }
    if (!needs_policy_insert && eviction_) eviction_->OnAccess(key);
    stats_.bytes_written += block.size_bytes();
    return Status::Ok();
}

Status StorageNode::Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out) {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    if (out) *out = BlockView{};
    if (!ready_) return Status::Unavailable("StorageNode::Get: node is not open");
    auto loc = index_.Find(key);
    if (!loc.ok()) {
        stats_.misses++;
        return loc.status();
    }

    Tier* owner = TierOfLocked(loc.value().tier);
    if (owner == nullptr) return Status::Internal("index points at an unknown tier");

    Status fetched = owner->Get(key, dst, out);
    if (!fetched.ok()) {
        if (fetched.code() == StatusCode::kNotFound) stats_.misses++;
        return fetched;
    }

    stats_.bytes_read += out->size;
    if (loc.value().tier == TierType::kDram) {
        stats_.dram_hits++;
        if (eviction_) eviction_->OnAccess(key);
        return Status::Ok();
    }

    stats_.ssd_hits++;
    TryPromoteToDramLocked(key, *out);
    return Status::Ok();
}

Result<BlockInfo> StorageNode::Probe(const BlockKey& key) {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    if (!ready_) return Status::Unavailable("StorageNode::Probe: node is not open");
    auto loc = index_.Find(key);
    if (!loc.ok()) return loc.status();
    Tier* owner = TierOfLocked(loc.value().tier);
    if (owner == nullptr) return Status::Internal("index points at an unknown tier");
    return owner->Probe(key);
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

StorageNodeStats StorageNode::Stats() const {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    return stats_;
}

}  // namespace tidepool
