#include "tidepool/store/storage_node.h"

#include <utility>

namespace tidepool {

StorageNode::StorageNode(NodeId id, std::vector<std::unique_ptr<Tier>> tiers, std::unique_ptr<EvictionPolicy> eviction)
    : id_(std::move(id)), tiers_(std::move(tiers)), eviction_(std::move(eviction)) {}

Tier* StorageNode::TierOf(TierType type) {
    for (size_t i = 0; i < tiers_.size(); i++) {
        Tier* t = tiers_[i].get();
        if (t && t->type() == type) return t;
    }
    return nullptr;
}

Tier* StorageNode::ColderTier() {
    // Tiers are hottest-first; the demotion sink for DRAM is the next tier down.
    // In the MVP that is the SSD tier when present.
    return TierOf(TierType::kSsd);
}

void StorageNode::MakeDramRoom() {
    if (!eviction_) return;
    Tier* dram = TierOf(TierType::kDram);
    Tier* sink = ColderTier();
    // Nothing to demote into: leave the block in DRAM (capacity is advisory in
    // the MVP, exactly as before this policy wiring existed).
    if (dram == nullptr || sink == nullptr) return;

    // Demote DRAM victims to the colder tier until DRAM is back within its byte
    // budget. capacity_bytes == 0 means "unbounded" (advisory): skip.
    while (true) {
        TierStats st = dram->Stats();
        if (st.capacity_bytes == 0 || st.used_bytes <= st.capacity_bytes) break;

        // Victim() also moves the key onto ARC's ghost list, so we must NOT call
        // OnRemove afterwards (that would erase the ghost bookkeeping).
        auto victim = eviction_->Victim();
        if (!victim) break;

        // Read the victim's bytes out of DRAM so we can sink them.
        Block blk;
        blk.data.resize(st.used_bytes);  // upper bound; resized to fit below
        MutableBuffer dst{blk.data.data(), blk.data.size()};
        BlockView view;
        if (Status s = dram->Get(*victim, dst, &view); !s.ok()) break;
        blk.data.resize(view.size);
        blk.metadata = view.metadata;

        uint64_t handle = 0;
        if (Status s = sink->Put(*victim, blk, &handle); !s.ok()) {
            // Sink unavailable (e.g. SSD stub): give up demoting this block. It
            // stays in DRAM; the ghost move above is a harmless over-count that
            // does not occur in supported (DRAM+SSD) configurations.
            break;
        }
        dram->Evict(*victim);
        index_.Upsert(*victim, Location{id_, sink->type(), handle});
    }
}

void StorageNode::PromoteToDram(const BlockKey& key, const BlockView& view) {
    Tier* dram = TierOf(TierType::kDram);
    if (dram == nullptr) return;  // no DRAM tier: nothing to promote into.

    // Reconstruct an owning block from the bytes already in the caller buffer.
    Block blk;
    blk.metadata = view.metadata;
    blk.data.assign(view.data, view.data + view.size);

    MakeDramRoom();  // evict/sink as needed before adding a block.

    uint64_t handle = 0;
    if (Status s = dram->Put(key, blk, &handle); !s.ok()) return;  // best-effort.
    index_.Upsert(key, Location{id_, dram->type(), handle});
    // Cold SSD-hit -> ARC Case IV (T1). If OnAccess already promoted the key out
    // of a ghost list, OnInsert is a no-op (it stays in T2). Either way correct.
    if (eviction_) eviction_->OnInsert(key, blk.size_bytes());
}

Status StorageNode::Put(const BlockKey& key, const Block& block) {
    if (tiers_.empty()) return Status::Internal("storage node has no tiers");
    // Place into the hottest tier (front). Cross-tier demotion on capacity
    // pressure is orchestrated here.
    Tier* hot = tiers_.front().get();
    uint64_t handle = 0;
    if (Status s = hot->Put(key, block, &handle); !s.ok()) return s;
    index_.Upsert(key, Location{id_, hot->type(), handle});
    // OnInsert records the cold insert (ARC: Case IV -> T1; LRU: push front).
    if (eviction_) eviction_->OnInsert(key, block.size_bytes());
    // After inserting, demote victims if the DRAM tier is now over budget.
    MakeDramRoom();
    return Status::Ok();
}

Status StorageNode::Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out) {
    // Contract (differs from a hit-only policy): notify the policy of EVERY
    // access FIRST, including misses. An adaptive policy (ARC) must observe
    // ghost-list hits to tune itself; for an unknown key OnAccess is a no-op.
    // LRU's OnAccess is likewise a no-op on keys it does not track.
    if (eviction_) eviction_->OnAccess(key);

    auto loc = index_.Find(key);
    if (!loc.ok()) {
        // Full miss: not resident in any tier on this node. The upper layer
        // recomputes and Puts (which calls OnInsert). OnAccess above already
        // gave ARC its chance to react to a ghost hit, if any.
        return loc.status();
    }

    Tier* owner = TierOf(loc.value().tier);
    if (owner == nullptr) return Status::Internal("index points at an unknown tier");

    // DRAM hit: OnAccess already recorded the recency/frequency signal.
    if (loc.value().tier == TierType::kDram) {
        return owner->Get(key, dst, out);
    }

    // DRAM miss, but resident on a colder tier (SSD hit). Read it out, then
    // promote it back into DRAM. The OnAccess above has already done ARC's
    // ghost-hit p-adaptation and list migration when the key was in B1/B2;
    // PromoteToDram's OnInsert handles the cold (key not in B1/B2) sub-case.
    BlockView local_view;
    BlockView* view = out ? out : &local_view;
    if (Status s = owner->Get(key, dst, view); !s.ok()) return s;
    PromoteToDram(key, *view);
    return Status::Ok();
}

bool StorageNode::Contains(const BlockKey& key) const { return index_.Contains(key); }

}  // namespace tidepool
