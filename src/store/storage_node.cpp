#include "tidepool/store/storage_node.h"

#include <utility>

namespace tidepool {

StorageNode::StorageNode(NodeId id, std::vector<std::unique_ptr<Tier>> tiers, std::unique_ptr<EvictionPolicy> eviction)
    : id_(std::move(id)), tiers_(std::move(tiers)), eviction_(std::move(eviction)) {}

Status StorageNode::Put(const BlockKey& key, const Block& block) {
    if (tiers_.empty()) return Status::Internal("storage node has no tiers");
    // Place into the hottest tier (front). Cross-tier demotion on capacity
    // pressure is orchestrated here.
    Tier* hot = tiers_.front().get();
    uint64_t handle = 0;
    // TODO: before insert, if hot tier is over capacity, ask
    // eviction_->Victim() and demote that block to the next-colder tier (DRAM
    // -> SSD) instead of dropping it. Cascade as needed.
    if (Status s = hot->Put(key, block, &handle); !s.ok()) return s;
    index_.Upsert(key, Location{id_, hot->type(), handle});
    if (eviction_) eviction_->OnInsert(key, block.size_bytes());
    return Status::Ok();
}

Status StorageNode::Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out) {
    auto loc = index_.Find(key);
    if (!loc.ok()) return loc.status();

    // Find the tier that currently owns the block and read into the caller
    // buffer.
    for (size_t i = 0; i < tiers_.size(); i++) {
        auto tier = std::move(tiers_[i]);
        if (tier->type() != loc.value().tier) continue;
        Status s = tier->Get(key, dst, out);
        if (s.ok() && eviction_) eviction_->OnAccess(key);
        // TODO: on an SSD hit, promote the block back into DRAM and update
        // index_.
        return s;
    }
    return Status::Internal("index points at an unknown tier");
}

bool StorageNode::Contains(const BlockKey& key) const { return index_.Contains(key); }

}  // namespace tidepool
