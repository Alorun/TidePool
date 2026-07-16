// storage_node.h — A single storage node = one slice of the shared pool.
// Plane: DATA (hot path serving) + a thin CONTROL-plane registration edge.
//
// A StorageNode owns the node-internal stack: an ordered list of Tiers
// (DRAM -> SSD), a LocalIndex, and an EvictionPolicy. N storage nodes together
// form the cross-instance shared KV cache pool. The node serves Put/Get for the
// keys that the hash ring assigns to it, and demotes/evicts across its own
// tiers when capacity is tight.
//
// What this node does NOT do: it never runs consensus and never decides global
// placement. It registers itself with the Coordinator (control plane) and then
// serves whatever keys hash to it.
//
// CONCURRENCY MODEL:
//   * Expected workload: many concurrent Get/Put from the data-plane RPC server
//     (one logical request per worker thread). Reads dominate.
//   * A correctness-first lifecycle mutex currently serializes Open/Close and
//     all node operations. Close therefore cannot tear down a Tier while an
//     operation is in flight, and LocalIndex is not accessed concurrently.
//   * TODO(concurrency-index): shard LocalIndex by key hash with a per-shard
//     reader/writer lock (or a concurrent hash map) before relaxing the node-wide
//     lock so Get/Put can scale.
#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "tidepool/api/block.h"
#include "tidepool/api/block_key.h"
#include "tidepool/api/status.h"
#include "tidepool/store/eviction_policy.h"
#include "tidepool/store/local_index.h"
#include "tidepool/store/tier.h"

namespace tidepool {

// Tiers are supplied hottest-first (e.g. {DRAM, SSD}).
class StorageNode {
public:
    StorageNode(NodeId id, std::vector<std::unique_ptr<Tier>> tiers, std::unique_ptr<EvictionPolicy> eviction);
    ~StorageNode();

    StorageNode(const StorageNode&) = delete;
    StorageNode& operator=(const StorageNode&) = delete;
    StorageNode(StorageNode&&) = delete;
    StorageNode& operator=(StorageNode&&) = delete;

    const NodeId& id() const { return id_; }

    // Open tiers in configured order. On failure, previously opened tiers are
    // closed in reverse order and the node remains unavailable.
    Status Open();
    // Close every tier in reverse order. Safe to call repeatedly.
    Status Close();
    bool IsReady() const;

    // Hot-path serving API (invoked by the local RPC server in
    // apps/tidepool_node)
    // ----------------------------------------------------------------------------
    // Insert a block, placing it in the hottest tier and updating the index.
    // May trigger demotion/eviction via the policy when capacity is tight.
    Status Put(const BlockKey& key, const Block& block);

    // Fetch a block into the caller-owned `dst`, consulting the index then the
    // owning tier; `*out` views the filled bytes on success. On a hot read the
    // policy is notified (OnAccess) and the block may be promoted. Signature
    // mirrors Tier::Get so the no-copy contract holds end to end (see buffer.h).
    Status Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out);

    // Cheap presence check used by Connector::Lookup batching. Returns
    // kUnavailable while the node is not open.
    Result<bool> Contains(const BlockKey& key) const;

private:
    // Return the owned tier of the given type, or nullptr if this node has none.
    Tier* TierOf(TierType type);
    // The next colder tier after DRAM (the demotion sink), or nullptr.
    Tier* ColderTier();
    // Free DRAM slots while the DRAM tier is over its (advisory) byte budget by
    // asking the policy for victims and sinking them to the colder tier. No-op
    // when there is no colder tier or no victim. Best-effort: stops if the sink
    // write fails (e.g. SSD backend not built).
    void MakeDramRoom();
    // Backfill a block just read from a colder tier into DRAM (ARC promotion):
    // make room, copy into DRAM, repoint the index, notify the policy.
    void PromoteToDram(const BlockKey& key, const BlockView& view);

    NodeId id_;
    std::vector<std::unique_ptr<Tier>> tiers_;  // hottest-first
    std::unique_ptr<EvictionPolicy> eviction_;
    LocalIndex index_;
    mutable std::mutex lifecycle_mu_;
    bool ready_ = false;
};

}  // namespace tidepool
