// shard_map.h — Cluster membership + shard assignment. Plane: CONTROL.
//
// The ShardMap is the single piece of cluster-global state. It is produced and
// versioned by the Coordinator (control plane, strongly consistent) and cached
// read-only by every Connector (data plane). Clients route by *computing*
// against their cached ring; they only re-fetch the ShardMap when its version
// is found to be stale (e.g. a read returns kUnavailable).
//
// IMPORTANT: consensus lives ONLY here, in the control plane. The map changes
// infrequently (node join/leave/rebalance). The get/put hot path never reads
// or writes consensus state.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "location.h"

namespace tidepool {

struct NodeInfo {
    NodeId id;
    std::string address;       // "host:port" for the data-plane transport
    uint32_t vnode_count = 0;  // virtual nodes on the ring (load-balancing knob)
    bool alive = true;         // last-known liveness per Coordinator heartbeats
};

// A monotonically-versioned snapshot of membership. The consistent-hash ring
// is *derived* from `nodes` (see hashring/hash_ring.h) so that the map stays a
// compact, easily-replicated description.
struct ShardMap {
    uint64_t version = 0;
    std::vector<NodeInfo> nodes;

    // Index-based loop on purpose: a range-based for desugars to begin()/end() iterators.
    const NodeInfo* FindNode(const NodeId& id) const {
        for (std::size_t i = 0; i < nodes.size(); ++i) {
            if (nodes[i].id == id) return &nodes[i];
        }
        return nullptr;
    }
};

}  // namespace tidepool
