// coordinator.h — Cluster control plane. Plane: CONTROL (low-frequency,
// strongly consistent).
//
// The Coordinator owns the authoritative, versioned ShardMap (membership +
// shard assignment) and configuration. Nodes register and heartbeat here;
// clients fetch (and cache) the ShardMap here on a miss/stale-version.
//
// CONSENSUS BELONGS HERE AND ONLY HERE. The MVP implementation is an embedded
// single-node coordinator; a Raft-backed implementation plugs in behind this
// SAME abstract base class. The get/put data path must never call into the
// Coordinator on the hot path — only on cold-start or shard-map invalidation.
//
// Pluggable seam: Coordinator is a pure abstract base class
// (single-node / Raft / etcd-backed are interchangeable).
#pragma once

#include <cstdint>

#include "tidepool/api/shard_map.h"
#include "tidepool/api/status.h"

namespace tidepool {

// RAFT-BACKED REPLACEMENT CONTRACT
// --------------------------------
// A future RaftCoordinator (reusing a separate Raft KV metadata layer) must
// satisfy these method semantics WITHOUT changing any signature below, so it
// can drop in for SingleNodeCoordinator transparently. The semantics per
// method:
//
//   GetShardMap   — LINEARIZABLE READ. Must reflect every committed membership
//                   change. Implement via Raft read-index or a leader lease;
//                   stale follower reads are only acceptable if the returned
//                   ShardMap.version lets the caller detect+refresh on
//                   staleness (clients already gate on version, so
//                   bounded-staleness reads are a valid optimization).
//   RegisterNode  — CONSENSUS WRITE. Append a membership-change entry to the
//   Raft
//                   log; only return ok() AFTER it commits. The version bump
//                   and ring rebalance are a deterministic function of the
//                   committed entry so every replica derives the same ShardMap.
//   Heartbeat     — LEASE RENEWAL, intentionally NOT a per-call consensus write
//                   (it is high-frequency and must stay off the consensus
//                   path). Renew the node's lease locally on the leader; only a
//                   STATE TRANSITION (a lease expiring -> node marked dead, or
//                   a dead node returning) is a consensus write that bumps the
//                   version.
//
// VERSION RULE (all implementations): ShardMap.version is monotonic and bumps
// exactly once per committed membership change (join / leave / liveness flip).
// It never moves on a pure read or a plain lease renewal. Clients treat a
// higher version as "rebuild your ring"; equal version means "your routing is
// current".
class Coordinator {
public:
    virtual ~Coordinator() = default;

    // Return the current, versioned ShardMap. Clients cache the result and only
    // call again when their cached version is rejected as stale.
    // Raft impl: linearizable read (see contract above).
    virtual Result<ShardMap> GetShardMap() = 0;

    // Register (or re-register) a storage node. Triggers a ShardMap version
    // bump and ring rebalance. TODO: idempotency + token/lease on
    // (re)registration. Raft impl: consensus write; return only after the entry
    // commits.
    virtual Status RegisterNode(const NodeInfo& node) = 0;

    // Liveness heartbeat from a node. On return, `*out_shard_map_version`
    // carries the coordinator's current version so the node can detect it must
    // refresh.
    // TODO: lease expiry -> mark node dead -> bump version -> rebalance.
    // Raft impl: lease renewal (NOT consensus); only a liveness state flip is a
    // consensus write that bumps the version.
    virtual Status Heartbeat(const NodeId& node_id, uint64_t* out_shard_map_version) = 0;
};

}  // namespace tidepool
