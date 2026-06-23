// single_node_coordinator.h — Embedded single-node Coordinator. Plane: CONTROL.
//
// MVP control plane: keeps the ShardMap in memory on one process. No real
// consensus — it is the trivial "cluster of one coordinator" case. A Raft- or
// etcd-backed implementation plugs in behind the SAME Coordinator ABC later
// (ROADMAP stage 2+). Suitable for single-coordinator deployments and tests.
#pragma once

#include <cstdint>
#include <mutex>

#include "tidepool/coordinator/coordinator.h"

namespace tidepool {

class SingleNodeCoordinator : public Coordinator {
public:
    SingleNodeCoordinator() = default;

    Result<ShardMap> GetShardMap() override;
    Status RegisterNode(const NodeInfo& node) override;
    Status Heartbeat(const NodeId& node_id, uint64_t* out_shard_map_version) override;

private:
    mutable std::mutex mu_;
    ShardMap map_;  // version starts at 0; bumped on every membership change.
};

}  // namespace tidepool
