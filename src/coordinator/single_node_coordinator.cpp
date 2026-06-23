#include "single_node_coordinator.h"

#include "tidepool/coordinator/factory.h"

namespace tidepool {

std::shared_ptr<Coordinator> MakeSingleNodeCoordinator() { return std::make_shared<SingleNodeCoordinator>(); }

Result<ShardMap> SingleNodeCoordinator::GetShardMap() {
    std::lock_guard<std::mutex> lock(mu_);
    return map_;  // copy; clients cache it and route locally
}

Status SingleNodeCoordinator::RegisterNode(const NodeInfo& node) {
    if (node.id.empty()) return Status::InvalidArgument("empty node id");
    std::lock_guard<std::mutex> lock(mu_);
    // Replace existing entry or append, then bump the version so clients
    // rebuild their hash ring. TODO: persist + replicate this transition under
    // Raft.
    for (size_t i = 0; i < map_.nodes.size(); i++) {
        if (map_.nodes[i].id == node.id) {
            map_.nodes[i] = node;
            map_.version++;
            return Status::Ok();
        }
    }
    map_.nodes.push_back(node);
    map_.version++;
    return Status::Ok();
}

Status SingleNodeCoordinator::Heartbeat(const NodeId& node_id, uint64_t* out_shard_map_version) {
    std::lock_guard<std::mutex> lock(mu_);
    const NodeInfo* n = map_.FindNode(node_id);
    if (n == nullptr) return Status::NotFound("unknown node: " + node_id);
    // TODO: record last-heartbeat time; a sweeper marks silent nodes dead,
    // bumps the version, and triggers a ring rebalance.
    if (out_shard_map_version) *out_shard_map_version = map_.version;
    return Status::Ok();
}

}  // namespace tidepool
