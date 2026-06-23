// connector.h — Public client API ("Connector"). Plane: DATA (hot path).
//
// The Connector is what an inference instance links against. Responsibilities:
//   * expose the public Lookup/Get/Put/Prefetch API;
//   * route a BlockKey to its owning node by COMPUTING against a locally cached
//     consistent-hash ring (no central query on the hot path);
//   * cache the ShardMap; refresh from the Coordinator only on stale-version;
//   * serialize/deserialize and chunk KV blocks for the Transfer Engine.
//
// This is the seam between the inference engine and the pool. Concrete class
// composed from pluggable parts (Transport, Coordinator, HashRing).
#pragma once

#include <memory>
#include <vector>

#include "tidepool/api/types.h"
#include "tidepool/coordinator/coordinator.h"
#include "tidepool/hashring/hash_ring.h"
#include "tidepool/store/storage_node.h"
#include "tidepool/transport/transport.h"

namespace tidepool {

struct ConnectorOptions {
    // How many times to refresh the ShardMap + retry on kUnavailable before
    // giving up on a request. TODO: make backoff configurable.
    int max_shardmap_refreshes = 1;
};

class Connector {
public:
    // `self_node` is this instance's node id (empty => pure client with no
    // local node). `local_node` is the co-located StorageNode used for the
    // local-hit fast path (not owned, may be null). The Connector holds the
    // Transport ABC by pointer so that swapping TcpTransport -> RdmaTransport
    // requires NO change here — that is the seam this revision makes explicit.
    Connector(std::shared_ptr<Coordinator> coordinator, std::shared_ptr<Transport> transport, NodeId self_node = {},
              StorageNode* local_node = nullptr, ConnectorOptions options = {});

    // Batch-probe presence so the engine can decide what to recompute before
    // fetching. hit_map[i] == true iff keys[i] is resident somewhere in the
    // pool.
    // TODO: fan out probes per owning node in parallel.
    Result<HitMap> Lookup(const std::vector<BlockKey>& keys);

    // Fetch a block from its owning node into the caller-owned `dst`; `*out`
    // views the bytes on success. Routing forks explicitly in the .cpp: a LOCAL
    // hit (owner == self_node) is served from `local_node_` with no network; a
    // REMOTE hit moves bytes via transport_->ReadRemote(...). No-copy contract
    // per buffer.h; transport-agnostic so RDMA drops in unchanged.
    Status Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out);

    // Publish a block to its owning node so other instances can reuse it. Same
    // local/remote fork: local -> local_node_->Put, remote -> WriteRemote.
    Status Put(const BlockKey& key, const Block& block);

    // Optional: hint the pool to warm these keys (e.g. promote SSD->DRAM, or
    // pull toward the requesting locality). Best-effort, may be a no-op.
    Status Prefetch(const std::vector<BlockKey>& keys);

private:
    // Ensure the cached ring is fresh enough to route `key`; refresh from the
    // coordinator if empty/stale. Returns the owning node id.
    Result<NodeId> ResolveOwner(const BlockKey& key);
    Status RefreshShardMap();

    std::shared_ptr<Coordinator> coordinator_;
    std::shared_ptr<Transport> transport_;
    NodeId self_node_;          // this instance's node id ("" => pure client)
    StorageNode* local_node_;   // co-located node for owner==self hits (not owned)
    ConnectorOptions options_;
    HashRing ring_;             // locally cached, rebuilt from the ShardMap on refresh
};

}  // namespace tidepool
