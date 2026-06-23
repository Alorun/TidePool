// hash_ring.h — Consistent-hash ring for global, query-free addressing.
// Plane: DATA (hot path) — derived from CONTROL-plane ShardMap.
//
// KEY DESIGN PRINCIPLE: data-plane placement is by COMPUTATION, not lookup.
// Given a BlockKey, any client/node computes the owning node by hashing onto a
// consistent-hash ring built from the cached ShardMap. No central query is on
// the hot path. When membership changes, the Coordinator bumps the ShardMap
// version and clients rebuild their ring; only a minimal fraction of keys remap
// (the standard consistent-hashing property).
//
// Concrete class (the algorithm is fixed); virtual-node count per node tunes
// balance. TODO: make the point-hash function pluggable / bounded-load variant.
#pragma once

#include <cstdint>
#include <map>

#include "tidepool/api/block_key.h"
#include "tidepool/api/location.h"
#include "tidepool/api/shard_map.h"
#include "tidepool/api/status.h"

namespace tidepool {

class HashRing {
public:
    HashRing() = default;

    // (Re)build the ring from a ShardMap snapshot. Cheap enough to do on every
    // shard-map version change. Ignores nodes marked !alive.
    void Rebuild(const ShardMap& map);

    uint64_t version() const { return version_; }
    bool empty() const { return ring_.empty(); }

    // Map a key to its owning node by walking clockwise to the next vnode.
    // Returns kUnavailable if the ring is empty.
    Result<NodeId> Owner(const BlockKey& key) const;

    // TODO: OwnerSet(key, replicas) for future multi-replica fault tolerance.

private:
    uint64_t version_ = 0;
    // vnode point hash -> owning node id (std::map gives ordered clockwise walk).
    std::map<uint64_t, NodeId> ring_;
};

}  // namespace tidepool
