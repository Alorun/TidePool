#include "tidepool/hashring/hash_ring.h"

#include <functional>
#include <string>

namespace tidepool {
namespace {

// Point hash for a vnode label like "node-7#13". TODO: swap std::hash for a
// well-distributed 64-bit hash (e.g. xxh3) — std::hash quality is impl-defined.
uint64_t PointHash(const std::string& label) { return std::hash<std::string>{}(label); }

}  // namespace

void HashRing::Rebuild(const ShardMap& map) {
    ring_.clear();
    version_ = map.version;
    for (const auto& node : map.nodes) {
        if (!node.alive) continue;
        const uint32_t vnodes = node.vnode_count > 0 ? node.vnode_count : 1;
        for (uint32_t v = 0; v < vnodes; ++v) {
            const std::string label = node.id + "#" + std::to_string(v);
            ring_[PointHash(label)] = node.id;
        }
    }
}

Result<NodeId> HashRing::Owner(const BlockKey& key) const {
    if (ring_.empty()) {
        return Status::Unavailable("hash ring is empty; refresh shard map");
    }
    // Walk clockwise: first vnode with point >= key hash, wrapping around.
    auto it = ring_.lower_bound(key.prefix_hash);
    if (it == ring_.end()) it = ring_.begin();
    return it->second;
}

}  // namespace tidepool
