#include "tidepool/client/connector.h"

#include <utility>

namespace tidepool {

Connector::Connector(std::shared_ptr<Coordinator> coordinator, std::shared_ptr<Transport> transport, NodeId self_node,
                     StorageNode* local_node, ConnectorOptions options)
    : coordinator_(std::move(coordinator)),
      transport_(std::move(transport)),
      self_node_(std::move(self_node)),
      local_node_(local_node),
      options_(options) {}

Status Connector::RefreshShardMap() {
    if (!coordinator_) return Status::Internal("connector has no coordinator");
    Result<ShardMap> map = coordinator_->GetShardMap();
    if (!map.ok()) return map.status();
    ring_.Rebuild(map.value());
    return Status::Ok();
}

Result<NodeId> Connector::ResolveOwner(const BlockKey& key) {
    // Data-plane addressing by COMPUTATION against the cached ring. Only
    // refresh from the coordinator if the ring is empty/stale — never on the
    // steady-state hot path.
    if (ring_.empty()) {
        if (Status s = RefreshShardMap(); !s.ok()) return s;
    }
    Result<NodeId> owner = ring_.Owner(key);
    if (owner.ok()) return owner;

    // Stale/empty ring: refresh up to the configured budget and retry.
    for (int i = 0; i < options_.max_shardmap_refreshes; ++i) {
        if (Status s = RefreshShardMap(); !s.ok()) return s;
        owner = ring_.Owner(key);
        if (owner.ok()) return owner;
    }
    return owner;
}

Result<HitMap> Connector::Lookup(const std::vector<BlockKey>& keys) {
    // TODO: group keys by owning node (ResolveOwner), issue one batched
    // presence probe per node over the transport, assemble the bitmap. Stubbed
    // for now.
    (void)keys;
    return Status::NotImplemented("Connector::Lookup");
}

Status Connector::Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out) {
    Result<NodeId> owner = ResolveOwner(key);
    if (!owner.ok()) return owner.status();

    // LOCAL HIT: the owning node is this instance's co-located StorageNode —
    // serve in-process, no network, no Transport. This is the fast path.
    if (local_node_ != nullptr && owner.value() == self_node_) {
        return local_node_->Get(key, dst, out);
    }

    // REMOTE: move bytes with the Transfer Engine. The Connector does not know
    // (or care) whether transport_ is TCP or RDMA — swapping the impl needs no
    // change here. ReadRemote lands bytes directly into the caller-owned dst (a
    // registered region under RDMA), preserving the no-copy contract.
    RemoteRef ref;
    ref.node_id = owner.value();
    // TODO: fill ref.address from the cached ShardMap and
    // ref.handle/remote_addr from a remote index probe before the transfer.
    if (Status s = transport_->ReadRemote(ref, dst.data, dst.capacity); !s.ok()) {
        return s;  // currently NotImplemented from the TCP stub — call path is
                   // real
    }
    // TODO: populate *out (size + metadata) from the transfer response framing.
    (void)out;
    return Status::NotImplemented("Connector::Get remote framing");
}

Status Connector::Put(const BlockKey& key, const Block& block) {
    Result<NodeId> owner = ResolveOwner(key);
    if (!owner.ok()) return owner.status();

    // LOCAL HIT: publish straight into the co-located node, no network.
    if (local_node_ != nullptr && owner.value() == self_node_) {
        return local_node_->Put(key, block);
    }

    // REMOTE: hand the bytes to the Transfer Engine (TCP now, RDMA later, same
    // ABC).
    RemoteRef ref;
    ref.node_id = owner.value();
    // TODO: fill ref.address/handle from the cached ShardMap; chunk large
    // blocks.
    return transport_->WriteRemote(ref, block.data.data(), block.size_bytes());
}

Status Connector::Prefetch(const std::vector<BlockKey>& keys) {
    // TODO: best-effort warm hint to owning nodes (promote SSD->DRAM).
    (void)keys;
    return Status::NotImplemented("Connector::Prefetch");
}

}  // namespace tidepool
