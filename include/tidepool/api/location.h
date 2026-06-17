// location.h — Where a block physically lives. Plane: SHARED.
//
// A Location is meaningful relative to a node: it names the node, the storage
// tier within that node, and a tier-local handle (offset / pointer token / db
// key id). The intra-node tiering (DRAM -> SSD) is the *vertical* dimension and
// is entirely a node-internal concern; the cross-node sharing is the
// *horizontal* dimension handled by the hash ring and coordinator.
#pragma once

#include <cstdint>
#include <string>

namespace tidepool {

using NodeId = std::string;

// Storage tiers, ordered hot -> cold. kGpu is reserved (interface only) — this
// scaffold implements DRAM + SSD; see ROADMAP in README.
enum class TierType : uint8_t {
    kGpu = 0,   // future: HBM/GPU tier (stub only)
    kDram = 1,  // in-process DRAM
    kSsd = 2,   // local SSD via LevelDB/RocksDB
};

const char* TierTypeName(TierType t);

struct Location {
    NodeId node_id;
    TierType tier = TierType::kDram;

    // HANDLE CONTRACT (defined here so every tier obeys the same rule):
    //   * `handle` is a STABLE, OPAQUE id, NOT a dereferenceable pointer. It
    //   names
    //     a block within a tier; you re-look-up the block by (key|handle) on
    //     the owning tier rather than dereferencing it. This is deliberate: a
    //     Location may be cached/transferred and outlive any in-memory layout,
    //     so it must survive a DRAM rehash, an SSD compaction, or the block
    //     moving tiers.
    //   * The owning TIER owns the underlying buffer; a Location never does.
    //   * `handle` stays valid only while the block is present in `tier`. After
    //     eviction/demotion the id is stale; a Get with it (or its key) returns
    //     kNotFound — it must never resolve to freed or reused memory.
    //   * Crossing tiers (DRAM->SSD) yields a NEW Location (new tier + handle);
    //     the index is updated. Holders of the old Location simply miss and
    //     refetch.
    // Interpretation of the bits is tier-private (e.g. a record id for SSD),
    // but the stability/lifetime rules above are mandatory.
    uint64_t handle = 0;
};

}  // namespace tidepool
