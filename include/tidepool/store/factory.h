// factory.h — Construction helpers for the concrete store components.
// Plane: DATA (node-internal). Lets apps/tests assemble a StorageNode without
// depending on the private impl headers under src/.
#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "tidepool/store/eviction_policy.h"
#include "tidepool/store/tier.h"

namespace tidepool {

// DRAM tier with the given byte capacity (capacity is advisory in the MVP).
std::unique_ptr<Tier> MakeDramTier(uint64_t capacity_bytes);

// SSD tier backed by LevelDB at `db_path`. Returns a tier whose Open/Put/Get
// methods report NotImplemented unless built with -DTIDEPOOL_WITH_LEVELDB=ON.
// StorageNode::Open initializes it through the common Tier lifecycle.
std::unique_ptr<Tier> MakeSsdTier(std::string db_path);

std::unique_ptr<EvictionPolicy> MakeLruEviction();
std::unique_ptr<EvictionPolicy> MakeCostAwareEviction();  // stub policy

// ARC (Adaptive Replacement Cache) policy over the DRAM tier. `capacity_blocks`
// is the DRAM cache size measured in block count (ARC's classic page count). It
// coexists with LRU as a selectable alternative; LRU stays the baseline.
std::unique_ptr<EvictionPolicy> MakeArcEviction(size_t capacity_blocks);

}  // namespace tidepool
