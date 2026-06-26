// types.h — Convenience umbrella header pulling in all shared API types.
// Plane: SHARED. Include this from call sites that want the whole vocabulary.
#pragma once

#include <vector>

#include "tidepool/api/block.h"
#include "tidepool/api/block_key.h"
#include "tidepool/api/buffer.h"
#include "tidepool/api/location.h"
#include "tidepool/api/shard_map.h"
#include "tidepool/api/status.h"

namespace tidepool {

// Result of a batched Lookup(): hit_map[i] == true iff keys[i] is present
// somewhere in the pool. Lets the engine batch-probe before deciding what to
// recompute.
using HitMap = std::vector<bool>;

}  // namespace tidepool
