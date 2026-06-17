// types.h — Convenience umbrella header pulling in all shared API types.
// Plane: SHARED. Include this from call sites that want the whole vocabulary.
#pragma once

#include <vector>

#include "block.h"
#include "block_key.h"
#include "buffer.h"
#include "location.h"
#include "shard_map.h"
#include "status.h"

namespace tidepool {

// Result of a batched Lookup(): hit_map[i] == true iff keys[i] is present
// somewhere in the pool. Lets the engine batch-probe before deciding what to
// recompute.
using HitMap = std::vector<bool>;

}  // namespace tidepool
