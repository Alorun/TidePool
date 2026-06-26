// buffer.h — Zero-copy-friendly buffer types for the read path. Plane: SHARED.
//
// WHY this file exists (design-review revision): the public read contract used
// to be `Result<Block> Get(...)`, which returns an owning vector by value and
// thus bakes a copy into the hottest path. tidepool's whole value proposition
// is transfer efficiency (RDMA / zero-copy later), so the read API must not
// force a copy. These two types let a reader hand in destination memory it owns
// and get back a view describing what landed there — no forced allocation, and
// a remote read can DMA straight into the caller's (registered) buffer.
#pragma once

#include <cstddef>
#include <cstdint>

#include "tidepool/api/block.h"  

namespace tidepool {

// Caller-owned, writable destination for a read. The caller allocates and owns
// the memory and must keep it valid for the duration of the call (and for as
// long as it uses any BlockView returned over it). Tiers / Transport fill into
// it WITHOUT allocating. For RDMA this is the registered target region.
struct MutableBuffer {
    uint8_t* data = nullptr;
    size_t capacity = 0;  // bytes available at `data`
};

// Read-only result of a successful Get: where the bytes landed, how many, and
// the block metadata. Ownership note: the bytes live in the caller's
// MutableBuffer (copy-into / RDMA-into path). The view does NOT own anything
// and is valid only as long as that backing buffer is — never store it past the
// block's lifetime. A future local zero-copy overload may instead return a view
// directly over tier-internal storage under a read-lock/refcount (see
// Tier::Get TODO).
struct BlockView {
    const uint8_t* data = nullptr;
    size_t size = 0;  // valid bytes at `data`; always <= the MutableBuffer.capacity
    BlockMetadata metadata;
};

}  // namespace tidepool
