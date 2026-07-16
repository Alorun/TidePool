// eviction_policy.h — Pluggable cache replacement policy. Plane: DATA
// (node-internal). Decides, when a tier is full, which block to demote/drop.
//
// The policy only tracks *keys and sizes*; it never touches block bytes.
// Victim eviction is a two-phase transaction:
//   SelectVictim() reserves one resident key without permanently changing its
//   resident/ghost state; after the Tier migration succeeds the caller invokes
//   CommitVictim(), otherwise CancelVictim(). Only one reservation may be active
//   per policy instance.
//
// Pluggable seam: EvictionPolicy is a pure abstract base class.
#pragma once

#include <cstddef>

#include "tidepool/api/block_key.h"
#include "tidepool/api/status.h"

namespace tidepool {

class EvictionPolicy {
public:
    virtual ~EvictionPolicy() = default;

    // Record that `key` was just read/used (recency/frequency signal).
    virtual void OnAccess(const BlockKey& key) = 0;

    // Record that `key` (of `size_bytes`) was just inserted into the tier.
    virtual void OnInsert(const BlockKey& key, size_t size_bytes) = 0;

    // Record an explicit, non-victim removal. A successful two-phase victim
    // path is finalized by CommitVictim instead and must not also call this.
    virtual void OnRemove(const BlockKey& key) = 0;

    // Select and reserve the next resident victim. The selected key remains in
    // resident bookkeeping until CommitVictim; another selection while a
    // reservation is active returns kAlreadyExists. Empty policies return
    // kNotFound.
    virtual Result<BlockKey> SelectVictim() = 0;

    // Finalize a successful physical eviction/demotion. LRU removes the resident
    // entry; ARC moves it from T1/T2 to B1/B2. `key` must match the reservation.
    virtual Status CommitVictim(const BlockKey& key) = 0;

    // Abort a failed physical eviction/demotion. Clears the reservation and
    // leaves the selected key in its original resident state and position.
    virtual Status CancelVictim(const BlockKey& key) = 0;

    virtual const char* name() const = 0;
};

}  // namespace tidepool
