// arc_eviction.h — Adaptive Replacement Cache (ARC) policy. Plane: DATA
// (node-internal). Second concrete implementation of the EvictionPolicy ABC,
// living alongside LRU (which stays as the benchmark baseline).
//
// ARC (Megiddo & Modha, FAST'03) self-tunes between recency and frequency. It
// manages exactly ONE cache layer here: the DRAM tier of a StorageNode. SSD is
// NOT part of ARC — it is only where DRAM victims happen to sink. The ghost
// lists B1/B2 below are ARC's own key-only bookkeeping and are unrelated to
// what physically still sits on SSD: a key that has aged out of B1/B2 is, to
// ARC, a cold/unknown miss even if its bytes are still on SSD.
//
// Four lists (each: front = MRU, back = LRU):
//   T1 — resident in DRAM, seen exactly once (recency).
//   T2 — resident in DRAM, seen twice or more (frequency).
//   B1 — ghost (key only) recently evicted from T1.
//   B2 — ghost (key only) recently evicted from T2.
// Invariant target: |T1| + |T2| <= c; the adaptive parameter p in [0, c] is the
// target size of T1.
//
// CAPACITY UNIT (explicit simplification): classic ARC counts fixed-size pages.
// Blocks here vary in size, so this policy counts in *block count*: the cache
// capacity c is the DRAM block-count budget passed to the constructor. The
// `size_bytes` argument of OnInsert is therefore ignored (kept only for ABC
// compatibility, exactly as LruEviction ignores it today).
#pragma once

#include <cstddef>
#include <list>
#include <optional>
#include <unordered_map>

#include "tidepool/store/eviction_policy.h"

namespace tidepool {

class ArcEviction : public EvictionPolicy {
public:
    // `capacity_blocks` = c, the DRAM cache size measured in blocks (>= 1).
    explicit ArcEviction(size_t capacity_blocks);

    // Called after a successful DRAM or SSD read (see StorageNode::Get).
    // Handles the cases where ARC already knows the key:
    //   Case I   (key in T1/T2): move it to the MRU of T2.
    //   Case II  (key in B1, recency ghost): adapt p up, move key B1 -> T2 MRU.
    //   Case III (key in B2, frequency ghost): adapt p down, move key B2 -> T2.
    // If the key is unknown (in none of the four lists) this is a no-op: the
    // cold insert is left to OnInsert (Case IV).
    void OnAccess(const BlockKey& key) override;

    // Cold insert of a block newly entering DRAM (recomputed+Put, or an SSD hit
    // whose key is no longer in B1/B2). Case IV: place the key at the MRU of T1.
    // Idempotent for already-tracked keys: a key that OnAccess just promoted out
    // of a ghost list is left in T2 (so the StorageNode may always call OnInsert
    // after a DRAM backfill without needing to know B1/B2 membership).
    void OnInsert(const BlockKey& key, size_t size_bytes) override;

    // Forget all bookkeeping for an explicit, non-victim removal of `key`.
    // Successful victim migration uses CommitVictim instead.
    void OnRemove(const BlockKey& key) override;

    // ARC's REPLACE subroutine split into selection and commit. Selection only
    // reserves the chosen T1/T2 LRU; commit performs T1->B1 or T2->B2.
    Result<BlockKey> SelectVictim() override;
    Status CommitVictim(const BlockKey& key) override;
    Status CancelVictim(const BlockKey& key) override;

    const char* name() const override { return "arc"; }

    // Test/inspection hooks (not part of the ABC).
    size_t c() const { return c_; }
    double p() const { return p_; }
    size_t t1_size() const { return t1_.size(); }
    size_t t2_size() const { return t2_.size(); }
    size_t b1_size() const { return b1_.size(); }
    size_t b2_size() const { return b2_.size(); }
    bool has_reservation() const { return reserved_.has_value(); }

private:
    enum class List { kNone, kT1, kT2, kB1, kB2 };
    struct Entry {
        List list = List::kNone;
        std::list<BlockKey>::iterator it;
    };
    struct Reservation {
        BlockKey key;
        List source = List::kNone;
    };

    std::list<BlockKey>& ListRef(List l);
    // Splice `key`'s node from its current list to `dst` (front=MRU side).
    void Relink(const BlockKey& key, List dst, bool front = true);
    void PushFront(const BlockKey& key, List dst);
    void EraseLru(List l);  // drop the LRU (back) key of a ghost list

    // ARC Case IV ghost-list bound maintenance run before a cold insert.
    void TrimGhostsForInsert();

    // p += max(|B2|/|B1|, 1), clamped to c (recency ghost hit, Case II).
    void AdaptUp();
    // p -= max(|B1|/|B2|, 1), clamped to 0 (frequency ghost hit, Case III).
    void AdaptDown();

    size_t c_;        // cache capacity in blocks
    double p_ = 0.0;  // adaptive target size of T1, in [0, c]
    // Whether the most recent recognized access was a B2 (frequency) ghost hit;
    // consumed by CommitVictim()/REPLACE for its tie-break.
    bool last_hit_in_b2_ = false;

    std::list<BlockKey> t1_, t2_, b1_, b2_;  // front = MRU, back = LRU
    std::unordered_map<BlockKey, Entry> pos_;
    std::optional<Reservation> reserved_;
};

}  // namespace tidepool
