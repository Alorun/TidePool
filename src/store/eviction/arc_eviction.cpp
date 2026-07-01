#include "arc_eviction.h"

#include <algorithm>  // std::max, std::min

#include "tidepool/store/factory.h"

namespace tidepool {

std::unique_ptr<EvictionPolicy> MakeArcEviction(size_t capacity_blocks) {
    return std::make_unique<ArcEviction>(capacity_blocks);
}

ArcEviction::ArcEviction(size_t capacity_blocks) : c_(capacity_blocks == 0 ? 1 : capacity_blocks) {}

std::list<BlockKey>& ArcEviction::ListRef(List l) {
    switch (l) {
        case List::kT1: return t1_;
        case List::kT2: return t2_;
        case List::kB1: return b1_;
        case List::kB2: return b2_;
        case List::kNone: break;
    }
    return t1_;  // unreachable; keeps the compiler happy
}

void ArcEviction::Relink(const BlockKey& key, List dst, bool front) {
    auto& e = pos_[key];
    std::list<BlockKey>& src = ListRef(e.list);
    std::list<BlockKey>& d = ListRef(dst);
    // splice preserves e.it: the node simply moves to `d`.
    if (front) {
        d.splice(d.begin(), src, e.it);
    } else {
        d.splice(d.end(), src, e.it);
    }
    e.list = dst;
}

void ArcEviction::PushFront(const BlockKey& key, List dst) {
    std::list<BlockKey>& d = ListRef(dst);
    d.push_front(key);
    pos_[key] = Entry{dst, d.begin()};
}

void ArcEviction::EraseLru(List l) {
    std::list<BlockKey>& d = ListRef(l);
    if (d.empty()) return;
    pos_.erase(d.back());
    d.pop_back();
}

void ArcEviction::AdaptUp() {
    // Case II: ratio max(|B2|/|B1|, 1). |B1| >= 1 here (key is in B1).
    const double ratio = b1_.empty() ? 1.0 : std::max(static_cast<double>(b2_.size()) / static_cast<double>(b1_.size()), 1.0);
    p_ = std::min(static_cast<double>(c_), p_ + ratio);
}

void ArcEviction::AdaptDown() {
    // Case III: ratio max(|B1|/|B2|, 1). |B2| >= 1 here (key is in B2).
    const double ratio = b2_.empty() ? 1.0 : std::max(static_cast<double>(b1_.size()) / static_cast<double>(b2_.size()), 1.0);
    p_ = std::max(0.0, p_ - ratio);
}

void ArcEviction::OnAccess(const BlockKey& key) {
    auto it = pos_.find(key);
    if (it == pos_.end()) {
        // Unknown key: ARC has never seen it (or it aged out of the ghosts).
        // Defer to the cold OnInsert (Case IV); clear the REPLACE context so a
        // later Victim() does not mis-apply the frequency-ghost tie-break.
        last_hit_in_b2_ = false;
        return;
    }
    switch (it->second.list) {
        case List::kT1:
        case List::kT2:
            // Case I: cache hit -> second-or-later reference, promote to T2 MRU.
            Relink(key, List::kT2);
            break;
        case List::kB1:
            // Case II: recency ghost hit. Adapt p (uses sizes before the move),
            // then bring the key back as a frequency block (B1 -> T2 MRU). The
            // data must be refetched into DRAM (StorageNode's job).
            AdaptUp();
            last_hit_in_b2_ = false;
            Relink(key, List::kT2);
            break;
        case List::kB2:
            // Case III: frequency ghost hit.
            AdaptDown();
            last_hit_in_b2_ = true;
            Relink(key, List::kT2);
            break;
        case List::kNone:
            break;
    }
}

void ArcEviction::TrimGhostsForInsert() {
    // ARC Case IV ghost-list bound maintenance. The physical eviction of a
    // resident DRAM block (REPLACE) is decoupled here into Victim(); this method
    // only trims the key-only ghost lists so they stay bounded (|L1| <= c,
    // total <= 2c).
    const size_t l1 = t1_.size() + b1_.size();
    if (l1 == c_) {
        if (t1_.size() < c_) {
            EraseLru(List::kB1);
        }
        // else |T1| == c (B1 empty): cache is all T1; the resident eviction is
        // Victim()'s responsibility, nothing to trim here.
    } else if (l1 < c_) {
        const size_t total = t1_.size() + t2_.size() + b1_.size() + b2_.size();
        if (total >= 2 * c_) {
            EraseLru(List::kB2);
        }
    }
}

void ArcEviction::OnInsert(const BlockKey& key, size_t /*size_bytes*/) {
    // size_bytes ignored: capacity is counted in blocks (see header).
    auto it = pos_.find(key);
    if (it != pos_.end()) {
        switch (it->second.list) {
            case List::kT1:
            case List::kT2:
                // Already resident (overwrite, or a ghost OnAccess just
                // promoted). Treat as a use; keep it in T2. No cold insert.
                Relink(key, List::kT2);
                return;
            case List::kB1:
                // A cold-insert path landed on a recency ghost (the access went
                // through OnInsert rather than OnAccess). Promote like Case II.
                AdaptUp();
                last_hit_in_b2_ = false;
                Relink(key, List::kT2);
                return;
            case List::kB2:
                AdaptDown();
                last_hit_in_b2_ = true;
                Relink(key, List::kT2);
                return;
            case List::kNone:
                break;
        }
    }
    // Case IV: brand-new key -> trim ghosts, then place at the MRU of T1.
    TrimGhostsForInsert();
    last_hit_in_b2_ = false;
    PushFront(key, List::kT1);
}

void ArcEviction::OnRemove(const BlockKey& key) {
    auto it = pos_.find(key);
    if (it == pos_.end()) return;
    ListRef(it->second.list).erase(it->second.it);
    pos_.erase(it);
}

std::optional<BlockKey> ArcEviction::Victim() {
    // ARC REPLACE: choose which resident DRAM block to demote.
    if (t1_.empty() && t2_.empty()) return std::nullopt;

    bool use_t1;
    if (t1_.empty()) {
        use_t1 = false;
    } else if (t2_.empty()) {
        use_t1 = true;
    } else if (last_hit_in_b2_) {
        // x in B2: evict from T1 when |T1| >= p (covers the |T1| == p tie).
        use_t1 = static_cast<double>(t1_.size()) >= p_;
    } else {
        use_t1 = static_cast<double>(t1_.size()) > p_;
    }

    BlockKey victim;
    if (use_t1) {
        victim = t1_.back();
        Relink(victim, List::kB1);  // T1.LRU -> B1.MRU (the sunk-to-SSD victim)
    } else {
        victim = t2_.back();
        Relink(victim, List::kB2);  // T2.LRU -> B2.MRU
    }
    // The REPLACE context is consumed; further victims in the same eviction
    // sweep use the plain |T1| > p rule.
    last_hit_in_b2_ = false;
    return victim;
}

}  // namespace tidepool
