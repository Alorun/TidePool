#include <cstdio>
#include <cstdlib>

#include "arc_eviction.h"
#include "lru_eviction.h"

using namespace tidepool;

#define CHECK(cond, msg)                                                                   \
    do {                                                                                   \
        if (!(cond)) {                                                                     \
            std::fprintf(stderr, "CHECK failed: %s (%s:%d)\n", (msg), __FILE__, __LINE__); \
            std::abort();                                                                  \
        }                                                                                  \
    } while (0)

namespace {

BlockKey Key(int value) { return BlockKey::FromTokenPrefix({value}, 1); }

void TestLruReservationLifecycle() {
    const BlockKey a = Key(1), b = Key(2), c = Key(3);
    LruEviction lru;
    CHECK(lru.OnInsert(a, 1).ok(), "insert A succeeds");
    CHECK(lru.OnInsert(b, 1).ok(), "insert B succeeds");
    CHECK(lru.OnInsert(c, 1).ok(), "insert C succeeds");
    lru.OnAccess(a);

    auto selected = lru.SelectVictim();
    CHECK(selected.ok() && selected.value() == b, "LRU selects the oldest resident key");
    CHECK(lru.resident_size() == 3 && lru.has_reservation(), "selection keeps victim resident and reserves it");
    CHECK(lru.SelectVictim().status().code() == StatusCode::kAlreadyExists,
          "LRU does not select another victim while reserved");
    CHECK(lru.ValidateVictimCommit(a).code() == StatusCode::kInvalidArgument,
          "wrong-key commit preflight is rejected");
    CHECK(lru.CancelVictim(a).code() == StatusCode::kInvalidArgument, "wrong-key cancel is rejected");
    CHECK(lru.CancelVictim(b).ok(), "LRU reservation can be cancelled");
    CHECK(lru.resident_size() == 3 && !lru.has_reservation(), "cancel preserves resident state");

    selected = lru.SelectVictim();
    CHECK(selected.ok() && selected.value() == b, "cancelled victim remains in its original LRU position");
    CHECK(lru.ValidateVictimCommit(b).ok(), "LRU victim commit preflight succeeds");
    lru.CommitVictim(b);
    CHECK(lru.resident_size() == 2 && !lru.has_reservation(), "commit removes the resident entry");

    selected = lru.SelectVictim();
    CHECK(selected.ok() && selected.value() == c, "next selection does not repeat committed victim");
    CHECK(lru.ValidateVictimCommit(c).ok(), "second commit preflight succeeds");
    lru.CommitVictim(c);
    selected = lru.SelectVictim();
    CHECK(selected.ok() && selected.value() == a, "OnAccess made A the final victim");
    CHECK(lru.ValidateVictimCommit(a).ok(), "final commit preflight succeeds");
    lru.CommitVictim(a);
    CHECK(lru.SelectVictim().status().code() == StatusCode::kNotFound, "empty LRU returns NotFound");
}

void TestArcReservationAndGhostTransitions() {
    const BlockKey a = Key(11), b = Key(12);
    ArcEviction arc(/*capacity_blocks=*/2);
    CHECK(arc.OnInsert(a, 1).ok(), "ARC insert A succeeds");
    CHECK(arc.OnInsert(b, 1).ok(), "ARC insert B succeeds");

    auto selected = arc.SelectVictim();
    CHECK(selected.ok() && selected.value() == a, "ARC selects T1 LRU");
    CHECK(arc.t1_size() == 2 && arc.b1_size() == 0 && arc.b2_size() == 0,
          "ARC selection does not change resident or ghost lists");
    CHECK(arc.SelectVictim().status().code() == StatusCode::kAlreadyExists,
          "ARC does not select another key while reserved");
    CHECK(arc.CancelVictim(a).ok(), "ARC reservation can be cancelled");
    CHECK(arc.t1_size() == 2 && arc.b1_size() == 0 && !arc.has_reservation(),
          "ARC cancel preserves the original resident state");

    selected = arc.SelectVictim();
    CHECK(selected.ok() && selected.value() == a, "cancelled ARC victim remains selectable");
    CHECK(arc.ValidateVictimCommit(a).ok(), "ARC T1 commit preflight succeeds");
    arc.CommitVictim(a);
    CHECK(arc.t1_size() == 1 && arc.b1_size() == 1, "T1 commit moves the key into B1");

    CHECK(arc.OnInsert(a, 1).ok(), "ARC B1 promotion succeeds");
    CHECK(arc.p() == 1.0 && arc.b1_size() == 0 && arc.t2_size() == 1,
          "B1 ghost insert preserves ARC p-up behavior");

    selected = arc.SelectVictim();
    CHECK(selected.ok() && selected.value() == a, "ARC selects T2 victim at the p tie");
    const double p_before_cancel = arc.p();
    const size_t t1_before_cancel = arc.t1_size();
    const size_t t2_before_cancel = arc.t2_size();
    CHECK(arc.CancelVictim(a).ok(), "ARC simulated I/O failure cancels reservation");
    CHECK(arc.p() == p_before_cancel && arc.t1_size() == t1_before_cancel &&
              arc.t2_size() == t2_before_cancel && arc.b2_size() == 0,
          "ARC cancel leaves p and all lists unchanged");

    selected = arc.SelectVictim();
    CHECK(selected.ok() && selected.value() == a, "ARC T2 victim remains selectable after cancel");
    CHECK(arc.ValidateVictimCommit(a).ok(), "ARC T2 commit preflight succeeds");
    arc.CommitVictim(a);
    CHECK(arc.t2_size() == 0 && arc.b2_size() == 1, "T2 commit moves the key into B2");

    CHECK(arc.OnInsert(a, 1).ok(), "ARC B2 promotion succeeds");
    CHECK(arc.p() == 0.0 && arc.b2_size() == 0 && arc.t2_size() == 1,
          "B2 ghost insert preserves ARC p-down behavior");
    CHECK(arc.ValidateVictimCommit(b).code() == StatusCode::kInvalidArgument,
          "ARC rejects commit without reservation");
    CHECK(arc.CancelVictim(b).code() == StatusCode::kInvalidArgument, "ARC rejects cancel without reservation");
}

}  // namespace

int main() {
    TestLruReservationLifecycle();
    TestArcReservationAndGhostTransitions();
    std::printf("tidepool eviction test: all checks passed\n");
    return 0;
}
