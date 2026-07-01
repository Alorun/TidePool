// smoke_test.cpp — Minimal placeholder test. Exercises the parts that have real
// (non-stub) behavior so `ctest` has something meaningful to run.
//
// Uses a CHECK macro rather than assert() on purpose: the default build type is
// RelWithDebInfo, which defines NDEBUG and compiles assert() out — that would
// make this test silently vacuous (and leave the checked variables "unused").
// CHECK always runs and aborts on failure regardless of build type.
//
// TODO: replace with a proper test framework (GoogleTest via FetchContent) and
// add per-module unit tests.
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "arc_eviction.h"  // private impl header (see tests/CMakeLists include dir)
#include "tidepool/coordinator/factory.h"
#include "tidepool/hashring/hash_ring.h"
#include "tidepool/store/factory.h"
#include "tidepool/store/storage_node.h"

using namespace tidepool;

#define CHECK(cond, msg)                                                                   \
    do {                                                                                   \
        if (!(cond)) {                                                                     \
            std::fprintf(stderr, "CHECK failed: %s (%s:%d)\n", (msg), __FILE__, __LINE__); \
            std::abort();                                                                  \
        }                                                                                  \
    } while (0)

static void TestBlockKeyPrefixReuse() {
    // Same prefix => same key (enables cross-instance prefix reuse).
    auto a = BlockKey::FromTokenPrefix({10, 11, 12, 13, 14}, 3);
    auto b = BlockKey::FromTokenPrefix({10, 11, 12, 99, 99}, 3);
    CHECK(a == b, "equal token prefixes must produce equal BlockKeys");

    // Distinct prefixes => distinct keys. These guard against a degenerate hash
    // that ignores its inputs (which would make every block collide and
    // silently serve the wrong KV). Cover three independent ways a prefix can
    // differ:
    auto c = BlockKey::FromTokenPrefix({10, 11, 99}, 3);  // same len, diff content
    CHECK(a != c, "different prefix content must differ");
    auto d = BlockKey::FromTokenPrefix({20, 21, 22}, 3);  // fully different tokens
    CHECK(a != d, "fully different prefixes must differ");
    auto e = BlockKey::FromTokenPrefix({10, 11, 12, 13, 14}, 5);  // diff length
    CHECK(a != e, "different prefix length must differ");
}

static void TestStorageNodeDramRoundTrip() {
    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(MakeDramTier(1 << 20));
    StorageNode node("node-test", std::move(tiers), MakeLruEviction());

    auto key = BlockKey::FromTokenPrefix({1, 2, 3}, 3);
    Block blk;
    blk.data = {1, 2, 3, 4, 5};
    CHECK(node.Put(key, blk).ok(), "put must succeed");
    CHECK(node.Contains(key), "node must contain the key after put");

    // Zero-copy read contract: caller provides the destination buffer; Get
    // fills it and returns a view over the bytes (see buffer.h).
    std::vector<uint8_t> rbuf(64);
    MutableBuffer dst{rbuf.data(), rbuf.size()};
    BlockView view;
    CHECK(node.Get(key, dst, &view).ok(), "get must succeed");
    CHECK(view.size == 5, "view size must match block size");
    CHECK(view.data == rbuf.data(), "view must point into the caller buffer");
}

static void TestHashRingDeterministicRouting() {
    ShardMap map;
    map.version = 1;
    map.nodes = {{"n0", "127.0.0.1:1", 16, true}, {"n1", "127.0.0.1:2", 16, true}};
    HashRing ring;
    ring.Rebuild(map);
    auto key = BlockKey::FromTokenPrefix({7, 8, 9}, 3);
    auto owner1 = ring.Owner(key);
    auto owner2 = ring.Owner(key);
    CHECK(owner1.ok() && owner2.ok(), "owner lookup must succeed on a built ring");
    CHECK(owner1.value() == owner2.value(), "routing must be deterministic for a fixed ring");
}

static void TestCoordinatorVersioning() {
    auto coord = MakeSingleNodeCoordinator();
    auto v0 = coord->GetShardMap();
    CHECK(v0.ok(), "initial GetShardMap must succeed");
    NodeInfo n;
    n.id = "n0";
    n.address = "127.0.0.1:7001";
    n.vnode_count = 8;
    CHECK(coord->RegisterNode(n).ok(), "RegisterNode must succeed");
    auto v1 = coord->GetShardMap();
    CHECK(v1.ok(), "GetShardMap after register must succeed");
    CHECK(v1.value().version > v0.value().version, "registration must bump the shard map version");
}

// Drives the ARC policy directly through one full cycle: cold insert -> T1,
// hit -> T2, REPLACE (Victim) -> ghost, recency ghost hit -> p up, frequency
// ghost hit -> p down. Asserts the internal lists + adaptive parameter so a
// regression in the algorithm (not just the wiring) is caught.
static void TestArcEvictionPolicy() {
    auto k = [](TokenId t) { return BlockKey::FromTokenPrefix({t}, 1); };
    const auto A = k(1), B = k(2), C = k(3), D = k(4);

    ArcEviction arc(/*capacity_blocks=*/2);
    CHECK(std::string(arc.name()) == "arc", "policy name must be arc");
    CHECK(arc.c() == 2, "capacity must be configured in blocks");

    // Cold inserts land in T1 (seen once).
    arc.OnInsert(A, 0);
    arc.OnInsert(B, 0);
    CHECK(arc.t1_size() == 2 && arc.t2_size() == 0, "cold inserts go to T1");

    // A real hit promotes to T2 (seen twice).
    arc.OnAccess(A);
    CHECK(arc.t1_size() == 1 && arc.t2_size() == 1, "hit promotes T1 -> T2");

    // Cold-insert C, then REPLACE: T1.LRU (B) is the recency victim and lands on
    // the B1 ghost list.
    arc.OnInsert(C, 0);
    auto v = arc.Victim();
    CHECK(v.has_value() && *v == B, "Victim must demote the T1 LRU (B)");
    CHECK(arc.b1_size() == 1 && arc.t1_size() == 1 && arc.t2_size() == 1, "victim moves to B1");

    // Recency ghost hit on B (in B1): p adapts up by max(|B2|/|B1|,1)=1, B -> T2.
    arc.OnAccess(B);
    CHECK(arc.p() == 1.0, "B1 ghost hit must raise p");
    CHECK(arc.b1_size() == 0 && arc.t2_size() == 2, "ghost hit brings key back to T2");

    // Push two more cold inserts and drain to force a T2 demotion into B2.
    arc.OnInsert(D, 0);  // T1=[D,C], T2=[A,B]
    auto v1 = arc.Victim();
    CHECK(v1.has_value() && *v1 == C, "first victim is recency (T1 LRU = C)");
    auto v2 = arc.Victim();  // now |T1|(=1) not > p(=1) -> demote from T2
    CHECK(v2.has_value() && *v2 == A, "second victim is frequency (T2 LRU = A)");
    CHECK(arc.b2_size() == 1, "T2 victim moves to B2");

    // Frequency ghost hit on A (in B2): p adapts down by max(|B1|/|B2|,1)=1.
    const double p_before = arc.p();
    arc.OnAccess(A);
    CHECK(arc.p() == p_before - 1.0, "B2 ghost hit must lower p");
    CHECK(arc.b2_size() == 0 && arc.t2_size() == 2, "frequency ghost hit returns key to T2");

    // OnRemove forgets all bookkeeping for a key.
    arc.OnRemove(A);
    CHECK(arc.t2_size() == 1, "OnRemove drops the key from its list");
}

int main() {
    TestBlockKeyPrefixReuse();
    TestStorageNodeDramRoundTrip();
    TestArcEvictionPolicy();
    TestHashRingDeterministicRouting();
    TestCoordinatorVersioning();
    std::printf("tidepool smoke test: all checks passed\n");
    return 0;
}
