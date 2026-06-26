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
#include <vector>

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

int main() {
    TestBlockKeyPrefixReuse();
    TestStorageNodeDramRoundTrip();
    TestHashRingDeterministicRouting();
    TestCoordinatorVersioning();
    std::printf("tidepool smoke test: all checks passed\n");
    return 0;
}
