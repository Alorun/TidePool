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
#include <cstring>  // std::memcmp
#include <memory>
#include <string>
#include <vector>

#include "arc_eviction.h"  // private impl header (see tests/CMakeLists include dir)
#include "tidepool/api/block_codec.h"
#include "tidepool/coordinator/factory.h"
#include "tidepool/hashring/hash_ring.h"
#include "tidepool/store/factory.h"
#include "tidepool/store/storage_node.h"

#ifdef TIDEPOOL_WITH_LEVELDB
#include <unistd.h>  // getpid

#include <filesystem>

#include "ssd_tier.h"  // private impl header (see tests/CMakeLists include dir)
#endif

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
    CHECK(node.Open().ok(), "storage node open must succeed");

    auto key = BlockKey::FromTokenPrefix({1, 2, 3}, 3);
    Block blk;
    blk.data = {1, 2, 3, 4, 5};
    CHECK(node.Put(key, blk).ok(), "put must succeed");
    auto contains = node.Contains(key);
    CHECK(contains.ok() && contains.value(), "node must contain the key after put");

    // Zero-copy read contract: caller provides the destination buffer; Get
    // fills it and returns a view over the bytes (see buffer.h).
    std::vector<uint8_t> rbuf(64);
    MutableBuffer dst{rbuf.data(), rbuf.size()};
    BlockView view;
    CHECK(node.Get(key, dst, &view).ok(), "get must succeed");
    CHECK(view.size == 5, "view size must match block size");
    CHECK(view.data == rbuf.data(), "view must point into the caller buffer");
    CHECK(node.Close().ok(), "storage node close must succeed");
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
    auto v = arc.SelectVictim();
    CHECK(v.ok() && v.value() == B, "SelectVictim must reserve the T1 LRU (B)");
    CHECK(arc.ValidateVictimCommit(B).ok(), "committing B must be valid");
    arc.CommitVictim(B);
    CHECK(arc.b1_size() == 1 && arc.t1_size() == 1 && arc.t2_size() == 1, "victim moves to B1");

    // Recency ghost hit on B (in B1): p adapts up by max(|B2|/|B1|,1)=1, B -> T2.
    arc.OnAccess(B);
    CHECK(arc.p() == 1.0, "B1 ghost hit must raise p");
    CHECK(arc.b1_size() == 0 && arc.t2_size() == 2, "ghost hit brings key back to T2");

    // Push two more cold inserts and drain to force a T2 demotion into B2.
    arc.OnInsert(D, 0);  // T1=[D,C], T2=[A,B]
    auto v1 = arc.SelectVictim();
    CHECK(v1.ok() && v1.value() == C, "first victim is recency (T1 LRU = C)");
    CHECK(arc.ValidateVictimCommit(C).ok(), "committing C must be valid");
    arc.CommitVictim(C);
    auto v2 = arc.SelectVictim();  // now |T1|(=1) not > p(=1) -> demote from T2
    CHECK(v2.ok() && v2.value() == A, "second victim is frequency (T2 LRU = A)");
    CHECK(arc.ValidateVictimCommit(A).ok(), "committing A must be valid");
    arc.CommitVictim(A);
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

// block_codec: a non-trivial Block survives Serialize -> DeserializeHeader with
// its shape metadata equal field-by-field and its payload equal byte-for-byte.
// Pure byte codec — no LevelDB, so this always runs.
static void TestBlockCodecRoundTrip() {
    Block blk;
    blk.metadata.num_tokens = 128;
    blk.metadata.num_layers = 32;
    blk.metadata.dtype_size = 2;
    blk.metadata.kv_heads = 8;
    blk.metadata.created_unix_ns = 0x0123456789abcdefULL;
    blk.metadata.model_fingerprint = 0xdeadbeefU;
    for (int i = 0; i < 777; ++i) blk.data.push_back(static_cast<uint8_t>((i * 131 + 7) & 0xff));

    const std::string blob = SerializeBlock(blk);
    CHECK(blob.size() == block_codec::kHeaderSize + blk.data.size(), "blob = header + payload");

    BlockMetadata meta;
    size_t plen = 0, poff = 0;
    CHECK(DeserializeHeader(blob, &meta, &plen, &poff).ok(), "DeserializeHeader must succeed");
    CHECK(meta.num_tokens == 128, "num_tokens roundtrips");
    CHECK(meta.num_layers == 32, "num_layers roundtrips");
    CHECK(meta.dtype_size == 2, "dtype_size roundtrips");
    CHECK(meta.kv_heads == 8, "kv_heads roundtrips");
    CHECK(meta.created_unix_ns == 0x0123456789abcdefULL, "created_unix_ns roundtrips");
    CHECK(meta.model_fingerprint == 0xdeadbeefU, "model_fingerprint roundtrips");
    CHECK(plen == blk.data.size(), "payload_len matches");
    CHECK(poff == block_codec::kHeaderSize, "payload offset is header size");
    CHECK(std::memcmp(blob.data() + poff, blk.data.data(), plen) == 0, "payload bytes roundtrip");
}

// Little-endian stability: pin the exact header bytes so anyone tempted to
// switch to a struct memcpy (which would change the layout) breaks this test.
static void TestBlockCodecEndianness() {
    Block blk;
    blk.metadata.num_tokens = 0x04030201U;  // distinct bytes to see the ordering
    const std::string blob = SerializeBlock(blk);
    CHECK(static_cast<uint8_t>(blob[0]) == 'T' && static_cast<uint8_t>(blob[1]) == 'P', "magic 'T','P'");
    CHECK(static_cast<uint8_t>(blob[2]) == 0x01, "version byte == 1");
    CHECK(static_cast<uint8_t>(blob[3]) == 0x00, "serde_id byte == 0 (raw)");
    CHECK(static_cast<uint8_t>(blob[4]) == 0x01 && static_cast<uint8_t>(blob[5]) == 0x02 &&
              static_cast<uint8_t>(blob[6]) == 0x03 && static_cast<uint8_t>(blob[7]) == 0x04,
          "num_tokens stored little-endian");
}

// Robustness: malformed blobs return a specific error and never crash.
static void TestBlockCodecRobustness() {
    BlockMetadata meta;
    size_t plen = 0, poff = 0;

    const std::string tiny(10, 'x');  // shorter than the 36-byte header
    CHECK(DeserializeHeader(tiny, &meta, &plen, &poff).code() == StatusCode::kInvalidArgument,
          "truncated blob -> kInvalidArgument");

    Block blk;
    blk.data = {1, 2, 3};
    const std::string good = SerializeBlock(blk);

    std::string bad_magic = good;
    bad_magic[0] = 'X';
    CHECK(DeserializeHeader(bad_magic, &meta, &plen, &poff).code() == StatusCode::kInvalidArgument,
          "bad magic -> kInvalidArgument");

    std::string bad_version = good;
    bad_version[2] = 0x7f;
    CHECK(DeserializeHeader(bad_version, &meta, &plen, &poff).code() == StatusCode::kInvalidArgument,
          "unknown version -> kInvalidArgument");

    std::string bad_serde = good;
    bad_serde[3] = 0x07;  // unknown serde_id
    CHECK(DeserializeHeader(bad_serde, &meta, &plen, &poff).code() == StatusCode::kNotImplemented,
          "unknown serde_id -> kNotImplemented");

    std::string lying_len = good;
    for (int i = 0; i < 8; ++i) lying_len[28 + i] = static_cast<char>(0xff);  // payload_len = huge
    CHECK(DeserializeHeader(lying_len, &meta, &plen, &poff).code() == StatusCode::kInvalidArgument,
          "payload_len past end -> kInvalidArgument");

    CHECK(DeserializeHeader(good + "x", &meta, &plen, &poff).code() ==
              StatusCode::kInvalidArgument,
          "trailing bytes beyond payload_len -> kInvalidArgument");
}

#ifdef TIDEPOOL_WITH_LEVELDB
// SsdTier end-to-end on a throwaway LevelDB directory: Put -> Get (fits) ->
// Get (too small, probe) -> Evict -> Get (gone). Only built with LevelDB.
static void TestSsdTierRoundTrip() {
    namespace fs = std::filesystem;
    const fs::path dir = fs::temp_directory_path() / ("tidepool_ssd_test_" + std::to_string(::getpid()));
    fs::remove_all(dir);  // start clean

    const auto key = BlockKey::FromTokenPrefix({1, 2, 3}, 3, 0xabcU);
    Block blk;
    blk.metadata.num_tokens = 3;
    blk.metadata.model_fingerprint = 0xabcU;
    blk.data = {9, 8, 7, 6, 5};
    const uint64_t expected_bytes = block_codec::kHeaderSize + blk.data.size();

    {
        SsdTier tier(dir.string());
        CHECK(tier.Open().ok(), "SsdTier::Open must succeed");
        uint64_t handle = 0;
        CHECK(tier.Put(key, blk, &handle).ok(), "SsdTier::Put must succeed");
        CHECK(tier.Stats().num_blocks == 1 && tier.Stats().used_bytes == expected_bytes,
              "Put updates SSD occupancy stats");
    }

    // Reopen the persistent database. Open must reconstruct occupancy stats,
    // and the self-describing value must remain readable without LocalIndex.
    {
        SsdTier tier(dir.string());
        CHECK(tier.Open().ok(), "SsdTier reopen must succeed");
        CHECK(tier.Stats().num_blocks == 1 && tier.Stats().used_bytes == expected_bytes,
              "Open rebuilds SSD occupancy stats");

        std::vector<uint8_t> buf(64);
        MutableBuffer dst{buf.data(), buf.size()};
        BlockView view;
        CHECK(tier.Get(key, dst, &view).ok(), "SsdTier::Get (fits) must succeed");
        CHECK(view.size == 5, "view size matches payload");
        CHECK(view.data == buf.data(), "view points into the caller buffer");
        CHECK(std::memcmp(view.data, blk.data.data(), 5) == 0, "payload roundtrips through SSD");
        CHECK(view.metadata.num_tokens == 3 && view.metadata.model_fingerprint == 0xabcU,
              "metadata roundtrips");

        std::vector<uint8_t> small(2);
        MutableBuffer sdst{small.data(), small.size()};
        BlockView sview;
        const Status too_small = tier.Get(key, sdst, &sview);
        CHECK(too_small.code() == StatusCode::kOutOfCapacity, "undersized dst -> kOutOfCapacity");
        CHECK(sview.data == nullptr && sview.size == 0, "failed Get clears the output view");
        auto info = tier.Probe(key);
        CHECK(info.ok() && info.value().payload_size == 5, "Probe reports the required payload size");

        CHECK(tier.Evict(key).ok(), "SsdTier::Evict must succeed");
        CHECK(tier.Stats().num_blocks == 0 && tier.Stats().used_bytes == 0,
              "Evict updates SSD occupancy stats");
        BlockView gone;
        CHECK(tier.Get(key, dst, &gone).code() == StatusCode::kNotFound, "Get after Evict -> kNotFound");
    }

    fs::remove_all(dir);  // cleanup after the LevelDB handle is closed
}
#endif

int main() {
    TestBlockKeyPrefixReuse();
    TestStorageNodeDramRoundTrip();
    TestArcEvictionPolicy();
    TestHashRingDeterministicRouting();
    TestCoordinatorVersioning();
    TestBlockCodecRoundTrip();
    TestBlockCodecEndianness();
    TestBlockCodecRobustness();
#ifdef TIDEPOOL_WITH_LEVELDB
    TestSsdTierRoundTrip();
#endif
    std::printf("tidepool smoke test: all checks passed\n");
    return 0;
}
