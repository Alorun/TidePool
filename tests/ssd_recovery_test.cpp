#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <leveldb/db.h>

#include "ssd_tier.h"
#include "tidepool/api/block_codec.h"
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

namespace {

std::filesystem::path Path(const char* suffix) {
    return std::filesystem::temp_directory_path() /
           ("tidepool_ssd_recovery_" + std::to_string(::getpid()) + "_" + suffix);
}

BlockKey Key(int value, uint32_t model = 0x77U) {
    return BlockKey::FromTokenPrefix({value}, 1, model);
}

Block SizedBlock(size_t size, uint8_t fill, uint32_t model = 0x77U) {
    Block block;
    block.metadata.num_tokens = 1;
    block.metadata.num_layers = 2;
    block.metadata.dtype_size = 2;
    block.metadata.kv_heads = 4;
    block.metadata.model_fingerprint = model;
    block.data.assign(size, fill);
    return block;
}

void PutRaw(const std::filesystem::path& path, const std::string& key,
            const std::string& value) {
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::DB* db = nullptr;
    const leveldb::Status opened = leveldb::DB::Open(options, path.string(), &db);
    CHECK(opened.ok(), "raw LevelDB opens");
    const leveldb::Status stored = db->Put(leveldb::WriteOptions(), key, value);
    CHECK(stored.ok(), "raw LevelDB Put succeeds");
    delete db;
}

void CheckPayload(StorageNode* node, const BlockKey& key, size_t size, uint8_t fill) {
    auto info = node->Probe(key);
    CHECK(info.ok() && info.value().payload_size == size, "recovered Probe succeeds");
    std::vector<uint8_t> bytes(size);
    BlockView view;
    CHECK(node->Get(key, MutableBuffer{bytes.empty() ? nullptr : bytes.data(), bytes.size()},
                    &view)
              .ok(),
          "recovered Get succeeds");
    CHECK(view.size == size, "recovered payload size matches");
    for (uint8_t byte : bytes) CHECK(byte == fill, "recovered payload byte matches");
}

void TestBlockKeyRoundTripAndStrictParsing() {
    const BlockKey key = Key(1);
    auto parsed = BlockKey::FromString(key.ToString());
    CHECK(parsed.ok() && parsed.value() == key, "BlockKey string round-trip is exact");
    CHECK(BlockKey::FromString("1:1:1").status().code() == StatusCode::kCorruption,
          "short hash is rejected");
    CHECK(BlockKey::FromString(key.ToString() + ":extra").status().code() ==
              StatusCode::kCorruption,
          "trailing key fields are rejected");
    CHECK(BlockKey::FromString("0000000000000001:4294967296:0").status().code() ==
              StatusCode::kCorruption,
          "numeric overflow is rejected");
}

void TestStorageNodeRecoveryAndStats() {
    namespace fs = std::filesystem;
    const fs::path path = Path("valid");
    fs::remove_all(path);
    const BlockKey a = Key(10);
    const BlockKey b = Key(11);
    Location a_before;
    Location b_before;

    {
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(MakeDramTier(4));
        tiers.push_back(MakeSsdTier(path.string()));
        StorageNode node("recovery-node", std::move(tiers), MakeLruEviction());
        CHECK(node.Open().ok(), "initial recovery node opens");
        CHECK(node.Put(a, SizedBlock(8, 1)).ok(), "oversized A is written to SSD");
        CHECK(node.Put(b, SizedBlock(8, 2)).ok(), "oversized B is written to SSD");
        auto a_loc = node.Locate(a);
        auto b_loc = node.Locate(b);
        CHECK(a_loc.ok() && b_loc.ok(), "initial locations exist");
        a_before = a_loc.value();
        b_before = b_loc.value();
        CHECK(a_before.tier == TierType::kSsd && b_before.tier == TierType::kSsd,
              "initial oversized primaries are SSD");
        CHECK(node.Close().ok(), "initial recovery node closes");
    }

    {
        auto ssd = std::make_unique<SsdTier>(path.string());
        SsdTier* ssd_ptr = ssd.get();
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(MakeDramTier(4));
        tiers.push_back(std::move(ssd));
        StorageNode node("recovery-node", std::move(tiers), MakeLruEviction());
        CHECK(node.Open().ok(), "new node reconstructs LocalIndex");
        CHECK(node.IsReady(), "recovered node is ready");
        auto a_loc = node.Locate(a);
        auto b_loc = node.Locate(b);
        CHECK(a_loc.ok() && b_loc.ok(), "both persisted keys are indexed");
        CHECK(a_loc.value().tier == TierType::kSsd && b_loc.value().tier == TierType::kSsd,
              "recovered primary tier is SSD");
        CHECK(a_loc.value().handle == a_before.handle &&
                  b_loc.value().handle == b_before.handle,
              "stable recovered handles match their original locations");
        CHECK(ssd_ptr->Stats().num_blocks == 2 &&
                  ssd_ptr->Stats().used_bytes ==
                      2 * (block_codec::kHeaderSize + static_cast<size_t>(8)),
              "SSD occupancy is rebuilt from validated records");
        CheckPayload(&node, a, 8, 1);
        CheckPayload(&node, b, 8, 2);
        CHECK(node.Locate(a).value().tier == TierType::kSsd,
              "block larger than DRAM remains SSD-only after Get");
        CHECK(node.Close().ok(), "recovered node closes");
    }

    {
        SsdTier tier(path.string());
        CHECK(tier.Open().ok(), "SSD opens for persisted eviction");
        CHECK(tier.Evict(a).ok(), "persisted A is evicted");
        CHECK(tier.Close().ok(), "SSD closes after eviction");
    }
    {
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(MakeDramTier(4));
        tiers.push_back(MakeSsdTier(path.string()));
        StorageNode node("recovery-after-evict", std::move(tiers), MakeLruEviction());
        CHECK(node.Open().ok(), "node opens after persisted eviction");
        CHECK(node.Locate(a).status().code() == StatusCode::kNotFound,
              "evicted key is not recovered");
        CHECK(node.Locate(b).ok(), "remaining key is recovered");
        CHECK(node.Close().ok(), "post-eviction recovery node closes");
    }

    fs::remove_all(path);
}

void TestRealLevelDbTieredClosure(bool use_arc) {
    namespace fs = std::filesystem;
    const char* policy_name = use_arc ? "arc" : "lru";
    const fs::path path = Path(policy_name);
    fs::remove_all(path);
    const BlockKey a = Key(use_arc ? 31 : 30);
    const BlockKey b = Key(use_arc ? 41 : 40);

    {
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(MakeDramTier(4));
        tiers.push_back(MakeSsdTier(path.string()));
        std::unique_ptr<EvictionPolicy> policy =
            use_arc ? MakeArcEviction(4) : MakeLruEviction();
        StorageNode node(std::string("real-closure-") + policy_name, std::move(tiers),
                         std::move(policy));
        CHECK(node.Open().ok(), "real tiered node opens");
        CHECK(node.Put(a, SizedBlock(4, 1)).ok(), "real tiered Put A succeeds");
        CHECK(node.Put(b, SizedBlock(4, 2)).ok(), "real tiered Put B demotes A");
        CHECK(node.Locate(a).value().tier == TierType::kSsd &&
                  node.Locate(b).value().tier == TierType::kDram,
              "real LevelDB receives the selected victim");

        std::vector<uint8_t> bytes(4);
        BlockView view;
        CHECK(node.Get(a, MutableBuffer{bytes.data(), bytes.size()}, &view).ok(),
              "real LevelDB hit promotes A");
        CHECK(node.Locate(a).value().tier == TierType::kDram &&
                  node.Locate(b).value().tier == TierType::kSsd,
              "promotion demotes the prior DRAM resident");
        CHECK(node.Stats().demotions == 2 && node.Stats().promotions == 1,
              "real closure counts both demotions and promotion");
        CHECK(node.Close().ok(), "real tiered node closes");
    }

    {
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(MakeDramTier(4));
        tiers.push_back(MakeSsdTier(path.string()));
        StorageNode node(std::string("real-reopen-") + policy_name, std::move(tiers),
                         use_arc ? MakeArcEviction(4) : MakeLruEviction());
        CHECK(node.Open().ok(), "real tiered node reopens");
        CHECK(node.Locate(a).value().tier == TierType::kSsd &&
                  node.Locate(b).value().tier == TierType::kSsd,
              "inclusive SSD copies rebuild both index entries after restart");
        CheckPayload(&node, a, 4, 1);
        CHECK(node.Close().ok(), "real reopened node closes");
    }

    fs::remove_all(path);
}

void ExpectCorruptOpen(const char* suffix, const std::string& raw_key,
                       const std::string& raw_value) {
    namespace fs = std::filesystem;
    const fs::path path = Path(suffix);
    fs::remove_all(path);
    PutRaw(path, raw_key, raw_value);

    auto ssd = std::make_unique<SsdTier>(path.string());
    SsdTier* ssd_ptr = ssd.get();
    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(MakeDramTier(4));
    tiers.push_back(std::move(ssd));
    StorageNode node("corrupt-recovery", std::move(tiers), MakeLruEviction());
    const Status opened = node.Open();
    CHECK(opened.code() == StatusCode::kCorruption, "corrupt database rejects node Open");
    CHECK(!node.IsReady() && !ssd_ptr->IsReady(),
          "failed recovery leaves node and SSD closed");
    fs::remove_all(path);
}

void TestStrictCorruptionHandling() {
    const BlockKey key = Key(20);
    const Block block = SizedBlock(4, 3);
    const std::string good = SerializeBlock(block);

    ExpectCorruptOpen("bad-key", "not-a-block-key", good);
    ExpectCorruptOpen("bad-value", key.ToString(), "garbage");
    ExpectCorruptOpen("trailing-value", key.ToString(), good + "x");

    Block mismatch = block;
    mismatch.metadata.model_fingerprint = 0x1234U;
    ExpectCorruptOpen("metadata-model", key.ToString(), SerializeBlock(mismatch));

    Block token_mismatch = block;
    token_mismatch.metadata.num_tokens = 2;
    ExpectCorruptOpen("metadata-token", key.ToString(), SerializeBlock(token_mismatch));
}

}  // namespace

int main() {
    TestBlockKeyRoundTripAndStrictParsing();
    TestStorageNodeRecoveryAndStats();
    TestRealLevelDbTieredClosure(false);
    TestRealLevelDbTieredClosure(true);
    TestStrictCorruptionHandling();
    std::printf("tidepool SSD recovery test: all checks passed\n");
    return 0;
}
