#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "arc_eviction.h"
#include "dram_tier.h"
#include "lru_eviction.h"
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

BlockKey Key(int value) { return BlockKey::FromTokenPrefix({value}, 1); }

Block SizedBlock(size_t size, uint8_t fill) {
    Block block;
    block.metadata.num_tokens = 1;
    block.data.assign(size, fill);
    return block;
}

class FakePersistentTier final : public Tier {
public:
    TierType type() const override { return TierType::kSsd; }

    Result<BlockInfo> Probe(const BlockKey& key) override {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = store_.find(key);
        if (it == store_.end()) return Status::NotFound(key.ToString());
        return BlockInfo{it->second.block.metadata, it->second.block.size_bytes(),
                         it->second.handle};
    }

    Status Put(const BlockKey& key, const Block& block, uint64_t* out_handle) override {
        std::lock_guard<std::mutex> lock(mu_);
        if (fail_next_put_) {
            fail_next_put_ = false;
            return Status::IoError("injected persistent Put failure");
        }
        auto it = store_.find(key);
        const size_t old_size = it == store_.end() ? 0 : it->second.block.size_bytes();
        const uint64_t handle = next_handle_++;
        if (it == store_.end()) {
            store_.emplace(key, Entry{block, handle});
            stats_.num_blocks++;
        } else {
            it->second = Entry{block, handle};
        }
        if (block.size_bytes() >= old_size) {
            stats_.used_bytes += block.size_bytes() - old_size;
        } else {
            stats_.used_bytes -= old_size - block.size_bytes();
        }
        stats_.put_count++;
        if (out_handle) *out_handle = handle;
        return Status::Ok();
    }

    Status Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out) override {
        std::lock_guard<std::mutex> lock(mu_);
        if (out) *out = BlockView{};
        if (out == nullptr) return Status::InvalidArgument("fake persistent out is null");
        if (dst.data == nullptr && dst.capacity != 0) {
            return Status::InvalidArgument("fake persistent destination is null");
        }
        auto it = store_.find(key);
        if (it == store_.end()) {
            stats_.misses++;
            return Status::NotFound(key.ToString());
        }
        const Block& block = it->second.block;
        if (dst.capacity < block.size_bytes()) {
            return Status(StatusCode::kOutOfCapacity, "fake persistent buffer too small");
        }
        if (!block.data.empty()) std::memcpy(dst.data, block.data.data(), block.size_bytes());
        out->data = dst.data;
        out->size = block.size_bytes();
        out->metadata = block.metadata;
        stats_.get_count++;
        stats_.hits++;
        return Status::Ok();
    }

    Status Evict(const BlockKey& key) override {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = store_.find(key);
        if (it == store_.end()) return Status::NotFound(key.ToString());
        stats_.used_bytes -= it->second.block.size_bytes();
        stats_.num_blocks--;
        store_.erase(it);
        stats_.evict_count++;
        return Status::Ok();
    }

    Status ValidateEraseExisting(const BlockKey&) const override {
        return Status::NotImplemented("persistent fake is not a volatile commit tier");
    }

    void EraseExisting(const BlockKey&) noexcept override { std::terminate(); }

    Status VisitEntries(
        const std::function<Status(const BlockKey&, const BlockInfo&)>& visitor) const override {
        std::vector<std::pair<BlockKey, BlockInfo>> entries;
        {
            std::lock_guard<std::mutex> lock(mu_);
            for (const auto& [key, entry] : store_) {
                entries.emplace_back(
                    key, BlockInfo{entry.block.metadata, entry.block.size_bytes(), entry.handle});
            }
        }
        for (const auto& [key, info] : entries) {
            if (Status s = visitor(key, info); !s.ok()) return s;
        }
        return Status::Ok();
    }

    TierStats Stats() const override {
        std::lock_guard<std::mutex> lock(mu_);
        return stats_;
    }

    void FailNextPut() {
        std::lock_guard<std::mutex> lock(mu_);
        fail_next_put_ = true;
    }

private:
    struct Entry {
        Block block;
        uint64_t handle = 0;
    };

    mutable std::mutex mu_;
    std::unordered_map<BlockKey, Entry> store_;
    uint64_t next_handle_ = 1;
    TierStats stats_;
    bool fail_next_put_ = false;
};

class FaultDramTier final : public DramTier {
public:
    explicit FaultDramTier(uint64_t capacity) : DramTier(capacity) {}

    Status Put(const BlockKey& key, const Block& block, uint64_t* out_handle) override {
        if (fail_next_put_.exchange(false)) return Status::IoError("injected DRAM Put failure");
        return DramTier::Put(key, block, out_handle);
    }

    void FailNextPut() { fail_next_put_.store(true); }

private:
    std::atomic<bool> fail_next_put_{false};
};

class FailPromotionLru final : public LruEviction {
public:
    Status OnPromotionCommitted(const BlockKey&, size_t) override {
        return Status::IoError("injected policy promotion failure");
    }
};

std::vector<uint8_t> ReadTier(Tier* tier, const BlockKey& key) {
    auto info = tier->Probe(key);
    CHECK(info.ok(), "tier Probe succeeds");
    std::vector<uint8_t> bytes(info.value().payload_size);
    BlockView view;
    CHECK(tier->Get(key, MutableBuffer{bytes.empty() ? nullptr : bytes.data(), bytes.size()},
                    &view)
              .ok(),
          "tier Get succeeds");
    return bytes;
}

void CheckPayload(Tier* tier, const BlockKey& key, size_t size, uint8_t fill) {
    const auto bytes = ReadTier(tier, key);
    CHECK(bytes.size() == size, "payload size matches");
    for (uint8_t byte : bytes) CHECK(byte == fill, "payload byte matches");
}

void CheckLocation(StorageNode* node, const BlockKey& key, TierType tier) {
    auto location = node->Locate(key);
    CHECK(location.ok() && location.value().tier == tier, "primary location matches");
}

void TestOldSsdPrimaryProtectedOnFailedOverwrite() {
    auto dram = std::make_unique<FaultDramTier>(4);
    auto ssd = std::make_unique<FakePersistentTier>();
    auto policy = std::make_unique<LruEviction>();
    FaultDramTier* dram_ptr = dram.get();
    FakePersistentTier* ssd_ptr = ssd.get();
    LruEviction* policy_ptr = policy.get();

    const BlockKey key = Key(1);
    uint64_t ignored = 0;
    CHECK(ssd_ptr->Put(key, SizedBlock(4, 1), &ignored).ok(), "old SSD primary is seeded");

    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::move(dram));
    tiers.push_back(std::move(ssd));
    StorageNode node("old-ssd-primary", std::move(tiers), std::move(policy));
    CHECK(node.Open().ok(), "node recovers seeded SSD primary");

    dram_ptr->FailNextPut();
    CHECK(node.Put(key, SizedBlock(4, 9)).code() == StatusCode::kIoError,
          "pre-commit DRAM failure reaches caller");
    CheckPayload(ssd_ptr, key, 4, 1);
    CheckLocation(&node, key, TierType::kSsd);
    CHECK(policy_ptr->resident_size() == 0 && !policy_ptr->has_reservation(),
          "failed overwrite leaves no policy residue");

    CHECK(node.Put(key, SizedBlock(4, 9)).ok(), "replacement enters DRAM");
    CHECK(node.Put(Key(2), SizedBlock(4, 2)).ok(), "next Put demotes replacement");
    CheckPayload(ssd_ptr, key, 4, 9);
    CheckLocation(&node, key, TierType::kSsd);
    CHECK(node.Close().ok(), "old-primary node closes");
}

void TestPromotionAndInclusiveCopy() {
    auto dram = std::make_unique<DramTier>(4);
    auto ssd = std::make_unique<FakePersistentTier>();
    DramTier* dram_ptr = dram.get();
    FakePersistentTier* ssd_ptr = ssd.get();
    const BlockKey key = Key(10);
    uint64_t ignored = 0;
    CHECK(ssd_ptr->Put(key, SizedBlock(4, 7), &ignored).ok(), "SSD value is seeded");

    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::move(dram));
    tiers.push_back(std::move(ssd));
    StorageNode node("promotion-basic", std::move(tiers), std::make_unique<LruEviction>());
    CHECK(node.Open().ok(), "promotion node opens");

    std::vector<uint8_t> bytes(4);
    BlockView view;
    CHECK(node.Get(key, MutableBuffer{bytes.data(), bytes.size()}, &view).ok(),
          "SSD Get remains successful");
    CheckLocation(&node, key, TierType::kDram);
    CheckPayload(dram_ptr, key, 4, 7);
    CheckPayload(ssd_ptr, key, 4, 7);
    const StorageNodeStats stats = node.Stats();
    CHECK(stats.ssd_hits == 1 && stats.promotions == 1, "SSD hit and promotion are counted");
    CHECK(node.Close().ok(), "promotion node closes");
}

void TestPromotionMakesRoomForMultipleVictims() {
    auto dram = std::make_unique<DramTier>(8);
    auto ssd = std::make_unique<FakePersistentTier>();
    DramTier* dram_ptr = dram.get();
    FakePersistentTier* ssd_ptr = ssd.get();
    const BlockKey target = Key(20);
    uint64_t ignored = 0;
    CHECK(ssd_ptr->Put(target, SizedBlock(8, 8), &ignored).ok(), "large SSD value is seeded");

    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::move(dram));
    tiers.push_back(std::move(ssd));
    StorageNode node("promotion-multiple", std::move(tiers), std::make_unique<LruEviction>());
    CHECK(node.Open().ok(), "multi-victim promotion node opens");
    CHECK(node.Put(Key(21), SizedBlock(4, 1)).ok(), "resident A is inserted");
    CHECK(node.Put(Key(22), SizedBlock(4, 2)).ok(), "resident B is inserted");

    std::vector<uint8_t> bytes(8);
    BlockView view;
    CHECK(node.Get(target, MutableBuffer{bytes.data(), bytes.size()}, &view).ok(),
          "large SSD hit succeeds");
    CheckLocation(&node, target, TierType::kDram);
    CheckLocation(&node, Key(21), TierType::kSsd);
    CheckLocation(&node, Key(22), TierType::kSsd);
    CHECK(dram_ptr->Stats().used_bytes == 8 && node.Stats().demotions == 2 &&
              node.Stats().promotions == 1,
          "promotion loops over both victims and converges");
    CHECK(node.Close().ok(), "multi-victim promotion node closes");
}

void TestPromotionFailuresDoNotFailGet() {
    {
        auto dram = std::make_unique<FaultDramTier>(4);
        auto ssd = std::make_unique<FakePersistentTier>();
        FaultDramTier* dram_ptr = dram.get();
        FakePersistentTier* ssd_ptr = ssd.get();
        const BlockKey key = Key(30);
        uint64_t ignored = 0;
        CHECK(ssd_ptr->Put(key, SizedBlock(4, 3), &ignored).ok(), "SSD value is seeded");
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(std::move(dram));
        tiers.push_back(std::move(ssd));
        auto policy = std::make_unique<LruEviction>();
        LruEviction* policy_ptr = policy.get();
        StorageNode node("promotion-dram-failure", std::move(tiers), std::move(policy));
        CHECK(node.Open().ok(), "DRAM-failure promotion node opens");
        dram_ptr->FailNextPut();
        std::vector<uint8_t> bytes(4);
        BlockView view;
        CHECK(node.Get(key, MutableBuffer{bytes.data(), bytes.size()}, &view).ok(),
              "DRAM promotion failure does not fail SSD Get");
        CheckLocation(&node, key, TierType::kSsd);
        CHECK(dram_ptr->Probe(key).status().code() == StatusCode::kNotFound,
              "failed promotion leaves no DRAM copy");
        CHECK(policy_ptr->resident_size() == 0, "failed promotion leaves no resident policy entry");
        CHECK(node.Close().ok(), "DRAM-failure promotion node closes");
    }

    {
        auto dram = std::make_unique<DramTier>(4);
        auto ssd = std::make_unique<FakePersistentTier>();
        FakePersistentTier* ssd_ptr = ssd.get();
        const BlockKey target = Key(31);
        uint64_t ignored = 0;
        CHECK(ssd_ptr->Put(target, SizedBlock(4, 4), &ignored).ok(), "SSD target is seeded");
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(std::move(dram));
        tiers.push_back(std::move(ssd));
        auto policy = std::make_unique<LruEviction>();
        LruEviction* policy_ptr = policy.get();
        StorageNode node("promotion-demotion-failure", std::move(tiers), std::move(policy));
        CHECK(node.Open().ok(), "demotion-failure promotion node opens");
        CHECK(node.Put(Key(32), SizedBlock(4, 2)).ok(), "DRAM victim is inserted");
        ssd_ptr->FailNextPut();
        std::vector<uint8_t> bytes(4);
        BlockView view;
        CHECK(node.Get(target, MutableBuffer{bytes.data(), bytes.size()}, &view).ok(),
              "victim demotion failure does not fail SSD Get");
        CheckLocation(&node, target, TierType::kSsd);
        CheckLocation(&node, Key(32), TierType::kDram);
        CHECK(!policy_ptr->has_reservation() && policy_ptr->resident_size() == 1,
              "failed demotion cancels the victim reservation");
        CHECK(node.Close().ok(), "demotion-failure promotion node closes");
    }

    {
        auto dram = std::make_unique<DramTier>(4);
        auto ssd = std::make_unique<FakePersistentTier>();
        DramTier* dram_ptr = dram.get();
        FakePersistentTier* ssd_ptr = ssd.get();
        const BlockKey key = Key(33);
        uint64_t ignored = 0;
        CHECK(ssd_ptr->Put(key, SizedBlock(4, 5), &ignored).ok(), "policy-failure target is seeded");
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(std::move(dram));
        tiers.push_back(std::move(ssd));
        auto policy = std::make_unique<FailPromotionLru>();
        FailPromotionLru* policy_ptr = policy.get();
        StorageNode node("promotion-policy-failure", std::move(tiers), std::move(policy));
        CHECK(node.Open().ok(), "policy-failure promotion node opens");
        std::vector<uint8_t> bytes(4);
        BlockView view;
        CHECK(node.Get(key, MutableBuffer{bytes.data(), bytes.size()}, &view).ok(),
              "policy failure does not fail SSD Get");
        CheckLocation(&node, key, TierType::kSsd);
        CHECK(dram_ptr->Probe(key).status().code() == StatusCode::kNotFound,
              "policy failure removes uncommitted DRAM copy");
        CHECK(policy_ptr->resident_size() == 0, "policy failure preserves policy state");
        CHECK(node.Close().ok(), "policy-failure promotion node closes");
    }
}

void TestArcGhostTransitionsOnlyOnPromotionCommit() {
    {
        auto dram = std::make_unique<DramTier>(4);
        auto ssd = std::make_unique<FakePersistentTier>();
        auto policy = std::make_unique<ArcEviction>(4);
        ArcEviction* policy_ptr = policy.get();
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(std::move(dram));
        tiers.push_back(std::move(ssd));
        StorageNode node("arc-b1-promotion", std::move(tiers), std::move(policy));
        CHECK(node.Open().ok(), "ARC B1 node opens");
        const BlockKey a = Key(40), b = Key(41);
        CHECK(node.Put(a, SizedBlock(4, 1)).ok(), "ARC A inserted");
        CHECK(node.Put(b, SizedBlock(4, 2)).ok(), "ARC A demoted to B1");
        CHECK(policy_ptr->b1_size() == 1 && policy_ptr->t2_size() == 0,
              "A remains a B1 ghost before SSD Get");
        std::vector<uint8_t> bytes(4);
        BlockView view;
        CHECK(node.Get(a, MutableBuffer{bytes.data(), bytes.size()}, &view).ok(),
              "B1 SSD Get promotes");
        CHECK(policy_ptr->t2_size() == 1 && policy_ptr->p() == 1.0,
              "B1 adaptation occurs only after promotion commits");
        CHECK(node.Close().ok(), "ARC B1 node closes");
    }

    {
        auto dram = std::make_unique<DramTier>(4);
        auto ssd = std::make_unique<FakePersistentTier>();
        auto policy = std::make_unique<ArcEviction>(4);
        ArcEviction* policy_ptr = policy.get();
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(std::move(dram));
        tiers.push_back(std::move(ssd));
        StorageNode node("arc-b2-promotion", std::move(tiers), std::move(policy));
        CHECK(node.Open().ok(), "ARC B2 node opens");
        const BlockKey a = Key(42), b = Key(43);
        CHECK(node.Put(a, SizedBlock(4, 1)).ok(), "ARC A inserted");
        std::vector<uint8_t> bytes(4);
        BlockView view;
        CHECK(node.Get(a, MutableBuffer{bytes.data(), bytes.size()}, &view).ok(),
              "DRAM hit moves A to T2");
        CHECK(node.Put(b, SizedBlock(4, 2)).ok(), "ARC A demoted to B2");
        CHECK(policy_ptr->b2_size() == 1 && policy_ptr->t2_size() == 0,
              "A remains a B2 ghost before SSD Get");
        CHECK(node.Get(a, MutableBuffer{bytes.data(), bytes.size()}, &view).ok(),
              "B2 SSD Get promotes");
        CHECK(policy_ptr->t2_size() == 1 && policy_ptr->b2_size() == 0,
              "B2 ghost enters T2 only after promotion commits");
        CHECK(node.Close().ok(), "ARC B2 node closes");
    }
}

void TestOversizedAndConcurrentPromotion() {
    {
        auto dram = std::make_unique<DramTier>(4);
        auto ssd = std::make_unique<FakePersistentTier>();
        DramTier* dram_ptr = dram.get();
        FakePersistentTier* ssd_ptr = ssd.get();
        const BlockKey key = Key(50);
        uint64_t ignored = 0;
        CHECK(ssd_ptr->Put(key, SizedBlock(8, 8), &ignored).ok(), "oversized SSD value is seeded");
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(std::move(dram));
        tiers.push_back(std::move(ssd));
        StorageNode node("oversized-promotion", std::move(tiers), std::make_unique<LruEviction>());
        CHECK(node.Open().ok(), "oversized promotion node opens");
        std::vector<uint8_t> bytes(8);
        BlockView view;
        CHECK(node.Get(key, MutableBuffer{bytes.data(), bytes.size()}, &view).ok(),
              "oversized SSD Get succeeds");
        CheckLocation(&node, key, TierType::kSsd);
        CHECK(dram_ptr->Probe(key).status().code() == StatusCode::kNotFound &&
                  node.Stats().promotions == 0,
              "oversized block remains SSD-only");
        CHECK(node.Close().ok(), "oversized promotion node closes");
    }

    {
        auto dram = std::make_unique<DramTier>(4);
        auto ssd = std::make_unique<FakePersistentTier>();
        FakePersistentTier* ssd_ptr = ssd.get();
        const BlockKey key = Key(51);
        uint64_t ignored = 0;
        CHECK(ssd_ptr->Put(key, SizedBlock(4, 6), &ignored).ok(), "concurrent target is seeded");
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(std::move(dram));
        tiers.push_back(std::move(ssd));
        StorageNode node("concurrent-promotion", std::move(tiers), std::make_unique<LruEviction>());
        CHECK(node.Open().ok(), "concurrent promotion node opens");
        constexpr int kThreads = 8;
        std::vector<std::thread> threads;
        for (int i = 0; i < kThreads; ++i) {
            threads.emplace_back([&]() {
                std::vector<uint8_t> bytes(4);
                BlockView view;
                CHECK(node.Get(key, MutableBuffer{bytes.data(), bytes.size()}, &view).ok(),
                      "concurrent Get succeeds");
            });
        }
        for (auto& thread : threads) thread.join();
        CheckLocation(&node, key, TierType::kDram);
        CHECK(node.Stats().promotions == 1, "concurrent Gets commit one promotion");
        CHECK(node.Close().ok(), "concurrent promotion node closes");
    }
}

}  // namespace

int main() {
    TestOldSsdPrimaryProtectedOnFailedOverwrite();
    TestPromotionAndInclusiveCopy();
    TestPromotionMakesRoomForMultipleVictims();
    TestPromotionFailuresDoNotFailGet();
    TestArcGhostTransitionsOnlyOnPromotionCommit();
    TestOversizedAndConcurrentPromotion();
    std::printf("tidepool storage promotion test: all checks passed\n");
    return 0;
}
