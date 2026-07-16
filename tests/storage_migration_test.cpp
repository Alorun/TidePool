#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
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
    block.data.assign(size, fill);
    return block;
}

class FakeSsdTier final : public Tier {
public:
    TierType type() const override { return TierType::kSsd; }

    Status Put(const BlockKey& key, const Block& block, uint64_t* out_handle) override {
        std::unique_lock<std::mutex> lock(mu_);
        if (block_next_put_) {
            block_next_put_ = false;
            put_is_blocked_ = true;
            cv_.notify_all();
            cv_.wait(lock, [this]() { return release_blocked_put_; });
            release_blocked_put_ = false;
            put_is_blocked_ = false;
        }
        if (fail_next_put_) {
            fail_next_put_ = false;
            return Status::IoError("injected SSD Put failure");
        }

        const size_t new_size = block.size_bytes();
        auto it = store_.find(key);
        if (it == store_.end()) {
            store_.emplace(key, block);
            stats_.num_blocks++;
            stats_.used_bytes += new_size;
        } else {
            const size_t old_size = it->second.size_bytes();
            it->second = block;
            if (new_size >= old_size) {
                stats_.used_bytes += new_size - old_size;
            } else {
                stats_.used_bytes -= old_size - new_size;
            }
        }
        stats_.put_count++;
        const uint64_t handle = next_handle_++;
        if (out_handle) *out_handle = handle;
        return Status::Ok();
    }

    Status Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out) override {
        std::lock_guard<std::mutex> lock(mu_);
        stats_.get_count++;
        if (fail_next_get_) {
            fail_next_get_ = false;
            return Status::IoError("injected SSD Get failure");
        }
        auto it = store_.find(key);
        if (it == store_.end()) {
            stats_.misses++;
            return Status::NotFound(key.ToString());
        }
        const Block& block = it->second;
        if (dst.data == nullptr || dst.capacity < block.size_bytes()) {
            return Status::InvalidArgument("fake SSD destination buffer too small");
        }
        std::memcpy(dst.data, block.data.data(), block.size_bytes());
        stats_.hits++;
        if (out) {
            out->data = dst.data;
            out->size = block.size_bytes();
            out->metadata = block.metadata;
        }
        return Status::Ok();
    }

    Status Evict(const BlockKey& key) override {
        std::lock_guard<std::mutex> lock(mu_);
        if (fail_next_evict_) {
            fail_next_evict_ = false;
            return Status::IoError("injected SSD Evict failure");
        }
        auto it = store_.find(key);
        if (it == store_.end()) return Status::NotFound(key.ToString());
        stats_.used_bytes -= it->second.size_bytes();
        stats_.num_blocks--;
        store_.erase(it);
        stats_.evict_count++;
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

    void FailNextGet() {
        std::lock_guard<std::mutex> lock(mu_);
        fail_next_get_ = true;
    }

    void FailNextEvict() {
        std::lock_guard<std::mutex> lock(mu_);
        fail_next_evict_ = true;
    }

    void BlockNextPut() {
        std::lock_guard<std::mutex> lock(mu_);
        block_next_put_ = true;
    }

    void WaitUntilPutBlocked() {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this]() { return put_is_blocked_; });
    }

    void ReleaseBlockedPut() {
        std::lock_guard<std::mutex> lock(mu_);
        release_blocked_put_ = true;
        cv_.notify_all();
    }

private:
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::unordered_map<BlockKey, Block> store_;
    uint64_t next_handle_ = 1;
    TierStats stats_;
    bool fail_next_put_ = false;
    bool fail_next_get_ = false;
    bool fail_next_evict_ = false;
    bool block_next_put_ = false;
    bool put_is_blocked_ = false;
    bool release_blocked_put_ = false;
};

class FaultInjectDramTier final : public DramTier {
public:
    explicit FaultInjectDramTier(uint64_t capacity_bytes) : DramTier(capacity_bytes) {}

    Status Get(const BlockKey& key, const MutableBuffer& dst, BlockView* out) override {
        if (fail_next_get_.exchange(false)) return Status::IoError("injected DRAM Get failure");
        return DramTier::Get(key, dst, out);
    }

    Status Evict(const BlockKey& key) override {
        if (fail_next_evict_.exchange(false)) return Status::IoError("injected DRAM Evict failure");
        return DramTier::Evict(key);
    }

    void FailNextGet() { fail_next_get_.store(true); }
    void FailNextEvict() { fail_next_evict_.store(true); }

private:
    std::atomic<bool> fail_next_get_{false};
    std::atomic<bool> fail_next_evict_{false};
};

std::vector<uint8_t> ReadTier(Tier* tier, const BlockKey& key) {
    const TierStats stats = tier->Stats();
    std::vector<uint8_t> bytes(static_cast<size_t>(stats.used_bytes == 0 ? 1 : stats.used_bytes));
    MutableBuffer dst{bytes.data(), bytes.size()};
    BlockView view;
    CHECK(tier->Get(key, dst, &view).ok(), "tier block is readable");
    bytes.resize(view.size);
    return bytes;
}

void CheckPayload(Tier* tier, const BlockKey& key, size_t size, uint8_t fill) {
    const std::vector<uint8_t> bytes = ReadTier(tier, key);
    CHECK(bytes.size() == size, "payload size matches");
    for (uint8_t byte : bytes) CHECK(byte == fill, "payload bytes match");
}

void CheckLocation(StorageNode& node, const BlockKey& key, TierType expected) {
    auto location = node.Locate(key);
    CHECK(location.ok() && location.value().tier == expected, "LocalIndex tier matches");
}

void TestLruSuccessfulDemotion() {
    auto dram = std::make_unique<DramTier>(8);
    auto ssd = std::make_unique<FakeSsdTier>();
    auto policy = std::make_unique<LruEviction>();
    DramTier* dram_ptr = dram.get();
    FakeSsdTier* ssd_ptr = ssd.get();
    LruEviction* policy_ptr = policy.get();
    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::move(dram));
    tiers.push_back(std::move(ssd));
    StorageNode node("lru-single", std::move(tiers), std::move(policy));
    CHECK(node.Open().ok(), "LRU node opens");

    const BlockKey a = Key(1), b = Key(2), c = Key(3);
    CHECK(node.Put(a, SizedBlock(4, 1)).ok(), "Put A succeeds");
    CHECK(node.Put(b, SizedBlock(4, 2)).ok(), "Put B at exact capacity succeeds");
    CHECK(node.Put(c, SizedBlock(4, 3)).ok(), "Put C demotes one LRU victim");

    CHECK(dram_ptr->Stats().used_bytes == 8 && dram_ptr->Stats().num_blocks == 2,
          "DRAM converges to exact capacity");
    CHECK(ssd_ptr->Stats().num_blocks == 1, "one victim reaches SSD");
    CHECK(dram_ptr->Get(a, MutableBuffer{}, nullptr).code() == StatusCode::kNotFound,
          "demoted LRU victim left DRAM");
    CheckPayload(ssd_ptr, a, 4, 1);
    CheckLocation(node, a, TierType::kSsd);
    CheckLocation(node, b, TierType::kDram);
    CheckLocation(node, c, TierType::kDram);
    CHECK(policy_ptr->resident_size() == 2 && !policy_ptr->has_reservation(),
          "LRU commit removes the victim and clears the reservation");
    CHECK(node.Close().ok(), "LRU node closes");
}

void TestMultipleVictimsLoop() {
    auto dram = std::make_unique<DramTier>(8);
    auto ssd = std::make_unique<FakeSsdTier>();
    DramTier* dram_ptr = dram.get();
    FakeSsdTier* ssd_ptr = ssd.get();
    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::move(dram));
    tiers.push_back(std::move(ssd));
    StorageNode node("lru-multiple", std::move(tiers), std::make_unique<LruEviction>());
    CHECK(node.Open().ok(), "multi-victim node opens");

    const BlockKey a = Key(11), b = Key(12), c = Key(13);
    CHECK(node.Put(a, SizedBlock(4, 1)).ok(), "Put A succeeds");
    CHECK(node.Put(b, SizedBlock(4, 2)).ok(), "Put B succeeds");
    CHECK(node.Put(c, SizedBlock(8, 3)).ok(), "large Put loops over multiple victims");

    CHECK(dram_ptr->Stats().used_bytes == 8 && dram_ptr->Stats().num_blocks == 1,
          "multiple demotions converge to capacity");
    CHECK(ssd_ptr->Stats().num_blocks == 2, "both older victims reach SSD");
    CheckLocation(node, a, TierType::kSsd);
    CheckLocation(node, b, TierType::kSsd);
    CheckLocation(node, c, TierType::kDram);
    CHECK(node.Close().ok(), "multi-victim node closes");
}

void TestSsdPutFailureCancels() {
    auto dram = std::make_unique<DramTier>(4);
    auto ssd = std::make_unique<FakeSsdTier>();
    auto policy = std::make_unique<LruEviction>();
    DramTier* dram_ptr = dram.get();
    FakeSsdTier* ssd_ptr = ssd.get();
    LruEviction* policy_ptr = policy.get();
    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::move(dram));
    tiers.push_back(std::move(ssd));
    StorageNode node("ssd-put-failure", std::move(tiers), std::move(policy));
    CHECK(node.Open().ok(), "SSD failure node opens");

    const BlockKey a = Key(21), b = Key(22);
    CHECK(node.Put(a, SizedBlock(4, 1)).ok(), "initial Put succeeds");
    ssd_ptr->FailNextPut();
    CHECK(node.Put(b, SizedBlock(4, 2)).code() == StatusCode::kIoError,
          "injected SSD Put failure reaches the caller");

    CheckPayload(dram_ptr, a, 4, 1);
    CheckLocation(node, a, TierType::kDram);
    CHECK(!node.Contains(b).value(), "failed initiating Put is rolled back");
    CHECK(ssd_ptr->Stats().num_blocks == 0, "failed SSD Put leaves no SSD copy");
    CHECK(policy_ptr->resident_size() == 1 && !policy_ptr->has_reservation(),
          "SSD failure cancels the victim and removes the rolled-back insert");
    CHECK(node.Close().ok(), "SSD failure node closes");
}

void TestFailedOverwriteRestoresDataAndPolicyOrder() {
    auto dram = std::make_unique<DramTier>(4);
    auto ssd = std::make_unique<FakeSsdTier>();
    auto policy = std::make_unique<LruEviction>();
    DramTier* dram_ptr = dram.get();
    FakeSsdTier* ssd_ptr = ssd.get();
    LruEviction* policy_ptr = policy.get();
    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::move(dram));
    tiers.push_back(std::move(ssd));
    StorageNode node("overwrite-failure", std::move(tiers), std::move(policy));
    CHECK(node.Open().ok(), "overwrite failure node opens");

    const BlockKey a = Key(25), b = Key(26);
    CHECK(node.Put(a, SizedBlock(2, 1)).ok(), "Put A succeeds");
    CHECK(node.Put(b, SizedBlock(2, 2)).ok(), "Put B succeeds");
    ssd_ptr->FailNextPut();
    CHECK(node.Put(a, SizedBlock(4, 9)).code() == StatusCode::kIoError,
          "failed overwrite reports the SSD error");

    CheckPayload(dram_ptr, a, 2, 1);
    CheckPayload(dram_ptr, b, 2, 2);
    CheckLocation(node, a, TierType::kDram);
    CHECK(dram_ptr->Stats().used_bytes == 4 && dram_ptr->Stats().num_blocks == 2,
          "failed overwrite restores old occupancy");
    auto selected = policy_ptr->SelectVictim();
    CHECK(selected.ok() && selected.value() == a, "failed overwrite preserves the old LRU order");
    CHECK(policy_ptr->CancelVictim(a).ok(), "test reservation cancels");
    CHECK(node.Close().ok(), "overwrite failure node closes");
}

void TestVictimReadAndDramEvictFailuresCancel() {
    {
        auto dram = std::make_unique<FaultInjectDramTier>(4);
        auto ssd = std::make_unique<FakeSsdTier>();
        auto policy = std::make_unique<LruEviction>();
        FaultInjectDramTier* dram_ptr = dram.get();
        FakeSsdTier* ssd_ptr = ssd.get();
        LruEviction* policy_ptr = policy.get();
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(std::move(dram));
        tiers.push_back(std::move(ssd));
        StorageNode node("dram-get-failure", std::move(tiers), std::move(policy));
        CHECK(node.Open().ok(), "DRAM Get failure node opens");

        const BlockKey a = Key(31), b = Key(32);
        CHECK(node.Put(a, SizedBlock(4, 1)).ok(), "initial Put succeeds");
        dram_ptr->FailNextGet();
        CHECK(node.Put(b, SizedBlock(4, 2)).code() == StatusCode::kIoError,
              "victim read failure reaches the caller");
        CheckPayload(dram_ptr, a, 4, 1);
        CheckLocation(node, a, TierType::kDram);
        CHECK(!node.Contains(b).value(), "read-failed initiating Put is rolled back");
        CHECK(ssd_ptr->Stats().num_blocks == 0, "victim read failure writes no SSD copy");
        CHECK(policy_ptr->resident_size() == 1 && !policy_ptr->has_reservation(),
              "victim read failure cancels reservation");
        CHECK(node.Close().ok(), "DRAM Get failure node closes");
    }

    {
        auto dram = std::make_unique<FaultInjectDramTier>(4);
        auto ssd = std::make_unique<FakeSsdTier>();
        auto policy = std::make_unique<LruEviction>();
        FaultInjectDramTier* dram_ptr = dram.get();
        FakeSsdTier* ssd_ptr = ssd.get();
        LruEviction* policy_ptr = policy.get();
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(std::move(dram));
        tiers.push_back(std::move(ssd));
        StorageNode node("dram-evict-failure", std::move(tiers), std::move(policy));
        CHECK(node.Open().ok(), "DRAM Evict failure node opens");

        const BlockKey a = Key(41), b = Key(42);
        CHECK(node.Put(a, SizedBlock(4, 1)).ok(), "initial Put succeeds");
        dram_ptr->FailNextEvict();
        CHECK(node.Put(b, SizedBlock(4, 2)).code() == StatusCode::kIoError,
              "DRAM Evict failure reaches the caller");
        CheckPayload(dram_ptr, a, 4, 1);
        CheckPayload(ssd_ptr, a, 4, 1);
        CheckLocation(node, a, TierType::kDram);
        CHECK(!node.Contains(b).value(), "evict-failed initiating Put is rolled back");
        CHECK(policy_ptr->resident_size() == 1 && !policy_ptr->has_reservation(),
              "DRAM Evict failure cancels reservation");
        CHECK(node.Close().ok(), "DRAM Evict failure node closes");
    }
}

void TestArcSuccessfulDemotion() {
    auto dram = std::make_unique<DramTier>(4);
    auto ssd = std::make_unique<FakeSsdTier>();
    auto policy = std::make_unique<ArcEviction>(1);
    DramTier* dram_ptr = dram.get();
    FakeSsdTier* ssd_ptr = ssd.get();
    ArcEviction* policy_ptr = policy.get();
    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::move(dram));
    tiers.push_back(std::move(ssd));
    StorageNode node("arc-success", std::move(tiers), std::move(policy));
    CHECK(node.Open().ok(), "ARC node opens");

    const BlockKey a = Key(51), b = Key(52);
    CHECK(node.Put(a, SizedBlock(4, 1)).ok(), "ARC Put A succeeds");
    CHECK(node.Put(b, SizedBlock(4, 2)).ok(), "ARC Put B demotes A");
    CHECK(dram_ptr->Stats().num_blocks == 1 && ssd_ptr->Stats().num_blocks == 1,
          "ARC migration moves exactly one block");
    CheckLocation(node, a, TierType::kSsd);
    CheckLocation(node, b, TierType::kDram);
    CHECK(policy_ptr->t1_size() == 1 && policy_ptr->b1_size() == 1 && !policy_ptr->has_reservation(),
          "ARC commit moves the T1 victim to B1");
    CHECK(node.Close().ok(), "ARC node closes");
}

void TestOverwriteAccountingAndOversizedBlock() {
    auto dram = std::make_unique<DramTier>(8);
    auto ssd = std::make_unique<FakeSsdTier>();
    auto policy = std::make_unique<LruEviction>();
    DramTier* dram_ptr = dram.get();
    FakeSsdTier* ssd_ptr = ssd.get();
    LruEviction* policy_ptr = policy.get();
    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::move(dram));
    tiers.push_back(std::move(ssd));
    StorageNode node("overwrite", std::move(tiers), std::move(policy));
    CHECK(node.Open().ok(), "overwrite node opens");

    const BlockKey key = Key(61);
    CHECK(node.Put(key, SizedBlock(4, 1)).ok(), "initial overwrite test Put succeeds");
    CHECK(node.Put(key, SizedBlock(8, 2)).ok(), "larger overwrite at capacity succeeds");
    CHECK(dram_ptr->Stats().used_bytes == 8 && dram_ptr->Stats().num_blocks == 1,
          "larger overwrite uses net growth and keeps one resident");
    CHECK(policy_ptr->resident_size() == 1, "overwrite does not duplicate LRU entries");

    CHECK(node.Put(key, SizedBlock(12, 3)).ok(), "single oversized block is safely demoted to SSD");
    CHECK(dram_ptr->Stats().used_bytes == 0 && dram_ptr->Stats().num_blocks == 0,
          "oversized block does not leave DRAM over capacity");
    CHECK(ssd_ptr->Stats().num_blocks == 1, "oversized block reaches SSD");
    CheckLocation(node, key, TierType::kSsd);
    CheckPayload(ssd_ptr, key, 12, 3);
    CHECK(policy_ptr->resident_size() == 0 && !policy_ptr->has_reservation(),
          "oversized self-demotion commits policy state");
    CHECK(node.Close().ok(), "overwrite node closes");
}

void TestNoSinkRollsBack() {
    auto dram = std::make_unique<DramTier>(4);
    DramTier* dram_ptr = dram.get();
    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::move(dram));
    StorageNode node("no-sink", std::move(tiers), std::make_unique<LruEviction>());
    CHECK(node.Open().ok(), "DRAM-only node opens");

    const BlockKey a = Key(71), b = Key(72);
    CHECK(node.Put(a, SizedBlock(4, 1)).ok(), "initial DRAM-only Put succeeds");
    CHECK(node.Put(b, SizedBlock(4, 2)).code() == StatusCode::kOutOfCapacity,
          "over-capacity Put without SSD returns OutOfCapacity");
    CheckPayload(dram_ptr, a, 4, 1);
    CheckLocation(node, a, TierType::kDram);
    CHECK(!node.Contains(b).value(), "failed DRAM-only Put leaves no index entry");
    CHECK(dram_ptr->Stats().used_bytes == 4 && dram_ptr->Stats().num_blocks == 1,
          "failed DRAM-only Put restores capacity");
    CHECK(node.Close().ok(), "DRAM-only node closes");
}

void TestConcurrentOverwriteCannotBeDeletedByOldVictim() {
    auto dram = std::make_unique<DramTier>(4);
    auto ssd = std::make_unique<FakeSsdTier>();
    DramTier* dram_ptr = dram.get();
    FakeSsdTier* ssd_ptr = ssd.get();
    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::move(dram));
    tiers.push_back(std::move(ssd));
    StorageNode node("concurrent-overwrite", std::move(tiers), std::make_unique<LruEviction>());
    CHECK(node.Open().ok(), "concurrent overwrite node opens");

    const BlockKey key = Key(81), other = Key(82);
    CHECK(node.Put(key, SizedBlock(4, 1)).ok(), "initial old version Put succeeds");
    ssd_ptr->BlockNextPut();

    Status first;
    Status second;
    std::thread demoter([&]() { first = node.Put(other, SizedBlock(4, 2)); });
    ssd_ptr->WaitUntilPutBlocked();
    std::thread overwriter([&]() { second = node.Put(key, SizedBlock(4, 9)); });
    ssd_ptr->ReleaseBlockedPut();
    demoter.join();
    overwriter.join();

    CHECK(first.ok() && second.ok(), "both serialized concurrent Puts succeed");
    CheckLocation(node, key, TierType::kDram);
    CheckPayload(dram_ptr, key, 4, 9);
    CheckLocation(node, other, TierType::kSsd);
    CHECK(dram_ptr->Stats().used_bytes == 4, "concurrent overwrite leaves DRAM within capacity");
    CHECK(node.Close().ok(), "concurrent overwrite node closes");
}

void TestConcurrentPutGet() {
    auto dram = std::make_unique<DramTier>(1 << 20);
    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::move(dram));
    StorageNode node("concurrent-put-get", std::move(tiers), std::make_unique<LruEviction>());
    CHECK(node.Open().ok(), "concurrent Put/Get node opens");

    const BlockKey key = Key(91);
    CHECK(node.Put(key, SizedBlock(64, 0)).ok(), "concurrent Put/Get seed succeeds");
    constexpr int kThreads = 6;
    constexpr int kIterations = 200;
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t]() {
            std::vector<uint8_t> bytes(64);
            for (int i = 0; i < kIterations; ++i) {
                CHECK(node.Put(key, SizedBlock(64, static_cast<uint8_t>(t))).ok(),
                      "concurrent StorageNode Put succeeds");
                MutableBuffer dst{bytes.data(), bytes.size()};
                BlockView view;
                CHECK(node.Get(key, dst, &view).ok() && view.size == bytes.size(),
                      "concurrent StorageNode Get succeeds");
            }
        });
    }
    for (auto& worker : workers) worker.join();

    CheckLocation(node, key, TierType::kDram);
    CHECK(node.Close().ok(), "concurrent Put/Get node closes");
}

}  // namespace

int main() {
    TestLruSuccessfulDemotion();
    TestMultipleVictimsLoop();
    TestSsdPutFailureCancels();
    TestFailedOverwriteRestoresDataAndPolicyOrder();
    TestVictimReadAndDramEvictFailuresCancel();
    TestArcSuccessfulDemotion();
    TestOverwriteAccountingAndOversizedBlock();
    TestNoSinkRollsBack();
    TestConcurrentOverwriteCannotBeDeletedByOldVictim();
    TestConcurrentPutGet();
    std::printf("tidepool storage migration test: all checks passed\n");
    return 0;
}
