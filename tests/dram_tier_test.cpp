#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

#include "dram_tier.h"

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

void CheckOccupancy(const DramTier& tier, uint64_t blocks, uint64_t bytes, uint64_t puts) {
    const TierStats stats = tier.Stats();
    CHECK(stats.num_blocks == blocks, "DRAM num_blocks matches");
    CHECK(stats.used_bytes == bytes, "DRAM used_bytes matches");
    CHECK(stats.put_count == puts, "DRAM put_count matches");
}

void TestOverwriteAndEvictStats() {
    DramTier tier(1 << 20);
    const BlockKey key = Key(1);
    uint64_t handle = 0;

    CHECK(tier.Put(key, SizedBlock(100, 1), &handle).ok(), "new Put succeeds");
    CheckOccupancy(tier, 1, 100, 1);

    CHECK(tier.Put(key, SizedBlock(100, 2), &handle).ok(), "same-size overwrite succeeds");
    CheckOccupancy(tier, 1, 100, 2);

    CHECK(tier.Put(key, SizedBlock(160, 3), &handle).ok(), "larger overwrite succeeds");
    CheckOccupancy(tier, 1, 160, 3);

    CHECK(tier.Put(key, SizedBlock(100, 4), &handle).ok(), "smaller overwrite succeeds");
    CheckOccupancy(tier, 1, 100, 4);

    size_t expected_size = 100;
    for (int i = 0; i < 200; ++i) {
        expected_size = i % 2 == 0 ? 1 : 257;
        CHECK(tier.Put(key, SizedBlock(expected_size, static_cast<uint8_t>(i)), &handle).ok(),
              "repeated overwrite succeeds");
        CheckOccupancy(tier, 1, expected_size, static_cast<uint64_t>(5 + i));
    }

    std::vector<uint8_t> bytes(512);
    MutableBuffer dst{bytes.data(), bytes.size()};
    BlockView view;
    CHECK(tier.Get(key, dst, &view).ok(), "Get existing key succeeds");
    CHECK(view.size == expected_size, "Get size matches final overwrite");
    CHECK(tier.Get(Key(99), dst, &view).code() == StatusCode::kNotFound, "Get missing key returns NotFound");
    MutableBuffer too_small{bytes.data(), 0};
    CHECK(tier.Get(key, too_small, &view).code() == StatusCode::kInvalidArgument,
          "invalid Get still counts as an attempted get");

    TierStats before_missing_evict = tier.Stats();
    CHECK(before_missing_evict.get_count == 3, "get_count includes hit, miss and invalid-buffer attempt");
    CHECK(before_missing_evict.hits == 1 && before_missing_evict.misses == 1, "hit/miss counters remain correct");
    CHECK(tier.Evict(Key(99)).code() == StatusCode::kNotFound, "Evict missing key returns NotFound");
    TierStats after_missing_evict = tier.Stats();
    CHECK(after_missing_evict.num_blocks == before_missing_evict.num_blocks &&
              after_missing_evict.used_bytes == before_missing_evict.used_bytes &&
              after_missing_evict.evict_count == before_missing_evict.evict_count,
          "missing Evict does not change occupancy or evict_count");

    CHECK(tier.Evict(key).ok(), "Evict existing key succeeds");
    const TierStats final = tier.Stats();
    CHECK(final.num_blocks == 0 && final.used_bytes == 0, "Evict clears occupancy");
    CHECK(final.evict_count == 1, "successful Evict increments evict_count");
}

void TestConcurrentOverwriteSameKey() {
    DramTier tier(1 << 20);
    const BlockKey key = Key(7);
    constexpr int kThreads = 8;
    constexpr int kPutsPerThread = 250;

    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t]() {
            for (int i = 0; i < kPutsPerThread; ++i) {
                uint64_t handle = 0;
                const size_t size = static_cast<size_t>((t * 31 + i) % 257 + 1);
                CHECK(tier.Put(key, SizedBlock(size, static_cast<uint8_t>(t)), &handle).ok(),
                      "concurrent overwrite succeeds");
            }
        });
    }
    for (auto& worker : workers) worker.join();

    std::vector<uint8_t> bytes(512);
    MutableBuffer dst{bytes.data(), bytes.size()};
    BlockView view;
    CHECK(tier.Get(key, dst, &view).ok(), "final concurrent value is readable");
    const TierStats stats = tier.Stats();
    CHECK(stats.num_blocks == 1, "concurrent overwrites keep one block");
    CHECK(stats.used_bytes == view.size, "concurrent occupancy matches stored payload");
    CHECK(stats.put_count == static_cast<uint64_t>(kThreads * kPutsPerThread),
          "all successful concurrent Puts are counted");
}

}  // namespace

int main() {
    TestOverwriteAndEvictStats();
    TestConcurrentOverwriteSameKey();
    std::printf("tidepool DRAM tier test: all checks passed\n");
    return 0;
}
