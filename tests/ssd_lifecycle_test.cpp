#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "ssd_tier.h"
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

std::filesystem::path TestPath(const char* suffix) {
    return std::filesystem::temp_directory_path() /
           ("tidepool_ssd_lifecycle_" + std::to_string(::getpid()) + "_" + suffix);
}

BlockKey TestKey() { return BlockKey::FromTokenPrefix({4, 5, 6}, 3, 0x1234U); }

Block TestBlock() {
    Block block;
    block.metadata.num_tokens = 3;
    block.metadata.model_fingerprint = 0x1234U;
    block.data = {7, 8, 9, 10, 11};
    return block;
}

void TestSsdTierLifecycle() {
    namespace fs = std::filesystem;
    const fs::path path = TestPath("direct");
    fs::remove_all(path);

    const BlockKey key = TestKey();
    const Block block = TestBlock();
    std::vector<uint8_t> bytes(64);
    MutableBuffer dst{bytes.data(), bytes.size()};
    BlockView view;
    uint64_t handle = 0;

    {
        SsdTier tier(path.string());
        CHECK(!tier.IsReady(), "new SSD tier starts closed");
        CHECK(tier.Close().ok() && tier.Close().ok(), "Close before Open is idempotent");
        CHECK(tier.Put(key, block, &handle).code() == StatusCode::kUnavailable, "closed SSD rejects Put");
        CHECK(tier.Get(key, dst, &view).code() == StatusCode::kUnavailable, "closed SSD rejects Get");
        CHECK(tier.Evict(key).code() == StatusCode::kUnavailable, "closed SSD rejects Evict");

        CHECK(tier.Open().ok(), "SSD Open succeeds");
        CHECK(tier.IsReady(), "SSD is ready after Open");
        CHECK(tier.Open().ok(), "repeated SSD Open is idempotent");
        CHECK(tier.Put(key, block, &handle).ok(), "SSD Put succeeds while open");

        CHECK(tier.Close().ok(), "SSD Close succeeds");
        CHECK(!tier.IsReady(), "SSD is not ready after Close");
        CHECK(tier.Close().ok(), "repeated SSD Close is idempotent");
        CHECK(tier.Put(key, block, &handle).code() == StatusCode::kUnavailable, "Put after Close is rejected");
        CHECK(tier.Get(key, dst, &view).code() == StatusCode::kUnavailable, "Get after Close is rejected");
        CHECK(tier.Evict(key).code() == StatusCode::kUnavailable, "Evict after Close is rejected");

        CHECK(tier.Open().ok() && tier.IsReady(), "SSD reopens after Close");
        CHECK(tier.Get(key, dst, &view).ok(), "persisted value is readable after reopen");
        CHECK(view.size == block.size_bytes() && std::memcmp(view.data, block.data.data(), view.size) == 0,
              "reopened SSD returns the persisted payload");
        CHECK(tier.Close().ok(), "reopened SSD closes");
    }

    fs::remove_all(path);
}

void TestStorageNodeOwnsSsdLifecycle() {
    namespace fs = std::filesystem;
    const fs::path path = TestPath("node");
    fs::remove_all(path);

    auto ssd = std::make_unique<SsdTier>(path.string());
    SsdTier* ssd_observer = ssd.get();
    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::move(ssd));
    StorageNode node("ssd-lifecycle-node", std::move(tiers), nullptr);

    const BlockKey key = TestKey();
    const Block block = TestBlock();
    CHECK(node.Open().ok(), "StorageNode opens its SSD tier");
    CHECK(node.IsReady() && ssd_observer->IsReady(), "node and SSD become ready together");
    CHECK(node.Put(key, block).ok(), "node can use automatically opened SSD");
    CHECK(node.Close().ok(), "StorageNode closes its SSD tier");
    CHECK(!node.IsReady() && !ssd_observer->IsReady(), "node and SSD become closed together");

    CHECK(node.Open().ok(), "StorageNode can reopen its SSD tier");
    std::vector<uint8_t> bytes(64);
    MutableBuffer dst{bytes.data(), bytes.size()};
    BlockView view;
    CHECK(node.Get(key, dst, &view).ok(), "node reads persisted SSD data after reopen");
    CHECK(node.Close().ok(), "reopened StorageNode closes its SSD tier");

    fs::remove_all(path);
}

void TestSsdDestructorAndConcurrentClose() {
    namespace fs = std::filesystem;
    const fs::path destructor_path = TestPath("destructor");
    fs::remove_all(destructor_path);
    {
        SsdTier tier(destructor_path.string());
        CHECK(tier.Open().ok(), "SSD opens before destructor fallback");
    }
    {
        SsdTier tier(destructor_path.string());
        CHECK(tier.Open().ok(), "destructor released the LevelDB handle");
        CHECK(tier.Close().ok(), "explicit Close before destructor succeeds");
    }
    fs::remove_all(destructor_path);

    const fs::path concurrent_path = TestPath("concurrent");
    fs::remove_all(concurrent_path);
    SsdTier tier(concurrent_path.string());
    CHECK(tier.Open().ok(), "SSD opens for concurrent Close test");
    const BlockKey key = TestKey();
    const Block block = TestBlock();
    uint64_t seed_handle = 0;
    CHECK(tier.Put(key, block, &seed_handle).ok(), "concurrent Close test is seeded");
    std::atomic<int> attempts{0};
    std::thread putter([&]() {
        for (int i = 0; i < 100; ++i) {
            uint64_t handle = 0;
            Status s = tier.Put(key, block, &handle);
            ++attempts;
            if (s.code() == StatusCode::kUnavailable) return;
            CHECK(s.ok(), "concurrent Put either succeeds or observes a closed tier");
        }
    });
    std::thread getter([&]() {
        std::vector<uint8_t> bytes(64);
        for (int i = 0; i < 100; ++i) {
            BlockView view;
            Status s = tier.Get(key, MutableBuffer{bytes.data(), bytes.size()}, &view);
            ++attempts;
            if (s.code() == StatusCode::kUnavailable) return;
            CHECK(s.ok() || s.code() == StatusCode::kNotFound,
                  "concurrent Get succeeds, misses, or observes Close");
        }
    });
    std::thread evicter([&]() {
        for (int i = 0; i < 100; ++i) {
            Status s = tier.Evict(key);
            ++attempts;
            if (s.code() == StatusCode::kUnavailable) return;
            CHECK(s.ok() || s.code() == StatusCode::kNotFound,
                  "concurrent Evict succeeds, misses, or observes Close");
        }
    });
    while (attempts.load() == 0) std::this_thread::yield();
    CHECK(tier.Close().ok(), "Close safely synchronizes with Put/Get/Evict");
    putter.join();
    getter.join();
    evicter.join();
    CHECK(!tier.IsReady(), "SSD is closed after concurrent operations finish");
    fs::remove_all(concurrent_path);
}

}  // namespace

int main() {
    TestSsdTierLifecycle();
    TestStorageNodeOwnsSsdLifecycle();
    TestSsdDestructorAndConcurrentClose();
    std::printf("tidepool SSD lifecycle test: all checks passed\n");
    return 0;
}
