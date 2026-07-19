#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "dram_tier.h"

#ifdef TIDEPOOL_WITH_LEVELDB
#include <unistd.h>

#include <filesystem>

#include "ssd_tier.h"
#endif

using namespace tidepool;

#define CHECK(cond, msg)                                                                   \
    do {                                                                                   \
        if (!(cond)) {                                                                     \
            std::fprintf(stderr, "CHECK failed: %s (%s:%d)\n", (msg), __FILE__, __LINE__); \
            std::abort();                                                                  \
        }                                                                                  \
    } while (0)

namespace {

BlockKey Key(int value) { return BlockKey::FromTokenPrefix({value}, 1, 0x51U); }

Block MakeBlock(size_t size, uint8_t fill) {
    Block block;
    block.metadata.num_tokens = 1;
    block.metadata.num_layers = 2;
    block.metadata.dtype_size = 2;
    block.metadata.kv_heads = 4;
    block.metadata.model_fingerprint = 0x51U;
    block.data.assign(size, fill);
    return block;
}

void FillBogusView(BlockView* view) {
    view->data = reinterpret_cast<const uint8_t*>(0x1);
    view->size = 999;
    view->metadata.num_tokens = 999;
}

void RunContract(Tier* tier, const char* name) {
    const BlockKey key = Key(1);
    const Block block = MakeBlock(5, 0x7a);
    const Block replacement = MakeBlock(7, 0x6b);
    uint64_t handle = 0;
    CHECK(tier->Put(key, block, &handle).ok(), "contract Put succeeds");
    CHECK(tier->Put(key, replacement, &handle).ok(), "contract overwrite succeeds");

    const TierStats after_put = tier->Stats();
    CHECK(after_put.put_count == 2 && after_put.num_blocks == 1,
          "successful Put and overwrite are counted without duplicating blocks");

    auto info = tier->Probe(key);
    CHECK(info.ok(), "Probe succeeds");
    CHECK(info.value().payload_size == replacement.size_bytes(), "Probe returns payload size");
    CHECK(info.value().metadata.num_layers == replacement.metadata.num_layers,
          "Probe returns metadata");
    CHECK(info.value().handle == handle, "Probe returns the stored handle");
    CHECK(tier->Stats().get_count == 0, "Probe is not counted as Get");

    std::vector<uint8_t> exact(replacement.size_bytes());
    BlockView view;
    CHECK(tier->Get(key, MutableBuffer{exact.data(), exact.size()}, &view).ok(),
          "exact-sized Get succeeds");
    CHECK(view.data == exact.data() && view.size == exact.size(), "successful view is bounded");
    CHECK(std::memcmp(view.data, replacement.data.data(), view.size) == 0, "payload matches");
    CHECK(tier->Stats().get_count == 1, "successful Get increments get_count");

    std::vector<uint8_t> small(replacement.size_bytes() - 1);
    FillBogusView(&view);
    CHECK(tier->Get(key, MutableBuffer{small.data(), small.size()}, &view).code() ==
              StatusCode::kOutOfCapacity,
          "short buffer returns OutOfCapacity");
    CHECK(view.data == nullptr && view.size == 0, "short-buffer failure clears out");
    CHECK(tier->Stats().get_count == 1, "short-buffer Get is not counted as success");

    FillBogusView(&view);
    CHECK(tier->Get(key, MutableBuffer{nullptr, 1}, &view).code() ==
              StatusCode::kInvalidArgument,
          "null data with nonzero capacity is invalid");
    CHECK(view.data == nullptr && view.size == 0, "invalid destination clears out");
    CHECK(tier->Stats().get_count == 1, "invalid Get is not counted as success");

    FillBogusView(&view);
    CHECK(tier->Get(Key(99), MutableBuffer{exact.data(), exact.size()}, &view).code() ==
              StatusCode::kNotFound,
          "missing Get returns NotFound");
    CHECK(view.data == nullptr && view.size == 0, "NotFound clears out");
    CHECK(tier->Stats().get_count == 1, "missing Get is not counted as success");

    const BlockKey empty_key = Key(2);
    const Block empty = MakeBlock(0, 0);
    CHECK(tier->Put(empty_key, empty, &handle).ok(), "zero-length Put succeeds");
    FillBogusView(&view);
    CHECK(tier->Get(empty_key, MutableBuffer{nullptr, 0}, &view).ok(),
          "zero-length Get accepts null zero-capacity destination");
    CHECK(view.data == nullptr && view.size == 0, "zero-length Get returns an empty view");

    const TierStats before_missing_evict = tier->Stats();
    CHECK(tier->Evict(Key(100)).code() == StatusCode::kNotFound,
          "missing Evict returns NotFound");
    CHECK(tier->Stats().evict_count == before_missing_evict.evict_count,
          "missing Evict is not counted");
    CHECK(tier->Evict(key).ok(), "existing Evict succeeds");
    CHECK(tier->Stats().evict_count == before_missing_evict.evict_count + 1,
          "successful Evict is counted");

    std::printf("%s Probe/Get/stat contract passed\n", name);
}

}  // namespace

int main() {
    DramTier dram(1 << 20);
    RunContract(&dram, "DRAM");

#ifdef TIDEPOOL_WITH_LEVELDB
    namespace fs = std::filesystem;
    const fs::path path = fs::temp_directory_path() /
                          ("tidepool_tier_contract_" + std::to_string(::getpid()));
    fs::remove_all(path);
    {
        SsdTier ssd(path.string());
        CHECK(ssd.Open().ok(), "SSD contract tier opens");
        RunContract(&ssd, "SSD");
        CHECK(ssd.Close().ok(), "SSD contract tier closes");
    }
    fs::remove_all(path);
#endif

    std::printf("tidepool tier contract test: all checks passed\n");
    return 0;
}
