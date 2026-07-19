#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "tidepool/store/factory.h"
#include "tidepool/store/storage_node.h"

using namespace tidepool;

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
    size_t block_size = 4096;
    size_t working_set_blocks = 1024;
    uint64_t dram_capacity = 0;
    size_t operations = 100000;
    std::string policy = "lru";
    std::string access_pattern = "zipf";
    uint64_t simulated_recompute_us = 1000;
    std::string db_path;
    std::string format = "text";
};

struct Summary {
    uint64_t operations = 0;
    uint64_t dram_hits = 0;
    uint64_t ssd_hits = 0;
    uint64_t misses = 0;
    double hit_rate = 0;
    double p50_us = 0;
    double p95_us = 0;
    double p99_us = 0;
    uint64_t demotions = 0;
    uint64_t promotions = 0;
    uint64_t bytes_read = 0;
    uint64_t bytes_written = 0;
    double dram_hit_us = 0;
    double ssd_hit_us = 0;
    double ssd_promotion_us = 0;
    double miss_us = 0;
    double simulated_saved_us = 0;
};

bool ParseUnsigned(const std::string& text, uint64_t* out) {
    if (text.empty() || text.front() == '-') return false;
    char* end = nullptr;
    const unsigned long long value = std::strtoull(text.c_str(), &end, 10);
    if (end == text.c_str() || *end != '\0') return false;
    *out = value;
    return true;
}

bool ParseOptions(int argc, char** argv, Options* options) {
    for (int i = 1; i < argc; ++i) {
        const std::string arg(argv[i]);
        const size_t equal = arg.find('=');
        if (equal == std::string::npos || arg.rfind("--", 0) != 0) return false;
        const std::string name = arg.substr(2, equal - 2);
        const std::string value = arg.substr(equal + 1);
        uint64_t number = 0;
        if (name == "block-size") {
            if (!ParseUnsigned(value, &number) || number == 0) return false;
            options->block_size = static_cast<size_t>(number);
        } else if (name == "working-set-blocks") {
            if (!ParseUnsigned(value, &number) || number == 0) return false;
            options->working_set_blocks = static_cast<size_t>(number);
        } else if (name == "dram-capacity") {
            if (!ParseUnsigned(value, &number)) return false;
            options->dram_capacity = number;
        } else if (name == "operations") {
            if (!ParseUnsigned(value, &number) || number == 0) return false;
            options->operations = static_cast<size_t>(number);
        } else if (name == "simulated-recompute-us") {
            if (!ParseUnsigned(value, &number)) return false;
            options->simulated_recompute_us = number;
        } else if (name == "policy") {
            options->policy = value;
        } else if (name == "access-pattern") {
            options->access_pattern = value;
        } else if (name == "db-path") {
            options->db_path = value;
        } else if (name == "format") {
            options->format = value;
        } else {
            return false;
        }
    }
    return (options->policy == "lru" || options->policy == "arc") &&
           (options->access_pattern == "uniform" ||
            options->access_pattern == "sequential" ||
            options->access_pattern == "zipf") &&
           (options->format == "text" || options->format == "csv" ||
            options->format == "json");
}

double Percentile(std::vector<double> values, double quantile) {
    if (values.empty()) return 0;
    std::sort(values.begin(), values.end());
    const size_t index = static_cast<size_t>(
        std::min<double>(values.size() - 1, std::ceil(quantile * values.size()) - 1));
    return values[index];
}

double Mean(const std::vector<double>& values) {
    if (values.empty()) return 0;
    return std::accumulate(values.begin(), values.end(), 0.0) / values.size();
}

template <typename Fn>
double TimeUs(Fn&& fn) {
    const auto begin = Clock::now();
    fn();
    const auto end = Clock::now();
    return std::chrono::duration<double, std::micro>(end - begin).count();
}

BlockKey Key(size_t index) {
    return BlockKey::FromTokenPrefix({static_cast<TokenId>(index)}, 1, 0x4242U);
}

Block MakeBlock(size_t size, uint8_t fill) {
    Block block;
    block.metadata.num_tokens = 1;
    block.metadata.num_layers = 32;
    block.metadata.dtype_size = 2;
    block.metadata.kv_heads = 8;
    block.metadata.model_fingerprint = 0x4242U;
    block.data.assign(size, fill);
    return block;
}

std::unique_ptr<EvictionPolicy> MakePolicy(const Options& options) {
    if (options.policy == "arc") {
        const size_t blocks = std::max<size_t>(
            1, static_cast<size_t>(options.dram_capacity / options.block_size));
        return MakeArcEviction(blocks);
    }
    return MakeLruEviction();
}

size_t NextIndex(const Options& options, size_t operation, std::mt19937_64* rng,
                 std::uniform_int_distribution<size_t>* uniform,
                 std::discrete_distribution<size_t>* zipf) {
    if (options.access_pattern == "sequential") {
        return operation % options.working_set_blocks;
    }
    if (options.access_pattern == "uniform") return (*uniform)(*rng);
    return (*zipf)(*rng);
}

bool Get(Tier* tier, const BlockKey& key, std::vector<uint8_t>* buffer) {
    BlockView view;
    return tier
        ->Get(key, MutableBuffer{buffer->empty() ? nullptr : buffer->data(), buffer->size()},
              &view)
        .ok();
}

void PrintSummary(const Options& options, const Summary& summary) {
    std::printf("tidepool phase-1 tiered cache benchmark\n");
    std::printf("policy=%s pattern=%s block_size=%zu working_set=%zu dram_capacity=%llu\n",
                options.policy.c_str(), options.access_pattern.c_str(), options.block_size,
                options.working_set_blocks,
                static_cast<unsigned long long>(options.dram_capacity));
    std::printf(
        "DRAM hit=%.3f us, LevelDB SSD hit=%.3f us, SSD hit+promotion=%.3f us, "
        "complete miss=%.3f us\n",
        summary.dram_hit_us, summary.ssd_hit_us, summary.ssd_promotion_us,
        summary.miss_us);
    std::printf(
        "operations=%llu dram_hits=%llu ssd_hits=%llu misses=%llu hit_rate=%.6f "
        "p50_us=%.3f p95_us=%.3f p99_us=%.3f demotions=%llu promotions=%llu "
        "bytes_read=%llu bytes_written=%llu\n",
        static_cast<unsigned long long>(summary.operations),
        static_cast<unsigned long long>(summary.dram_hits),
        static_cast<unsigned long long>(summary.ssd_hits),
        static_cast<unsigned long long>(summary.misses), summary.hit_rate,
        summary.p50_us, summary.p95_us, summary.p99_us,
        static_cast<unsigned long long>(summary.demotions),
        static_cast<unsigned long long>(summary.promotions),
        static_cast<unsigned long long>(summary.bytes_read),
        static_cast<unsigned long long>(summary.bytes_written));
    std::printf(
        "simulated recompute only (not a real vLLM/LLM Prefill benchmark): "
        "configured=%llu us, mean estimated saved_time=%.3f us per cache hit\n",
        static_cast<unsigned long long>(options.simulated_recompute_us),
        summary.simulated_saved_us);

    if (options.format == "csv") {
        std::printf(
            "operations,dram_hits,ssd_hits,misses,hit_rate,p50_us,p95_us,p99_us,"
            "demotions,promotions,bytes_read,bytes_written,dram_hit_us,ssd_hit_us,"
            "ssd_promotion_us,miss_us\n");
        std::printf(
            "%llu,%llu,%llu,%llu,%.6f,%.3f,%.3f,%.3f,%llu,%llu,%llu,%llu,"
            "%.3f,%.3f,%.3f,%.3f\n",
            static_cast<unsigned long long>(summary.operations),
            static_cast<unsigned long long>(summary.dram_hits),
            static_cast<unsigned long long>(summary.ssd_hits),
            static_cast<unsigned long long>(summary.misses), summary.hit_rate,
            summary.p50_us, summary.p95_us, summary.p99_us,
            static_cast<unsigned long long>(summary.demotions),
            static_cast<unsigned long long>(summary.promotions),
            static_cast<unsigned long long>(summary.bytes_read),
            static_cast<unsigned long long>(summary.bytes_written),
            summary.dram_hit_us, summary.ssd_hit_us, summary.ssd_promotion_us,
            summary.miss_us);
    } else if (options.format == "json") {
        std::printf(
            "{\"operations\":%llu,\"dram_hits\":%llu,\"ssd_hits\":%llu,"
            "\"misses\":%llu,\"hit_rate\":%.6f,\"p50_us\":%.3f,"
            "\"p95_us\":%.3f,\"p99_us\":%.3f,\"demotions\":%llu,"
            "\"promotions\":%llu,\"bytes_read\":%llu,\"bytes_written\":%llu,"
            "\"dram_hit_us\":%.3f,\"ssd_hit_us\":%.3f,"
            "\"ssd_promotion_us\":%.3f,\"miss_us\":%.3f}\n",
            static_cast<unsigned long long>(summary.operations),
            static_cast<unsigned long long>(summary.dram_hits),
            static_cast<unsigned long long>(summary.ssd_hits),
            static_cast<unsigned long long>(summary.misses), summary.hit_rate,
            summary.p50_us, summary.p95_us, summary.p99_us,
            static_cast<unsigned long long>(summary.demotions),
            static_cast<unsigned long long>(summary.promotions),
            static_cast<unsigned long long>(summary.bytes_read),
            static_cast<unsigned long long>(summary.bytes_written),
            summary.dram_hit_us, summary.ssd_hit_us, summary.ssd_promotion_us,
            summary.miss_us);
    }
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!ParseOptions(argc, argv, &options)) {
        std::fprintf(
            stderr,
            "usage: %s [--block-size=N] [--working-set-blocks=N] "
            "[--dram-capacity=N] [--operations=N] [--policy=lru|arc] "
            "[--access-pattern=uniform|sequential|zipf] "
            "[--simulated-recompute-us=N] [--db-path=PATH] "
            "[--format=text|csv|json]\n",
            argv[0]);
        return 2;
    }
    if (options.dram_capacity == 0) {
        options.dram_capacity =
            options.block_size * std::max<size_t>(1, options.working_set_blocks / 8);
    }

    namespace fs = std::filesystem;
    const bool temporary_path = options.db_path.empty();
    const fs::path root = temporary_path
                              ? fs::temp_directory_path() /
                                    ("tidepool_bench_" + std::to_string(::getpid()) + "_" +
                                     options.policy)
                              : fs::path(options.db_path);
    const fs::path tiered_path = root / "tiered";
    const fs::path direct_path = root / "direct";
    if (temporary_path) fs::remove_all(root);
    fs::create_directories(root);

    Summary summary;
    std::vector<uint8_t> buffer(options.block_size);
    const Block direct_block = MakeBlock(options.block_size, 0x5a);
    const BlockKey direct_key = Key(options.working_set_blocks + 1);

    auto direct_dram = MakeDramTier(options.block_size * 2);
    uint64_t ignored = 0;
    if (!direct_dram->Put(direct_key, direct_block, &ignored).ok()) return 1;
    std::vector<double> direct_dram_samples;
    for (size_t i = 0; i < 1000; ++i) {
        direct_dram_samples.push_back(
            TimeUs([&]() { (void)Get(direct_dram.get(), direct_key, &buffer); }));
    }
    summary.dram_hit_us = Percentile(direct_dram_samples, 0.5);

    auto direct_ssd = MakeSsdTier(direct_path.string());
    if (Status s = direct_ssd->Open(); !s.ok()) {
        std::fprintf(stderr, "benchmark SSD open failed: %s\n", s.ToString().c_str());
        return 1;
    }
    if (!direct_ssd->Put(direct_key, direct_block, &ignored).ok()) return 1;
    std::vector<double> direct_ssd_samples;
    for (size_t i = 0; i < 1000; ++i) {
        direct_ssd_samples.push_back(
            TimeUs([&]() { (void)Get(direct_ssd.get(), direct_key, &buffer); }));
    }
    summary.ssd_hit_us = Percentile(direct_ssd_samples, 0.5);
    if (!direct_ssd->Close().ok()) return 1;

    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(MakeDramTier(options.dram_capacity));
    tiers.push_back(MakeSsdTier(tiered_path.string()));
    StorageNode node("tiered-benchmark", std::move(tiers), MakePolicy(options));
    if (Status s = node.Open(); !s.ok()) {
        std::fprintf(stderr, "benchmark node open failed: %s\n", s.ToString().c_str());
        return 1;
    }

    std::vector<BlockKey> keys;
    keys.reserve(options.working_set_blocks);
    for (size_t i = 0; i < options.working_set_blocks; ++i) {
        keys.push_back(Key(i));
        if (Status s = node.Put(keys.back(), MakeBlock(options.block_size,
                                                       static_cast<uint8_t>(i)));
            !s.ok()) {
            std::fprintf(stderr, "benchmark seed Put failed: %s\n", s.ToString().c_str());
            return 1;
        }
    }

    for (const BlockKey& key : keys) {
        auto location = node.Locate(key);
        if (location.ok() && location.value().tier == TierType::kSsd) {
            BlockView view;
            summary.ssd_promotion_us = TimeUs([&]() {
                (void)node.Get(key, MutableBuffer{buffer.data(), buffer.size()}, &view);
            });
            break;
        }
    }

    const StorageNodeStats before = node.Stats();
    std::mt19937_64 rng(0x74696465706f6f6cULL);
    std::uniform_int_distribution<size_t> uniform(0, options.working_set_blocks - 1);
    std::vector<double> weights(options.working_set_blocks);
    for (size_t i = 0; i < weights.size(); ++i) {
        weights[i] = 1.0 / std::pow(static_cast<double>(i + 1), 1.1);
    }
    std::discrete_distribution<size_t> zipf(weights.begin(), weights.end());
    std::vector<double> latencies;
    std::vector<double> dram_latencies;
    std::vector<double> miss_latencies;
    latencies.reserve(options.operations);

    const BlockKey missing = Key(options.working_set_blocks + 1000);
    for (size_t i = 0; i < options.operations; ++i) {
        const bool inject_miss = (i % 20 == 0);
        const BlockKey& key =
            inject_miss
                ? missing
                : keys[NextIndex(options, i, &rng, &uniform, &zipf)];
        TierType before_tier = TierType::kDram;
        if (!inject_miss) {
            auto location = node.Locate(key);
            if (location.ok()) before_tier = location.value().tier;
        }
        BlockView view;
        Status status;
        const double latency = TimeUs([&]() {
            status = node.Get(key, MutableBuffer{buffer.data(), buffer.size()}, &view);
        });
        latencies.push_back(latency);
        if (status.ok() && before_tier == TierType::kDram) {
            dram_latencies.push_back(latency);
        } else if (!status.ok()) {
            miss_latencies.push_back(latency);
        }
    }

    const StorageNodeStats after = node.Stats();
    summary.operations = options.operations;
    summary.dram_hits = after.dram_hits - before.dram_hits;
    summary.ssd_hits = after.ssd_hits - before.ssd_hits;
    summary.misses = after.misses - before.misses;
    const uint64_t hits = summary.dram_hits + summary.ssd_hits;
    summary.hit_rate = summary.operations == 0
                           ? 0
                           : static_cast<double>(hits) / summary.operations;
    summary.p50_us = Percentile(latencies, 0.50);
    summary.p95_us = Percentile(latencies, 0.95);
    summary.p99_us = Percentile(latencies, 0.99);
    summary.demotions = after.demotions;
    summary.promotions = after.promotions;
    summary.bytes_read = after.bytes_read - before.bytes_read;
    summary.bytes_written = after.bytes_written;
    if (!dram_latencies.empty()) summary.dram_hit_us = Percentile(dram_latencies, 0.5);
    summary.miss_us = Percentile(miss_latencies, 0.5);
    summary.simulated_saved_us =
        std::max(0.0, static_cast<double>(options.simulated_recompute_us) -
                          Mean(latencies));

    PrintSummary(options, summary);
    if (Status s = node.Close(); !s.ok()) {
        std::fprintf(stderr, "benchmark node close failed: %s\n", s.ToString().c_str());
        return 1;
    }
    if (temporary_path) fs::remove_all(root);
    return 0;
}
