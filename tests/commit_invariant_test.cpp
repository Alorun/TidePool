#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <functional>

#include "arc_eviction.h"
#include "dram_tier.h"
#include "lru_eviction.h"
#include "tidepool/store/local_index.h"

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

void ExpectInvariantTermination(const std::function<void()>& operation,
                                const char* description) {
    const pid_t child = ::fork();
    CHECK(child >= 0, "fork succeeds");
    if (child == 0) {
        operation();
        std::_Exit(0);
    }
    int status = 0;
    CHECK(::waitpid(child, &status, 0) == child, "waitpid succeeds");
    CHECK(WIFSIGNALED(status), description);
}

void TestCommitInterfacesRejectMissingPreflight() {
    const BlockKey key = Key(1);
    ExpectInvariantTermination(
        [&]() {
            DramTier tier(16);
            tier.EraseExisting(key);
        },
        "DramTier commit erasure rejects an absent key");

    ExpectInvariantTermination(
        [&]() {
            LocalIndex index;
            index.RelocateExisting(key, TierType::kDram, TierType::kSsd, 1);
        },
        "LocalIndex commit relocation rejects an absent key");

    ExpectInvariantTermination(
        [&]() {
            LruEviction lru;
            lru.CommitVictim(key);
        },
        "LRU commit rejects a missing reservation");

    ExpectInvariantTermination(
        [&]() {
            ArcEviction arc(2);
            arc.CommitVictim(key);
        },
        "ARC commit rejects a missing reservation");
}

}  // namespace

int main() {
    TestCommitInterfacesRejectMissingPreflight();
    std::printf("tidepool commit invariant test: all checks passed\n");
    return 0;
}
