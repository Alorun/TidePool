// block_key.h — Global, content-derived identity of a KV cache block.
// Plane: SHARED.
//
// KEY DESIGN PRINCIPLE: a block is keyed by a *hash of its token prefix*, not
// by a request id. Two requests that share a common prompt prefix therefore
// produce identical BlockKeys for the shared region and can reuse each other's
// cached KV — this is what enables cross-instance prefix reuse.
//
// Because the key is content-addressed, any client can compute it locally and
// then route to the owning node via the consistent-hash ring WITHOUT querying
// any central service (see hashring/).
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "tidepool/api/status.h"

namespace tidepool {

// A token id as seen by the inference engine.
using TokenId = int32_t;

struct BlockKey {
    // Hash over the token prefix [0, prefix_len). Equal prefixes => equal hash.
    uint64_t prefix_hash = 0;
    // Number of tokens covered by this block's prefix. Disambiguates rare hash
    // collisions and records prefix length for reuse/debugging.
    uint32_t prefix_len = 0;
    // Optional model/quantization fingerprint so caches for different models do
    // not alias. 0 means "unspecified".
    uint32_t model_fingerprint = 0;

    bool operator==(const BlockKey& o) const {
        return prefix_hash == o.prefix_hash && prefix_len == o.prefix_len && model_fingerprint == o.model_fingerprint;
    }
    bool operator!=(const BlockKey& o) const { return !(*this == o); }

    // Stable string form used by tier backends (e.g. LevelDB key) and logs.
    std::string ToString() const;
    // Strict inverse of ToString(). Rejects malformed, overflowing or trailing
    // input instead of guessing missing fields.
    static Result<BlockKey> FromString(std::string_view encoded);

    // TODO: replace std::hash<uint64_t> seed with a stronger 64-bit mixer
    // (e.g. xxh3) and make `model_fingerprint` derivation explicit.
    static BlockKey FromTokenPrefix(const std::vector<TokenId>& tokens, uint32_t prefix_len,
                                    uint32_t model_fingerprint = 0);
};

}  // namespace tidepool

namespace std {
template <>
struct hash<tidepool::BlockKey> {
    size_t operator()(const tidepool::BlockKey& k) const noexcept {
        // Combine fields; prefix_hash already carries most entropy.
        size_t h = std::hash<uint64_t>{}(k.prefix_hash);
        h ^= std::hash<uint32_t>{}(k.prefix_len) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= std::hash<uint32_t>{}(k.model_fingerprint) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return h;
    }
};
}  // namespace std
