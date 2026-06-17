#include "../../include/tidepool/api/block_key.h"

#include <cstdio>
#include <string>

namespace tidepool {

std::string BlockKey::ToString() const {
    // Compact, sortable-ish textual form: <hash16>:<len>:<model>.
    char buf[48];
    std::snprintf(buf, sizeof(buf), "%016llx:%u:%u", static_cast<unsigned long long>(prefix_hash), prefix_len,
                  model_fingerprint);
    return std::string(buf);
}

BlockKey BlockKey::FromTokenPrefix(const std::vector<TokenId>& tokens, uint32_t prefix_len,
                                   uint32_t model_fingerprint) {
    // TODO: replace this FNV-1a placeholder with a strong 64-bit hash (xxh3)
    // and fold the model fingerprint into the seed so different models never
    // alias.
    uint64_t h = 1469598103934665603ULL;  // FNV offset basis
    const uint32_t n = prefix_len < tokens.size() ? prefix_len : static_cast<uint32_t>(tokens.size());
    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t t = static_cast<uint32_t>(tokens[i]);
        for (int b = 0; b < 4; ++b) {
            h ^= static_cast<uint8_t>((t >> (b * 8)) & 0xff);
            h *= 1099511628211ULL;  // FNV prime
        }
    }
    BlockKey key;
    key.prefix_hash = h;
    key.prefix_len = n;
    key.model_fingerprint = model_fingerprint;
    return key;
}

}  // namespace tidepool
