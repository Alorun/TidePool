#include "tidepool/api/block_key.h"

#include <charconv>
#include <cstdio>
#include <limits>
#include <string>

namespace tidepool {
namespace {

template <typename T>
bool ParseUnsigned(std::string_view text, int base, T* out) {
    if (text.empty() || out == nullptr) return false;
    T value = 0;
    const char* begin = text.data();
    const char* end = begin + text.size();
    const auto parsed = std::from_chars(begin, end, value, base);
    if (parsed.ec != std::errc() || parsed.ptr != end) return false;
    *out = value;
    return true;
}

}  // namespace

std::string BlockKey::ToString() const {
    // Compact, sortable-ish textual form: <hash16>:<len>:<model>.
    char buf[48];
    std::snprintf(buf, sizeof(buf), "%016llx:%u:%u", static_cast<unsigned long long>(prefix_hash), prefix_len,
                  model_fingerprint);
    return std::string(buf);
}

Result<BlockKey> BlockKey::FromString(std::string_view encoded) {
    const size_t first = encoded.find(':');
    if (first != 16 || first == std::string_view::npos) {
        return Status::Corruption("BlockKey: expected exactly 16 hexadecimal hash digits");
    }
    const size_t second = encoded.find(':', first + 1);
    if (second == std::string_view::npos || encoded.find(':', second + 1) != std::string_view::npos) {
        return Status::Corruption("BlockKey: expected hash:length:model");
    }

    BlockKey key;
    if (!ParseUnsigned(encoded.substr(0, first), 16, &key.prefix_hash) ||
        !ParseUnsigned(encoded.substr(first + 1, second - first - 1), 10, &key.prefix_len) ||
        !ParseUnsigned(encoded.substr(second + 1), 10, &key.model_fingerprint)) {
        return Status::Corruption("BlockKey: invalid or overflowing numeric field");
    }
    if (key.ToString() != encoded) {
        return Status::Corruption("BlockKey: non-canonical encoding");
    }
    return key;
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
