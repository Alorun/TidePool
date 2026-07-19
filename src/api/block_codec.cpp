#include "tidepool/api/block_codec.h"

namespace tidepool {
namespace {

using block_codec::kHeaderSize;
using block_codec::kMagic0;
using block_codec::kMagic1;
using block_codec::kSerdeRaw;
using block_codec::kVersion;

// Little-endian field writers. We write field-by-field (NOT memcpy of the
// BlockMetadata struct) on purpose: the struct has alignment padding (a pad
// gap precedes the u64) and its layout is compiler/ABI-specific, so a struct
// memcpy would (a) freeze the disk format to today's ABI and (b) silently
// misread every already-persisted blob the moment a field is added to
// BlockMetadata. Explicit per-field LE codec decouples the disk format from the
// C++ struct.
void PutU16(std::string* out, uint16_t v) {
    out->push_back(static_cast<char>(v & 0xff));
    out->push_back(static_cast<char>((v >> 8) & 0xff));
}

void PutU32(std::string* out, uint32_t v) {
    for (int i = 0; i < 4; ++i) out->push_back(static_cast<char>((v >> (i * 8)) & 0xff));
}

void PutU64(std::string* out, uint64_t v) {
    for (int i = 0; i < 8; ++i) out->push_back(static_cast<char>((v >> (i * 8)) & 0xff));
}

// Little-endian field readers over a raw byte pointer. Callers guarantee at
// least the header is present before invoking these (length is checked first in
// DeserializeHeader).
uint16_t GetU16(const uint8_t* p) { return static_cast<uint16_t>(p[0]) | (static_cast<uint16_t>(p[1]) << 8); }

uint32_t GetU32(const uint8_t* p) {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(p[i]) << (i * 8);
    return v;
}

uint64_t GetU64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (i * 8);
    return v;
}

}  // namespace

std::string SerializeBlock(const Block& block) {
    const BlockMetadata& m = block.metadata;
    std::string blob;
    blob.reserve(kHeaderSize + block.data.size());

    // Header (offsets per block_codec.h).
    blob.push_back(static_cast<char>(kMagic0));  // 0
    blob.push_back(static_cast<char>(kMagic1));  // 1
    blob.push_back(static_cast<char>(kVersion));  // 2
    blob.push_back(static_cast<char>(kSerdeRaw));  // 3
    PutU32(&blob, m.num_tokens);        // 4
    PutU32(&blob, m.num_layers);        // 8
    PutU16(&blob, m.dtype_size);        // 12
    PutU16(&blob, m.kv_heads);          // 14
    PutU64(&blob, m.created_unix_ns);   // 16
    PutU32(&blob, m.model_fingerprint);  // 24
    PutU64(&blob, block.data.size());   // 28 (payload_len)

    // Payload (serde_id=0: verbatim).
    if (!block.data.empty()) {
        blob.append(reinterpret_cast<const char*>(block.data.data()), block.data.size());
    }
    return blob;
}

Status DeserializeHeader(std::string_view blob, BlockMetadata* meta_out, size_t* payload_len_out,
                         size_t* payload_offset_out) {
    if (blob.size() < kHeaderSize) {
        return Status::InvalidArgument("block_codec: blob shorter than header (36 bytes)");
    }
    const auto* p = reinterpret_cast<const uint8_t*>(blob.data());

    if (p[0] != kMagic0 || p[1] != kMagic1) {
        return Status::InvalidArgument("block_codec: bad magic (not a TP blob)");
    }
    if (p[2] != kVersion) {
        return Status::InvalidArgument("block_codec: unsupported version");
    }
    if (p[3] != kSerdeRaw) {
        // 1+ reserved for future compression codecs (LMCache-style SERDE seam).
        return Status::NotImplemented("block_codec: serde_id not supported (only raw=0)");
    }

    const uint64_t payload_len = GetU64(p + 28);
    // Guard against a truncated/lying header before we hand out the range. Write
    // it as `payload_len > available` (not `kHeaderSize + payload_len > size`)
    // so a huge payload_len can't overflow the addition and slip past the check.
    // blob.size() >= kHeaderSize is already established above.
    const uint64_t available = blob.size() - kHeaderSize;
    if (payload_len != available) {
        return Status::InvalidArgument("block_codec: payload_len does not exactly match blob length");
    }

    if (meta_out) {
        meta_out->num_tokens = GetU32(p + 4);
        meta_out->num_layers = GetU32(p + 8);
        meta_out->dtype_size = GetU16(p + 12);
        meta_out->kv_heads = GetU16(p + 14);
        meta_out->created_unix_ns = GetU64(p + 16);
        meta_out->model_fingerprint = GetU32(p + 24);
    }
    if (payload_len_out) *payload_len_out = static_cast<size_t>(payload_len);
    if (payload_offset_out) *payload_offset_out = kHeaderSize;
    return Status::Ok();
}

}  // namespace tidepool
