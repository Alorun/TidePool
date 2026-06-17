// block.h — A KV cache block: opaque bytes plus the metadata needed to place,
// transfer and reuse it. Plane: SHARED.
//
// tidepool treats the payload as an opaque byte buffer; the layout (layers,
// heads, dtype) is owned by the inference engine / integration adapter. We only
// carry enough metadata to size buffers, validate compatibility and account for
// capacity in the tiers.
#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace tidepool {

struct BlockMetadata {
    uint32_t num_tokens = 0;  // tokens covered by this block
    uint32_t num_layers = 0;  // transformer layers represented
    uint16_t dtype_size = 0;  // bytes per element (e.g. 2 for fp16/bf16)
    uint16_t kv_heads = 0;    // number of KV heads
    uint64_t created_unix_ns = 0;
    uint32_t model_fingerprint = 0;  // must match BlockKey::model_fingerprint
};

// Owning block. For zero-copy paths the Transfer Engine may instead operate on
// externally-registered memory regions (see transport/transport.h); this struct
// is the simple owning representation used on the control/serialization path.
struct Block {
    BlockMetadata metadata;
    std::vector<uint8_t> data;

    size_t size_bytes() const { return data.size(); }
    bool empty() const { return data.empty(); }
};

}  // namespace tidepool
