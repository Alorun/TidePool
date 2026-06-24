// vllm_adapter.h — Adapter between an inference framework (vLLM / LMCache
// backend) and the tidepool Connector. Plane: DATA (integration shim).
//
// This is a STUB illustrating where the engine plugs in. A real adapter would
// implement the LMCache "remote backend" / vLLM KV-connector interface and
// translate the engine's notion of a KV block (layer-major tensors over a token
// range) into tidepool BlockKeys + serialized Blocks, then drive the Connector.
//
// Mapping sketch:
//   engine token range  --> BlockKey::FromTokenPrefix(tokens, len, model_fp)
//   engine KV tensors    --> Block{metadata, serialized bytes}
//   batched prefix probe --> Connector::Lookup  (decide what to recompute)
//   load / store         --> Connector::Get / Connector::Put
//   speculative warm      --> Connector::Prefetch
#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "tidepool/client/connector.h"

namespace tidepool::integration::vllm {

// Opaque description of an engine-side KV block to (de)serialize. Real fields
// (device pointers, layer/head layout, dtype) are framework-specific.
struct EngineBlockDesc {
    std::vector<TokenId> tokens;  // the token range this block covers
    uint32_t prefix_len = 0;
    uint32_t model_fingerprint = 0;
};

// Thin façade the engine calls. Methods are stubs returning NotImplemented.
class VllmKvAdapter {
public:
    explicit VllmKvAdapter(std::shared_ptr<Connector> connector) : connector_(std::move(connector)) {}

    // Batch-probe which engine blocks are already cached in the pool.
    // TODO: derive BlockKeys, call connector_->Lookup, return the bitmap.
    Result<HitMap> LookupPrefix(const std::vector<EngineBlockDesc>& descs);

    // Pull a cached block into engine memory. TODO: Connector::Get +
    // deserialize into the engine's KV tensors.
    Status Load(const EngineBlockDesc& desc, void* dst, std::size_t dst_capacity);

    // Publish a freshly-computed block so other instances can reuse it.
    // TODO: serialize engine KV -> Block, Connector::Put.
    Status Store(const EngineBlockDesc& desc, const void* src, std::size_t len);

private:
    // TODO: helper to turn an EngineBlockDesc into a BlockKey.
    std::shared_ptr<Connector> connector_;
};

}  // namespace tidepool::integration::vllm
