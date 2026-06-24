#include "vllm_adapter.h"

namespace tidepool::integration::vllm {

Result<HitMap> VllmKvAdapter::LookupPrefix(const std::vector<EngineBlockDesc>& /*descs*/) {
    // TODO: for each desc, key = BlockKey::FromTokenPrefix(desc.tokens,
    // desc.prefix_len, desc.model_fingerprint); then connector_->Lookup(keys).
    return Status::NotImplemented("VllmKvAdapter::LookupPrefix");
}

Status VllmKvAdapter::Load(const EngineBlockDesc& /*desc*/, void* /*dst*/, std::size_t /*dst_capacity*/) {
    // TODO: connector_->Get(key); copy/deserialize the block into dst.
    return Status::NotImplemented("VllmKvAdapter::Load");
}

Status VllmKvAdapter::Store(const EngineBlockDesc& /*desc*/, const void* /*src*/, std::size_t /*len*/) {
    // TODO: wrap src/len into a Block and connector_->Put(key, block).
    return Status::NotImplemented("VllmKvAdapter::Store");
}

}  // namespace tidepool::integration::vllm
