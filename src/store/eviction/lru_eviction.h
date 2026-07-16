// lru_eviction.h — Least-Recently-Used replacement policy. Plane: DATA
// (node-internal). Concrete implementation of the EvictionPolicy ABC.
#pragma once

#include <cstddef>
#include <list>
#include <optional>
#include <unordered_map>

#include "tidepool/store/eviction_policy.h"

namespace tidepool {

class LruEviction : public EvictionPolicy {
public:
    void OnAccess(const BlockKey& key) override;
    void OnInsert(const BlockKey& key, size_t size_bytes) override;
    void OnRemove(const BlockKey& key) override;
    Result<BlockKey> SelectVictim() override;
    Status CommitVictim(const BlockKey& key) override;
    Status CancelVictim(const BlockKey& key) override;
    const char* name() const override { return "lru"; }

    size_t resident_size() const { return pos_.size(); }
    bool has_reservation() const { return reserved_.has_value(); }

private:
    // Front = most-recently-used, back = least-recently-used (the victim).
    std::list<BlockKey> order_;
    std::unordered_map<BlockKey, std::list<BlockKey>::iterator> pos_;
    std::optional<BlockKey> reserved_;
};

}  // namespace tidepool
