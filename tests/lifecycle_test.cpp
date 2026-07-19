#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "tidepool/store/factory.h"
#include "tidepool/store/storage_node.h"

using namespace tidepool;

#define CHECK(cond, msg)                                                                   \
    do {                                                                                   \
        if (!(cond)) {                                                                     \
            std::fprintf(stderr, "CHECK failed: %s (%s:%d)\n", (msg), __FILE__, __LINE__); \
            std::abort();                                                                  \
        }                                                                                  \
    } while (0)

namespace {

struct FakeState {
    bool ready = false;
    bool fail_open = false;
    int open_calls = 0;
    int close_calls = 0;
};

class FakeTier final : public Tier {
public:
    FakeTier(TierType type, int id, std::shared_ptr<FakeState> state, std::vector<int>* close_order = nullptr)
        : type_(type), id_(id), state_(std::move(state)), close_order_(close_order) {}

    TierType type() const override { return type_; }

    Status Open() override {
        ++state_->open_calls;
        if (state_->ready) return Status::Ok();
        if (state_->fail_open) return Status::IoError("injected open failure");
        state_->ready = true;
        return Status::Ok();
    }

    Status Close() override {
        ++state_->close_calls;
        if (state_->ready && close_order_ != nullptr) close_order_->push_back(id_);
        state_->ready = false;
        return Status::Ok();
    }

    bool IsReady() const override { return state_->ready; }

    Result<BlockInfo> Probe(const BlockKey&) override {
        return state_->ready ? Result<BlockInfo>(Status::NotFound())
                             : Result<BlockInfo>(Status::Unavailable("FakeTier is not open"));
    }

    Status Put(const BlockKey&, const Block&, uint64_t*) override {
        return state_->ready ? Status::Ok() : Status::Unavailable("FakeTier is not open");
    }

    Status Get(const BlockKey&, const MutableBuffer& dst, BlockView* out) override {
        if (out) *out = BlockView{};
        if (out == nullptr) return Status::InvalidArgument("FakeTier::Get: out is null");
        if (dst.data == nullptr && dst.capacity != 0) {
            return Status::InvalidArgument("FakeTier::Get: destination is null");
        }
        return state_->ready ? Status::NotFound() : Status::Unavailable("FakeTier is not open");
    }

    Status Evict(const BlockKey&) override {
        return state_->ready ? Status::NotFound() : Status::Unavailable("FakeTier is not open");
    }

    Status ValidateEraseExisting(const BlockKey&) const override {
        return Status::NotFound("FakeTier contains no entries");
    }

    void EraseExisting(const BlockKey&) noexcept override { std::terminate(); }

    TierStats Stats() const override { return {}; }

private:
    TierType type_;
    int id_;
    std::shared_ptr<FakeState> state_;
    std::vector<int>* close_order_;
};

void TestDramNodeLifecycle() {
    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(MakeDramTier(1 << 20));
    StorageNode node("lifecycle-dram", std::move(tiers), nullptr);

    const BlockKey key = BlockKey::FromTokenPrefix({1, 2, 3}, 3);
    Block block;
    block.data = {1, 2, 3, 4};
    std::vector<uint8_t> bytes(16);
    MutableBuffer dst{bytes.data(), bytes.size()};
    BlockView view;

    CHECK(!node.IsReady(), "new node starts closed");
    CHECK(node.Put(key, block).code() == StatusCode::kUnavailable, "Put before Open is rejected");
    CHECK(node.Get(key, dst, &view).code() == StatusCode::kUnavailable, "Get before Open is rejected");
    CHECK(!node.Contains(key).ok(), "Contains before Open returns an error");

    CHECK(node.Open().ok(), "DRAM-only node opens");
    CHECK(node.IsReady(), "node is ready after Open");
    CHECK(node.Open().ok(), "repeated node Open is idempotent");
    CHECK(node.Put(key, block).ok(), "Put works after Open");
    auto contains = node.Contains(key);
    CHECK(contains.ok() && contains.value(), "Contains works after Open");
    CHECK(node.Get(key, dst, &view).ok(), "Get works after Open");
    CHECK(view.size == block.size_bytes() && std::memcmp(view.data, block.data.data(), view.size) == 0,
          "DRAM payload survives lifecycle access");

    CHECK(node.Close().ok(), "node Close succeeds");
    CHECK(!node.IsReady(), "node is not ready after Close");
    CHECK(node.Close().ok(), "repeated node Close is idempotent");
    CHECK(node.Put(key, block).code() == StatusCode::kUnavailable, "Put after Close is rejected");
    CHECK(node.Get(key, dst, &view).code() == StatusCode::kUnavailable, "Get after Close is rejected");
    CHECK(!node.Contains(key).ok(), "Contains after Close returns an error");

    CHECK(node.Open().ok(), "node can reopen after Close");
    CHECK(node.Get(key, dst, &view).ok(), "DRAM-only node works after reopen");
    CHECK(node.Close().ok(), "reopened node closes");
}

void TestOpenFailureRollsBackEarlierTiers() {
    auto first = std::make_shared<FakeState>();
    auto second = std::make_shared<FakeState>();
    second->fail_open = true;

    std::vector<std::unique_ptr<Tier>> tiers;
    tiers.push_back(std::make_unique<FakeTier>(TierType::kDram, 0, first));
    tiers.push_back(std::make_unique<FakeTier>(TierType::kSsd, 1, second));
    StorageNode node("lifecycle-rollback", std::move(tiers), nullptr);

    Status s = node.Open();
    CHECK(!s.ok(), "injected tier Open failure reaches the caller");
    CHECK(s.message().find("tier[1]") != std::string::npos, "Open error identifies the failing tier");
    CHECK(!node.IsReady(), "node stays closed after partial Open failure");
    CHECK(first->open_calls == 1 && first->close_calls == 1 && !first->ready,
          "previously opened tier is rolled back");
    CHECK(second->open_calls == 1 && !second->ready, "failing tier is not published ready");
    CHECK(node.Close().ok(), "Close remains safe after failed Open");
}

void TestCloseOrderAndDestructorFallback() {
    std::vector<int> close_order;
    auto first = std::make_shared<FakeState>();
    auto second = std::make_shared<FakeState>();
    auto third = std::make_shared<FakeState>();
    {
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(std::make_unique<FakeTier>(TierType::kDram, 0, first, &close_order));
        tiers.push_back(std::make_unique<FakeTier>(TierType::kSsd, 1, second, &close_order));
        tiers.push_back(std::make_unique<FakeTier>(TierType::kGpu, 2, third, &close_order));
        StorageNode node("lifecycle-order", std::move(tiers), nullptr);
        CHECK(node.Open().ok() && node.Open().ok(), "Open is idempotent");
        CHECK(first->open_calls == 1 && second->open_calls == 1 && third->open_calls == 1,
              "idempotent Open does not reopen tiers");
        CHECK(node.Close().ok(), "explicit Close succeeds");
    }
    CHECK(close_order == std::vector<int>({2, 1, 0}), "tiers close in reverse configuration order");

    auto destructor_state = std::make_shared<FakeState>();
    {
        std::vector<std::unique_ptr<Tier>> tiers;
        tiers.push_back(std::make_unique<FakeTier>(TierType::kDram, 0, destructor_state));
        StorageNode node("lifecycle-destructor", std::move(tiers), nullptr);
        CHECK(node.Open().ok(), "node for destructor fallback opens");
    }
    CHECK(!destructor_state->ready && destructor_state->close_calls == 1,
          "StorageNode destructor closes an open tier");
}

}  // namespace

int main() {
    TestDramNodeLifecycle();
    TestOpenFailureRollsBackEarlierTiers();
    TestCloseOrderAndDestructorFallback();
    std::printf("tidepool lifecycle test: all checks passed\n");
    return 0;
}
