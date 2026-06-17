// status.h — Error-handling primitives shared across data plane and control
// plane. Plane: SHARED (used everywhere).
//
// Design note: the data plane (hot path) returns Status / Result<T> instead of
// throwing, so that get/put never pay exception-unwinding cost. Stub code in
// this scaffold returns Status::NotImplemented(); a few non-hot-path stubs may
// throw std::runtime_error("not implemented") where a return value is awkward.
#pragma once

#include <optional>
#include <string>
#include <type_traits>
#include <utility>

namespace tidepool {

enum class StatusCode {
    kOk = 0,
    kNotImplemented,
    kNotFound,
    kInvalidArgument,
    kAlreadyExists,
    kIoError,
    kNetworkError,
    kUnavailable,    // node down / shard map stale, caller should refresh+retry
    kOutOfCapacity,  // tier full and eviction could not free space
    kInternal,
};

const char* StatusCodeName(StatusCode code);

// Lightweight, copyable status object. Empty (kOk) status is the common case.
class Status {
public:
    Status() = default;
    Status(StatusCode code, std::string message) : code_(code), message_(std::move(message)) {}

    static Status Ok() { return Status{}; }
    static Status NotImplemented(std::string msg = "not implemented") {
        return Status{StatusCode::kNotImplemented, std::move(msg)};
    }
    static Status NotFound(std::string msg = "not found") { return Status{StatusCode::kNotFound, std::move(msg)}; }
    static Status InvalidArgument(std::string msg) { return Status{StatusCode::kInvalidArgument, std::move(msg)}; }
    static Status Unavailable(std::string msg) { return Status{StatusCode::kUnavailable, std::move(msg)}; }
    static Status IoError(std::string msg) { return Status{StatusCode::kIoError, std::move(msg)}; }
    static Status NetworkError(std::string msg) { return Status{StatusCode::kNetworkError, std::move(msg)}; }
    static Status Internal(std::string msg) { return Status{StatusCode::kInternal, std::move(msg)}; }

    bool ok() const { return code_ == StatusCode::kOk; }
    StatusCode code() const { return code_; }
    const std::string& message() const { return message_; }

    std::string ToString() const;

private:
    StatusCode code_ = StatusCode::kOk;
    std::string message_;
};

// Result<T> bundles a value with a Status. On error, value() is undefined and
// status() carries the reason. Modelled loosely on absl::StatusOr.
//
// Why std::optional<T> (not absl's tagged union): absl uses a union to avoid
// requiring T to be default/movable and to save the optional's bookkeeping
// bool. At scaffold scale that does not matter and optional is far simpler. The
// one real footgun the union avoids is constructor ambiguity when T can be
// built from Status (or vice versa) — then `Result<T>(something)` is ambiguous
// between the value and status constructors. We guard that explicitly with the
// static_assert below; for our actual T's (Block, std::string/NodeId, ShardMap,
// Location, HitMap, BlockView...) none is convertible to/from Status, so the
// two implicit constructors are unambiguous and we keep them for terse call
// sites.
template <typename T>
class Result {
    static_assert(!std::is_same_v<T, Status>, "Result<Status> is ambiguous; return Status directly");
    static_assert(!std::is_convertible_v<Status, T> && !std::is_convertible_v<T, Status>,
                  "Result<T> where T converts to/from Status has ambiguous "
                  "constructors; wrap T or make its Status conversion explicit");

public:
    // Implicit construction from a value (success) or a Status (failure) keeps
    // call sites terse: `return block;` or `return Status::NotFound();`. Safe
    // to be implicit because of the static_asserts above (T never aliases
    // Status).
    Result(T value) : status_(Status::Ok()), value_(std::move(value)) {}
    Result(Status status) : status_(std::move(status)) {}

    bool ok() const { return status_.ok(); }
    const Status& status() const { return status_; }

    T& value() { return *value_; }
    const T& value() const { return *value_; }
    T value_or(T fallback) const { return ok() ? *value_ : std::move(fallback); }

private:
    Status status_;
    std::optional<T> value_;
};

}  // namespace tidepool
