#include "tidepool/api/status.h"

namespace tidepool {

const char* StatusCodeName(StatusCode code) {
    switch (code) {
        case StatusCode::kOk:
            return "Ok";
        case StatusCode::kNotImplemented:
            return "NotImplemented";
        case StatusCode::kNotFound:
            return "NotFound";
        case StatusCode::kInvalidArgument:
            return "InvalidArgument";
        case StatusCode::kAlreadyExists:
            return "AlreadyExists";
        case StatusCode::kIoError:
            return "IoError";
        case StatusCode::kNetworkError:
            return "NetworkError";
        case StatusCode::kUnavailable:
            return "Unavailable";
        case StatusCode::kOutOfCapacity:
            return "OutOfCapacity";
        case StatusCode::kInternal:
            return "Internal";
    }
    return "Unknown";
}

std::string Status::ToString() const {
    std::string s = StatusCodeName(code_);
    if (!message_.empty()) {
        s += ": ";
        s += message_;
    }
    return s;
}

}  // namespace tidepool
