#include "tidepool/api/location.h"

namespace tidepool {

const char* TierTypeName(TierType t) {
    switch (t) {
        case TierType::kGpu:
            return "gpu";
        case TierType::kDram:
            return "dram";
        case TierType::kSsd:
            return "ssd";
    }
    return "unknown";
}

}  // namespace tidepool
