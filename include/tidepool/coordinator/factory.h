// factory.h — Construction helper for the control-plane Coordinator.
// Plane: CONTROL. Keeps the concrete impl header private to src/.
#pragma once

#include <memory>

#include "tidepool/coordinator/coordinator.h"

namespace tidepool {

// MVP embedded single-node coordinator. A Raft/etcd-backed coordinator would
// add a sibling factory here behind the same Coordinator ABC.
std::shared_ptr<Coordinator> MakeSingleNodeCoordinator();

}  // namespace tidepool
