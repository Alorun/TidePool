// factory.h — Construction helpers for concrete Transports. Plane: DATA.
// Keeps the TCP/RDMA impl headers private to src/ while letting apps/tests pick
// a transport at runtime.
#pragma once

#include <memory>

#include "tidepool/transport/transport.h"

namespace tidepool {

std::unique_ptr<Transport> MakeTcpTransport();
std::unique_ptr<Transport> MakeRdmaTransport();  // stub transport

}  // namespace tidepool
