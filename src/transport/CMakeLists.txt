# Transfer Engine implementations. Each concrete transport is its own library
# behind the shared tidepool::transport abstract base class.
add_subdirectory(tcp)
add_subdirectory(rdma)
