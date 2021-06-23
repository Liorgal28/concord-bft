add_executable(client-pool-example client_pool_example.cpp)
target_link_libraries(client-pool-example
  ${boost_program_options_STATIC_LIBRARY}
  concord_external_client
  concord_proto
)
