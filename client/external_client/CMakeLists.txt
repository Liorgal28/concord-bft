project("Concord Client Pool")

# Require C++17.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

file(GLOB concord_external_client_sources "*.cpp" "*.hpp"
        "../../threshsign/src/*.cpp")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/submodules)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(../../threshsign/include/)
include_directories(../../bftengine/include/bcstatetransfer/)
add_library(concord_external_client STATIC
        ${concord_external_client_sources}
        )
get_property(thresh_include GLOBAL PROPERTY thresh_include_folder)
target_include_directories(concord_external_client PRIVATE ${thresh_include})
target_include_directories(concord_external_client PUBLIC ${perf_include})
target_include_directories(concord_external_client PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../../bftengine/cmf)

target_link_libraries(concord_external_client PUBLIC
        ${Boost_LIBRARIES}
        ${yaml-cpp_LIBRARIES}
        logging
        bftclient
        bftclient_new
        kvbc
        util
        threshsign
        secretsmanager)

