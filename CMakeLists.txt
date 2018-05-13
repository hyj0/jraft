
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -g -fstack-protector-all")

add_subdirectory(third_party/libco)
add_subdirectory(third_party/json11)

add_library(pb2json ${PROJECT_SOURCE_DIR}/third_party/pb2json/byReflection/pb2json.cpp)
message(STATUS "CXX_FLAGS=${CMAKE_CXX_FLAGS}")

add_subdirectory(src)
