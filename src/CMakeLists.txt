set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -o3 -fpermissive -g   -fstack-protector-all -static-libstdc++ -static-libgcc  -Wl,--wrap=memcpy  -Wl,--wrap=aligned_alloc  -Wl,--wrap=clock_gettime ")
#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -static -v ")
#set(CMAKE_EXE_LINKER_FLAGS "-static -pthread -lrt -ldl")
include(FindProtobuf)

if (NOT PROTOBUF_PROTOC_EXECUTABLE)
    get_filename_component(PROTO_LIB_DIR ${PROTOBUF_LIBRARY} DIRECTORY)
    set (PROTOBUF_PROTOC_EXECUTABLE "${PROTO_LIB_DIR}/../bin/protoc")
endif()

protobuf_generate_cpp(PROTO_SRC PROTO_HEADER raft.proto storage.proto)

include_directories(${CMAKE_CURRENT_BINARY_DIR} ${PROTOBUF_INCLUDE_DIRS}
        ${PROJECT_SOURCE_DIR}/third_party/json11
        ${PROJECT_SOURCE_DIR}/third_party/libco
        ${PROJECT_SOURCE_DIR}/third_party/pb2json/byReflection
        ${PROJECT_SOURCE_DIR}/third_party/leveldb/include/)

set(SRC  Config.cpp Network.cpp RaftMachine.cpp Storage.cpp Log.cpp Log.h Common.cpp Common.h Utils.cpp Utils.h  Storage_rocksdb.cpp KVServer.cpp )
link_directories(${PROJECT_SOURCE_DIR}/static_lib/)
add_executable(main main.cpp ${SRC} ${PROTO_SRC} ${PROTO_HEADER})
target_link_libraries(main colib_static json11  pb2json libprotobuf.a libleveldb.a librocksdb.a libz.a libbz2.a libsnappy.a pthread dl rt libm.a  libc.a)

#add_executable(Test Test.cpp Utils.cpp  ${PROTO_SRC} ${PROTO_HEADER} Storage.cpp Storage_leveldb.cpp)
#target_link_libraries(Test colib_static json11  pb2json protobuf leveldb)

add_executable(Client Client.cpp Network.cpp ${PROTO_SRC} ${PROTO_HEADER})
target_link_libraries(Client colib_static json11 pb2json libprotobuf.a rt pthread dl)

add_executable(KVClient KVClient.cpp Network.cpp ${PROTO_SRC} ${PROTO_HEADER})
target_link_libraries(KVClient colib_static json11 pb2json protobuf pthread dl)

add_executable(dataTool dataTool.cpp ${SRC} ${PROTO_SRC} ${PROTO_HEADER})
target_link_libraries(dataTool colib_static json11  pb2json libprotobuf.a libleveldb.a librocksdb.a rt pthread dl)