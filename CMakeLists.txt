cmake_minimum_required(VERSION 3.0.0)
project(nodepp-mongodb VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(NODEPP_MONGODB_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

# Search for Nodepp Core
if (NOT TARGET nodepp)
    find_package(nodepp REQUIRED)
    if(NOT nodepp_FOUND)
        message(FATAL_ERROR "Nodepp not found. Please install nodepp.")
    endif()
endif()

# 1. Search for MongoDB C Driver
find_package(MongoC REQUIRED) 
if(NOT MongoC_FOUND)
    message(FATAL_ERROR "MongoDB C Driver (MongoC) not found. Please install the MongoDB C driver package.")
endif()


# interface-target
add_library(nodepp-mongodb INTERFACE)

# Combine all necessary include directories
target_include_directories(nodepp-mongodb INTERFACE
    ${NODEPP_MONGODB_INCLUDE_DIR}
    ${MongoC_INCLUDE_DIRS}
)

# Link against all dependencies
target_link_libraries(nodepp-mongodb INTERFACE
    nodepp 
    ${MongoC_LIBRARIES}
)