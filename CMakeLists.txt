# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.26)

project(pg_lakehouse
    VERSION 0.1.0
    DESCRIPTION "PostgreSQL Lakehouse Extension - Native Iceberg/Delta/Hudi support"
    LANGUAGES C CXX
)

# ============================================================================
# Build Options
# ============================================================================
option(PG_LAKEHOUSE_BUILD_TESTS "Build tests" ON)
option(PG_LAKEHOUSE_BUILD_ICEBERG "Build Iceberg support" ON)
option(PG_LAKEHOUSE_USE_BUNDLED_ARROW "Use bundled Arrow from iceberg-cpp" ON)
option(PG_LAKEHOUSE_ENABLE_S3 "Enable S3/MinIO support" ON)

# ============================================================================
# C++ Standard Requirements
# ============================================================================
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)

# ============================================================================
# Compiler Settings
# ============================================================================
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

# Compiler warnings (applied only to our code, not thirdparty)
# Note: We set these per-target instead of globally to avoid affecting thirdparty code

# ============================================================================
# Find PostgreSQL
# ============================================================================
find_program(PG_CONFIG pg_config REQUIRED)

execute_process(
    COMMAND ${PG_CONFIG} --includedir-server
    OUTPUT_VARIABLE PG_INCLUDEDIR_SERVER
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
execute_process(
    COMMAND ${PG_CONFIG} --pkglibdir
    OUTPUT_VARIABLE PG_PKGLIBDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
execute_process(
    COMMAND ${PG_CONFIG} --sharedir
    OUTPUT_VARIABLE PG_SHAREDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
execute_process(
    COMMAND ${PG_CONFIG} --libdir
    OUTPUT_VARIABLE PG_LIBDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
execute_process(
    COMMAND ${PG_CONFIG} --version
    OUTPUT_VARIABLE PG_VERSION_STRING
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

message(STATUS "PostgreSQL version: ${PG_VERSION_STRING}")
message(STATUS "PostgreSQL include: ${PG_INCLUDEDIR_SERVER}")
message(STATUS "PostgreSQL pkglib:  ${PG_PKGLIBDIR}")
message(STATUS "PostgreSQL share:   ${PG_SHAREDIR}")

# ============================================================================
# External Dependencies
# ============================================================================

# iceberg-cpp (includes Arrow support when built with ICEBERG_BUILD_BUNDLE)
if(PG_LAKEHOUSE_BUILD_ICEBERG AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/iceberg-cpp/CMakeLists.txt")
    message(STATUS "Building iceberg-cpp from thirdparty/")

    set(ICEBERG_BUILD_STATIC ON CACHE BOOL "" FORCE)
    set(ICEBERG_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(ICEBERG_BUILD_BUNDLE ON CACHE BOOL "" FORCE)
    set(ICEBERG_BUILD_TESTS OFF CACHE BOOL "" FORCE)

    # Disable warnings-as-errors for thirdparty code (iceberg-cpp sets CMAKE_COMPILE_WARNING_AS_ERROR)
    set(CMAKE_COMPILE_WARNING_AS_ERROR OFF CACHE BOOL "" FORCE)

    add_subdirectory(thirdparty/iceberg-cpp EXCLUDE_FROM_ALL)

    # iceberg-cpp's own top-level CMakeLists.txt force-enables warnings-as-errors via
    # `set(CMAKE_COMPILE_WARNING_AS_ERROR ON)`, which can break the build on newer
    # compilers (e.g. GCC 14) due to thirdparty warnings being promoted to errors.
    # Keep warnings-as-errors for our code, but disable it for iceberg-cpp targets.
    foreach(_tgt IN ITEMS
        iceberg_static iceberg_shared
        iceberg_bundle_static iceberg_bundle_shared
        iceberg_rest_static iceberg_rest_shared
    )
        if(TARGET ${_tgt})
            set_property(TARGET ${_tgt} PROPERTY COMPILE_WARNING_AS_ERROR OFF)
        endif()
    endforeach()
    set(ICEBERG_CPP_AVAILABLE TRUE)
else()
    message(STATUS "iceberg-cpp not found in thirdparty/, will try find_package")
    find_package(IcebergCpp QUIET)
    if(IcebergCpp_FOUND)
        set(ICEBERG_CPP_AVAILABLE TRUE)
    else()
        set(ICEBERG_CPP_AVAILABLE FALSE)
        message(WARNING "iceberg-cpp not available. Run: git submodule update --init --recursive")
    endif()
endif()

# Arrow (if not using bundled from iceberg-cpp)
if(NOT PG_LAKEHOUSE_USE_BUNDLED_ARROW)
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/arrow/cpp/CMakeLists.txt")
        message(STATUS "Building Arrow from thirdparty/")
        set(ARROW_BUILD_STATIC ON CACHE BOOL "" FORCE)
        set(ARROW_BUILD_SHARED OFF CACHE BOOL "" FORCE)
        set(ARROW_PARQUET ON CACHE BOOL "" FORCE)
        set(ARROW_FILESYSTEM ON CACHE BOOL "" FORCE)
        set(ARROW_S3 ${PG_LAKEHOUSE_ENABLE_S3} CACHE BOOL "" FORCE)
        set(ARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
        add_subdirectory(thirdparty/arrow/cpp EXCLUDE_FROM_ALL)
        set(ARROW_AVAILABLE TRUE)
    else()
        find_package(Arrow QUIET)
        find_package(Parquet QUIET)
        if(Arrow_FOUND AND Parquet_FOUND)
            set(ARROW_AVAILABLE TRUE)
        else()
            set(ARROW_AVAILABLE FALSE)
        endif()
    endif()
else()
    # Using bundled Arrow from iceberg-cpp
    set(ARROW_AVAILABLE ${ICEBERG_CPP_AVAILABLE})
endif()

# ============================================================================
# Installation Directories
# ============================================================================
set(PG_EXTENSION_DIR "${PG_SHAREDIR}/extension")
set(PG_MODULE_DIR "${PG_PKGLIBDIR}")

# ============================================================================
# Sub-extensions
# ============================================================================

# pg_lakehouse_core - shared utilities and catalog integration (planned)
# add_subdirectory(pg_lakehouse_core)

# pg_lakehouse_iceberg - Iceberg Table Access Method
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/pg_lakehouse_iceberg/CMakeLists.txt")
    message(STATUS "Adding pg_lakehouse_iceberg extension")
    add_subdirectory(pg_lakehouse_iceberg)
endif()

# ============================================================================
# Status Summary
# ============================================================================
message(STATUS "")
message(STATUS "pg_lakehouse ${PROJECT_VERSION} Configuration Summary:")
message(STATUS "  Build type:        ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ compiler:      ${CMAKE_CXX_COMPILER}")
message(STATUS "  C++ standard:      ${CMAKE_CXX_STANDARD}")
message(STATUS "  PostgreSQL:        ${PG_VERSION_STRING}")
message(STATUS "  iceberg-cpp:       ${ICEBERG_CPP_AVAILABLE}")
message(STATUS "  Arrow:             ${ARROW_AVAILABLE}")
message(STATUS "  S3 support:        ${PG_LAKEHOUSE_ENABLE_S3}")
message(STATUS "  Build tests:       ${PG_LAKEHOUSE_BUILD_TESTS}")
message(STATUS "")
