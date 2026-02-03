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

# pg_lakehouse_iceberg - Iceberg Table Access Method
# CMake build configuration

cmake_minimum_required(VERSION 3.26)

project(pg_lakehouse_iceberg
    VERSION 0.1.0
    DESCRIPTION "Apache Iceberg Table Access Method for PostgreSQL"
    LANGUAGES C CXX
)

# Extension metadata
set(EXTENSION_NAME pg_lakehouse_iceberg)
set(EXTENSION_VERSION ${PROJECT_VERSION})

# ============================================================================
# Source files
# ============================================================================

# C sources (PostgreSQL extension entry point)
set(EXTENSION_C_SOURCES
    src/iceberg_am.c
)

# C++ sources (iceberg-cpp bridge)
set(EXTENSION_CXX_SOURCES
    src/iceberg_bridge.cpp
)

# ============================================================================
# Build shared library
# ============================================================================

# Combine C and C++ sources based on availability
set(EXTENSION_SOURCES ${EXTENSION_C_SOURCES})

if(ICEBERG_CPP_AVAILABLE)
    list(APPEND EXTENSION_SOURCES ${EXTENSION_CXX_SOURCES})
endif()

add_library(${EXTENSION_NAME} MODULE ${EXTENSION_SOURCES})

# Include PostgreSQL headers
target_include_directories(${EXTENSION_NAME} PRIVATE
    ${PG_INCLUDEDIR_SERVER}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# Compiler settings for C
target_compile_options(${EXTENSION_NAME} PRIVATE
    $<$<COMPILE_LANGUAGE:C>:-Wall -Wextra -Wno-unused-parameter>
    $<$<COMPILE_LANGUAGE:CXX>:-Wall -Wextra -Wno-unused-parameter>
)

# Language standards
set_target_properties(${EXTENSION_NAME} PROPERTIES
    C_STANDARD 17
    C_STANDARD_REQUIRED ON
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
    PREFIX ""  # No 'lib' prefix for PostgreSQL extensions
)

# Link against iceberg-cpp when available
if(ICEBERG_CPP_AVAILABLE)
    # iceberg-cpp with bundled Arrow (use _static target)
    target_link_libraries(${EXTENSION_NAME} PRIVATE
        iceberg_bundle_static
    )

    # Ensure iceberg-cpp is built before pg_lakehouse_iceberg
    add_dependencies(${EXTENSION_NAME} iceberg_bundle_static)

    # Include iceberg-cpp headers
    target_include_directories(${EXTENSION_NAME} PRIVATE
        ${CMAKE_SOURCE_DIR}/thirdparty/iceberg-cpp/src
    )

    target_compile_definitions(${EXTENSION_NAME} PRIVATE
        HAVE_ICEBERG_CPP
    )

    message(STATUS "pg_lakehouse_iceberg: linking with iceberg_bundle_static")
else()
    message(WARNING "pg_lakehouse_iceberg: iceberg-cpp not available, scan will return no data")
endif()

# ============================================================================
# Installation
# ============================================================================

# Install shared library
install(TARGETS ${EXTENSION_NAME}
    LIBRARY DESTINATION ${PG_PKGLIBDIR}
)

# Install control file
install(FILES ${EXTENSION_NAME}.control
    DESTINATION ${PG_SHAREDIR}/extension
)

# Install SQL files
install(FILES
    sql/${EXTENSION_NAME}--${EXTENSION_VERSION}.sql
    DESTINATION ${PG_SHAREDIR}/extension
)

# ============================================================================
# Regression tests
# ============================================================================

if(PG_LAKEHOUSE_BUILD_TESTS)
    # TODO: Add regression tests
    # add_test(NAME iceberg_regress
    #     COMMAND ${PG_BINDIR}/pg_regress
    #         --inputdir=${CMAKE_CURRENT_SOURCE_DIR}
    #         --bindir=${PG_BINDIR}
    #         iceberg_basic
    # )
endif()
