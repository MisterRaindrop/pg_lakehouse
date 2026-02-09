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
# PGXS-based Makefile for standalone builds

EXTENSION = pg_lakehouse_iceberg
EXTVERSION = 0.1.0

MODULE_big = pg_lakehouse_iceberg

# ── C sources ──
OBJS = \
	src/iceberg_am.o \
	src/handler.o \
	src/operations/table_ops.o \
	src/operations/scan_ops.o \
	src/operations/write_ops.o \
	src/operations/commit_ops.o \
	src/io/file_io.o \
	src/io/s3_io.o

# ── C++ sources (when iceberg-cpp is available) ──
ifdef HAVE_ICEBERG_CPP
OBJS += \
	src/bridge/iceberg_bridge.o \
	src/catalog/pg_catalog.o \
	src/catalog/catalog_bridge.o
endif

DATA = sql/pg_lakehouse_iceberg--$(EXTVERSION).sql

# Compiler flags
PG_CPPFLAGS = -I$(srcdir)/src
PG_CFLAGS = -Wall -Wextra -Wno-unused-parameter

# C++ compiler flags (when iceberg-cpp is integrated)
ifdef HAVE_ICEBERG_CPP
PG_CXXFLAGS = -std=c++23 -Wall -Wextra -Wno-unused-parameter
PG_CPPFLAGS += -DHAVE_ICEBERG_CPP
endif

# Include paths for iceberg-cpp and Arrow (when integrated)
ifdef ICEBERG_CPP_DIR
PG_CPPFLAGS += -I$(ICEBERG_CPP_DIR)/src
endif

ifdef ARROW_DIR
PG_CPPFLAGS += -I$(ARROW_DIR)/cpp/src
endif

# Link flags for iceberg-cpp and Arrow (when integrated)
ifdef ICEBERG_CPP_DIR
SHLIB_LINK += -L$(ICEBERG_CPP_DIR)/build -liceberg
endif

ifdef ARROW_DIR
SHLIB_LINK += -L$(ARROW_DIR)/cpp/build/release -larrow -lparquet
endif

# Regression tests
REGRESS = iceberg_basic

# Find pg_config
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Custom targets
.PHONY: format lint

format:
	find src -name '*.c' -o -name '*.h' -o -name '*.cpp' | xargs clang-format -i 2>/dev/null || true

lint:
	cppcheck --enable=all --suppress=missingIncludeSystem src/
