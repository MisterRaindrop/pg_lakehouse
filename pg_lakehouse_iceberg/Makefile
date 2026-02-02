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
OBJS = src/iceberg_am.o

DATA = sql/pg_lakehouse_iceberg--$(EXTVERSION).sql

# Add C++ sources when iceberg-cpp integration is ready
# OBJS += src/iceberg_reader.o src/iceberg_writer.o

# Compiler flags
PG_CPPFLAGS = -I$(srcdir)/src
PG_CFLAGS = -Wall -Wextra -Werror

# For C++ files (when added)
# PG_CXXFLAGS = -std=c++23 -Wall -Wextra

# Include paths for iceberg-cpp and Arrow (when integrated)
# ICEBERG_CPP_DIR = ../thirdparty/iceberg-cpp
# ARROW_DIR = ../thirdparty/arrow
# PG_CPPFLAGS += -I$(ICEBERG_CPP_DIR)/src -I$(ARROW_DIR)/cpp/src

# Link flags for iceberg-cpp and Arrow (when integrated)
# SHLIB_LINK += -L$(ICEBERG_CPP_DIR)/build -liceberg
# SHLIB_LINK += -L$(ARROW_DIR)/cpp/build/release -larrow -lparquet

# Regression tests
REGRESS = iceberg_basic

# Find pg_config
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Custom targets
.PHONY: format lint

format:
	clang-format -i src/*.c src/*.h src/*.cpp 2>/dev/null || true

lint:
	cppcheck --enable=all --suppress=missingIncludeSystem src/
