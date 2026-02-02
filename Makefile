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

# pg_lakehouse - PostgreSQL Lakehouse Extension
# Top-level Makefile for development convenience

.PHONY: all build clean install test docker-build docker-up docker-down \
        docker-shell submodules format lint help

# Build configuration
BUILD_DIR := build
CMAKE_GENERATOR := Ninja
CMAKE_BUILD_TYPE ?= Release
INSTALL_PREFIX ?= /usr/local

# Docker configuration
DOCKER_COMPOSE := docker compose -f docker/docker-compose.yml

# Default target
all: build

# ============================================================================
# Submodules
# ============================================================================
submodules:
	@echo "Initializing git submodules..."
	git submodule update --init --recursive

submodules-update:
	@echo "Updating git submodules..."
	git submodule update --remote --merge

# ============================================================================
# Build Targets
# ============================================================================
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

configure: $(BUILD_DIR) submodules
	cmake -S . -B $(BUILD_DIR) \
		-G $(CMAKE_GENERATOR) \
		-DCMAKE_BUILD_TYPE=$(CMAKE_BUILD_TYPE) \
		-DCMAKE_INSTALL_PREFIX=$(INSTALL_PREFIX) \
		-DCMAKE_EXPORT_COMPILE_COMMANDS=ON

build: configure
	cmake --build $(BUILD_DIR) --parallel

install: build
	cmake --install $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR)

rebuild: clean build

# ============================================================================
# Testing
# ============================================================================
test: build
	ctest --test-dir $(BUILD_DIR) --output-on-failure

# ============================================================================
# Docker Development Environment
# ============================================================================
docker-build:
	$(DOCKER_COMPOSE) build

docker-up:
	$(DOCKER_COMPOSE) up -d

docker-down:
	$(DOCKER_COMPOSE) down

docker-logs:
	$(DOCKER_COMPOSE) logs -f

docker-shell:
	$(DOCKER_COMPOSE) exec dev bash

docker-clean:
	$(DOCKER_COMPOSE) down -v --rmi local

docker-restart: docker-down docker-up

# ============================================================================
# Development Tools
# ============================================================================
format:
	@echo "Formatting C/C++ code..."
	find . -name "*.c" -o -name "*.cpp" -o -name "*.h" -o -name "*.hpp" | \
		grep -v extern | grep -v build | \
		xargs clang-format -i

lint:
	@echo "Running pre-commit hooks..."
	pre-commit run --all-files

# ============================================================================
# Help
# ============================================================================
help:
	@echo "pg_lakehouse - PostgreSQL Lakehouse Extension"
	@echo ""
	@echo "Build targets:"
	@echo "  all          - Build the project (default)"
	@echo "  build        - Build with CMake/Ninja"
	@echo "  install      - Install to system"
	@echo "  clean        - Remove build directory"
	@echo "  rebuild      - Clean and rebuild"
	@echo "  test         - Run tests"
	@echo ""
	@echo "Submodules:"
	@echo "  submodules        - Initialize git submodules"
	@echo "  submodules-update - Update submodules to latest"
	@echo ""
	@echo "Docker targets:"
	@echo "  docker-build   - Build Docker images"
	@echo "  docker-up      - Start development environment"
	@echo "  docker-down    - Stop development environment"
	@echo "  docker-shell   - Open shell in dev container"
	@echo "  docker-logs    - Follow container logs"
	@echo "  docker-clean   - Remove containers and volumes"
	@echo "  docker-restart - Restart containers"
	@echo ""
	@echo "Development:"
	@echo "  format - Format C/C++ code with clang-format"
	@echo "  lint   - Run pre-commit hooks"
	@echo ""
	@echo "Configuration:"
	@echo "  CMAKE_BUILD_TYPE  - Debug/Release (default: Release)"
	@echo "  INSTALL_PREFIX    - Installation prefix (default: /usr/local)"
