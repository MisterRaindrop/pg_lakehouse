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

# This is an alternative standalone Dockerfile for VS Code Dev Containers
# Use docker-compose.yml for the full development environment with MinIO

FROM ubuntu:24.04

LABEL maintainer="pg_lakehouse developers"
LABEL description="Development environment for pg_lakehouse"

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC
ENV PG_MAJOR=16
ENV CC=gcc-14
ENV CXX=g++-14

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash-completion \
    build-essential \
    bison \
    ccache \
    clang-format \
    clangd \
    cmake \
    curl \
    flex \
    gcc-14 \
    g++-14 \
    git \
    gnupg2 \
    htop \
    libboost-all-dev \
    libcurl4-openssl-dev \
    libedit-dev \
    libicu-dev \
    liblz4-dev \
    libpam0g-dev \
    libreadline-dev \
    libselinux1-dev \
    libssl-dev \
    libsystemd-dev \
    libxml2-dev \
    libxslt1-dev \
    libzstd-dev \
    lsb-release \
    meson \
    ninja-build \
    pkg-config \
    python3 \
    python3-pip \
    sudo \
    uuid-dev \
    vim \
    wget \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Install PostgreSQL 16
RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    postgresql-${PG_MAJOR} \
    postgresql-client-${PG_MAJOR} \
    postgresql-server-dev-${PG_MAJOR} \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/usr/lib/postgresql/${PG_MAJOR}/bin:${PATH}"
ENV PG_CONFIG="/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config"

# Install Python tools
RUN pip3 install --break-system-packages pre-commit cmake-format

# Create development user
RUN useradd -ms /bin/bash lakehouse \
    && usermod -aG sudo lakehouse \
    && echo "lakehouse ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/lakehouse \
    && chmod 0440 /etc/sudoers.d/lakehouse

USER lakehouse
WORKDIR /workspace

CMD ["bash"]
