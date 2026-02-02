# External Dependencies

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.

## Overview

This directory contains external dependencies as Git submodules.

## Submodules

| Submodule | Description | License |
|-----------|-------------|---------|
| `iceberg-cpp` | Apache Iceberg C++ implementation | Apache-2.0 |
| `arrow` | Apache Arrow - cross-language columnar data format | Apache-2.0 |

## Initialization

To initialize and update all submodules:

```bash
git submodule update --init --recursive
```

To update submodules to their latest versions:

```bash
git submodule update --remote --merge
```

## Build Requirements

### iceberg-cpp
- CMake 3.26+
- C++23 compliant compiler (GCC 14+ or Clang 18+)
- See: https://github.com/apache/iceberg-cpp

### arrow
- CMake 3.16+
- C++17 compliant compiler
- See: https://arrow.apache.org/docs/developers/cpp/building.html

## Notes

- These submodules are pinned to specific commits for reproducible builds
- The build system (CMakeLists.txt) handles building these dependencies
- For development, you may want to build iceberg-cpp with bundled Arrow support
