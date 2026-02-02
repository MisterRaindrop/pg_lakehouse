# pg_lakehouse

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

PostgreSQL Lakehouse Extension - Native Apache Iceberg support for PostgreSQL.

## Overview

pg_lakehouse is a PostgreSQL extension that enables native lakehouse capabilities by integrating Apache Iceberg (and future support for Delta Lake, Apache Hudi) directly into PostgreSQL through a custom Table Access Method (AM).

### Key Features

- **Native Table Access Method**: Use Iceberg tables like regular PostgreSQL tables
- **ACID Transactions**: Full transactional support across lakehouse and local tables
- **S3-Compatible Storage**: Works with MinIO, AWS S3, and other S3-compatible stores
- **High Performance**: C++ implementation with zero JVM overhead
- **Catalog Integration**: PostgreSQL-native metadata management

### Example Usage

```sql
-- Create an Iceberg-backed table
CREATE TABLE orders (
    id BIGINT,
    customer TEXT,
    amount DECIMAL,
    ts TIMESTAMP
) USING iceberg_am
WITH (
    location = 's3://bucket/warehouse/orders/',
    catalog_type = 'rest',
    uri = 'http://catalog:8181'
);

-- Use it like a regular PostgreSQL table
INSERT INTO orders VALUES (1, 'Alice', 100.50, NOW());
SELECT * FROM orders WHERE ts > '2026-01-01';

-- Join with local tables
SELECT o.*, c.email
FROM orders o
JOIN local_customers c ON o.customer = c.name;
```

## Requirements

- PostgreSQL 16+
- CMake 3.26+
- C++23 compliant compiler (GCC 14+ or Clang 18+)
- Ninja (recommended)

## Quick Start

### Using Docker (Recommended)

```bash
# Clone the repository
git clone https://github.com/your-org/pg_lakehouse.git
cd pg_lakehouse

# Initialize submodules
git submodule update --init --recursive

# Start development environment
make docker-build
make docker-up

# Open shell in container
make docker-shell

# Build inside container
make build
```

### Manual Build

```bash
# Clone and initialize
git clone https://github.com/your-org/pg_lakehouse.git
cd pg_lakehouse
git submodule update --init --recursive

# Configure and build
cmake -S . -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local
cmake --build build

# Install
sudo cmake --install build
```

## Development

### VS Code Dev Container

This project includes Dev Container configuration for VS Code:

1. Install the "Dev Containers" extension
2. Open the project in VS Code
3. Click "Reopen in Container" when prompted

### Project Structure

```
pg_lakehouse/
├── .devcontainer/          # VS Code Dev Container config
├── docker/                 # Docker development environment
│   ├── Dockerfile.dev
│   ├── docker-compose.yml
│   └── docker-entrypoint.sh
├── thirdparty/             # Git submodules (third-party dependencies)
│   ├── iceberg-cpp/        # Apache Iceberg C++
│   └── arrow/              # Apache Arrow (optional)
├── pg_lakehouse_core/      # Core utilities (planned)
├── pg_lakehouse_iceberg/   # Iceberg AM implementation (planned)
├── CMakeLists.txt
├── Makefile
└── README.md
```

### Available Make Targets

```bash
make help          # Show all available targets
make build         # Build the project
make test          # Run tests
make docker-up     # Start Docker environment
make docker-shell  # Open shell in container
make format        # Format code
make lint          # Run linters
```

### Services

The Docker environment includes:

| Service | Port | Description |
|---------|------|-------------|
| PostgreSQL | 5432 | PostgreSQL 16 with extension |
| MinIO S3 | 9000 | S3-compatible object storage |
| MinIO Console | 9001 | MinIO web interface |

Default MinIO credentials: `minioadmin` / `minioadmin`

## Roadmap

- [x] **v0.1**: Project setup, build infrastructure
- [ ] **v0.2**: Basic Iceberg read support (scan)
- [ ] **v0.3**: Write support (INSERT)
- [ ] **v0.4**: UPDATE/DELETE support
- [ ] **v0.5**: Schema evolution
- [ ] **v1.0**: Production-ready Iceberg support
- [ ] **v2.0**: Multi-format support (Delta, Hudi)

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

```bash
# Install pre-commit hooks
pip install pre-commit
pre-commit install

# Run linters
pre-commit run --all-files
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Acknowledgments

- [Apache Iceberg](https://iceberg.apache.org/) - Table format
- [Apache Arrow](https://arrow.apache.org/) - Columnar data format
- [iceberg-cpp](https://github.com/apache/iceberg-cpp) - C++ Iceberg implementation
- [PostgreSQL](https://www.postgresql.org/) - The world's most advanced open source database
