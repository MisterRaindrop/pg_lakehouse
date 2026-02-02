# pg_lakehouse_iceberg

Apache Iceberg Table Access Method for PostgreSQL.

## Overview

This extension provides a custom Table Access Method (AM) that allows PostgreSQL
to read and write Apache Iceberg tables stored on S3-compatible object storage.

## Features (Planned)

- [x] TableAM skeleton implementation
- [ ] Basic table scan (SELECT)
- [ ] INSERT support
- [ ] UPDATE/DELETE support (merge-on-read)
- [ ] Snapshot isolation
- [ ] Schema evolution
- [ ] Partition pruning

## Installation

### Using PGXS (Standalone)

```bash
cd pg_lakehouse_iceberg
make
sudo make install
```

### Using CMake (with parent project)

```bash
cd pg_lakehouse
cmake -S . -B build -G Ninja
cmake --build build
sudo cmake --install build
```

## Usage

```sql
-- Load the extension
CREATE EXTENSION pg_lakehouse_iceberg;

-- Create an Iceberg table
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

-- Query the table
SELECT * FROM orders WHERE ts > '2026-01-01';

-- Insert data
INSERT INTO orders VALUES (1, 'Alice', 100.50, NOW());
```

## Development Status

This extension is currently in early development (MVP phase).

### Week 1-2: TableAM Skeleton (Current)
- Basic extension structure
- Empty TableAmRoutine callbacks
- Extension loading and registration

### Week 3-5: Read Support
- iceberg-cpp integration
- Parquet file scanning
- Arrow to PostgreSQL type conversion

### Week 6-8: Write Support
- INSERT implementation
- Parquet file writing
- Iceberg metadata updates

## Architecture

```
+-------------------+
|   PostgreSQL      |
|   Executor        |
+--------+----------+
         |
         v
+--------+----------+
|  TableAmRoutine   |
|  (iceberg_am.c)   |
+--------+----------+
         |
         v
+--------+----------+
|  iceberg-cpp      |
|  (C++ bridge)     |
+--------+----------+
         |
         v
+--------+----------+
|  Apache Arrow     |
|  (Parquet I/O)    |
+--------+----------+
         |
         v
+--------+----------+
|  Object Storage   |
|  (S3/MinIO)       |
+-------------------+
```

## License

Licensed under the Apache License, Version 2.0.
