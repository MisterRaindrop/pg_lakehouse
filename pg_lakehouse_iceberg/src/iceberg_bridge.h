/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * iceberg_bridge.h
 *      C interface for iceberg-cpp functionality
 *
 * This header provides a C-compatible interface to call iceberg-cpp
 * from the PostgreSQL extension (which is written in C).
 */

#ifndef ICEBERG_BRIDGE_H
#define ICEBERG_BRIDGE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

/*
 * Opaque handle types for C interface
 */
typedef struct IcebergTableHandle IcebergTableHandle;
typedef struct IcebergScanHandle IcebergScanHandle;

/*
 * Column value union for passing data back to PostgreSQL
 */
typedef struct IcebergValue {
    enum {
        ICEBERG_NULL,
        ICEBERG_BOOL,
        ICEBERG_INT32,
        ICEBERG_INT64,
        ICEBERG_FLOAT,
        ICEBERG_DOUBLE,
        ICEBERG_STRING,
        ICEBERG_BINARY,
        ICEBERG_TIMESTAMP,
        ICEBERG_DATE,
        ICEBERG_DECIMAL
    } type;

    union {
        bool        bool_val;
        int32_t     int32_val;
        int64_t     int64_val;
        float       float_val;
        double      double_val;
        struct {
            const char *data;
            int32_t     len;
        } string_val;
        int64_t     timestamp_val;  /* microseconds since epoch */
        int32_t     date_val;       /* days since epoch */
    } value;

    bool is_null;
} IcebergValue;

/*
 * Table metadata structure
 */
typedef struct IcebergTableInfo {
    char       *location;
    int64_t     snapshot_id;
    int32_t     schema_id;
    int64_t     total_records;
    int32_t     total_files;
    int64_t     total_size_bytes;
} IcebergTableInfo;

/*
 * Error handling
 */
typedef struct IcebergError {
    int         code;
    char        message[1024];
} IcebergError;

/*
 * Initialize the iceberg-cpp library
 * Returns 0 on success, non-zero on failure
 */
int iceberg_init(IcebergError *error);

/*
 * Cleanup the iceberg-cpp library
 */
void iceberg_shutdown(void);

/*
 * Open an Iceberg table
 *
 * Parameters:
 *   location     - S3 path to table (e.g., "s3://bucket/warehouse/table")
 *   catalog_type - Type of catalog ("rest", "hive", "hadoop")
 *   catalog_uri  - URI of the catalog service (for REST catalog)
 *   error        - Error information if function fails
 *
 * Returns:
 *   Handle to table, or NULL on failure
 */
IcebergTableHandle *iceberg_table_open(
    const char *location,
    const char *catalog_type,
    const char *catalog_uri,
    IcebergError *error
);

/*
 * Close an Iceberg table handle
 */
void iceberg_table_close(IcebergTableHandle *table);

/*
 * Get table metadata
 */
int iceberg_table_get_info(
    IcebergTableHandle *table,
    IcebergTableInfo *info,
    IcebergError *error
);

/*
 * Begin a table scan
 *
 * Parameters:
 *   table        - Table handle
 *   snapshot_id  - Snapshot to read (0 for latest)
 *   column_names - Array of column names to read (NULL for all)
 *   num_columns  - Number of columns in array
 *   error        - Error information if function fails
 *
 * Returns:
 *   Scan handle, or NULL on failure
 */
IcebergScanHandle *iceberg_scan_begin(
    IcebergTableHandle *table,
    int64_t snapshot_id,
    const char **column_names,
    int num_columns,
    IcebergError *error
);

/*
 * Get next row from scan
 *
 * Parameters:
 *   scan    - Scan handle
 *   values  - Array to receive column values (must be pre-allocated)
 *   num_cols - Number of columns expected
 *   error   - Error information if function fails
 *
 * Returns:
 *   true if a row was returned, false if end of scan or error
 */
bool iceberg_scan_next(
    IcebergScanHandle *scan,
    IcebergValue *values,
    int num_cols,
    IcebergError *error
);

/*
 * End a table scan and release resources
 */
void iceberg_scan_end(IcebergScanHandle *scan);

/*
 * Get the number of columns in the table schema
 */
int iceberg_table_get_column_count(IcebergTableHandle *table);

/*
 * Get column name by index
 * Returns pointer to internal string, do not free
 */
const char *iceberg_table_get_column_name(
    IcebergTableHandle *table,
    int column_index
);

/*
 * Get column type by index
 * Returns Iceberg type string (e.g., "long", "string", "timestamp")
 */
const char *iceberg_table_get_column_type(
    IcebergTableHandle *table,
    int column_index
);

#ifdef __cplusplus
}
#endif

#endif /* ICEBERG_BRIDGE_H */
