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
#include <stddef.h>

/*
 * Opaque handle types for C interface
 */
typedef struct IcebergTableHandle IcebergTableHandle;
typedef struct IcebergScanHandle IcebergScanHandle;

/*
 * Iceberg value type enum (defined outside struct for C++ compatibility)
 */
typedef enum IcebergValueType {
    ICEBERG_TYPE_NULL,
    ICEBERG_TYPE_BOOL,
    ICEBERG_TYPE_INT32,
    ICEBERG_TYPE_INT64,
    ICEBERG_TYPE_FLOAT,
    ICEBERG_TYPE_DOUBLE,
    ICEBERG_TYPE_STRING,
    ICEBERG_TYPE_BINARY,
    ICEBERG_TYPE_TIMESTAMP,
    ICEBERG_TYPE_DATE,
    ICEBERG_TYPE_DECIMAL
} IcebergValueType;

/*
 * Column value union for passing data back to PostgreSQL
 */
typedef struct IcebergValue {
    IcebergValueType type;

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
int iceberg_bridge_init(IcebergError *error);

/*
 * Cleanup the iceberg-cpp library
 */
void iceberg_bridge_shutdown(void);

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
IcebergTableHandle *iceberg_bridge_table_open(
    const char *location,
    const char *catalog_type,
    const char *catalog_uri,
    IcebergError *error
);

/*
 * Close an Iceberg table handle
 */
void iceberg_bridge_table_close(IcebergTableHandle *table);

/*
 * Get table metadata
 */
int iceberg_bridge_table_get_info(
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
IcebergScanHandle *iceberg_bridge_scan_begin(
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
bool iceberg_bridge_scan_next(
    IcebergScanHandle *scan,
    IcebergValue *values,
    int num_cols,
    IcebergError *error
);

/*
 * End a table scan and release resources
 */
void iceberg_bridge_scan_end(IcebergScanHandle *scan);

/*
 * Get the number of columns in the table schema
 */
int iceberg_bridge_table_get_column_count(IcebergTableHandle *table);

/*
 * Get column name by index
 * Returns pointer to internal string, do not free
 */
const char *iceberg_bridge_table_get_column_name(
    IcebergTableHandle *table,
    int column_index
);

/*
 * Get column type by index
 * Returns Iceberg type string (e.g., "long", "string", "timestamp")
 */
const char *iceberg_bridge_table_get_column_type(
    IcebergTableHandle *table,
    int column_index
);

/* ============================================================================
 * Parallel Scan Support
 * ============================================================================
 */

#define ICEBERG_PARALLEL_MAGIC 0x49434250  /* "ICBP" */
#define ICEBERG_MAX_PATH_LENGTH 1024

/*
 * Iceberg file formats
 * Each format has different chunking strategies for parallel reads
 */
typedef enum IcebergFileFormat {
    ICEBERG_FORMAT_UNKNOWN = 0,
    ICEBERG_FORMAT_PARQUET = 1,  /* Parallel by row group */
    ICEBERG_FORMAT_ORC     = 2,  /* Parallel by stripe */
    ICEBERG_FORMAT_AVRO    = 3   /* Parallel by file (blocks too small) */
} IcebergFileFormat;

/*
 * Parallel scan task - represents one unit of work
 *
 * For Parquet: one row group
 * For ORC: one stripe
 * For Avro: one file (no sub-file parallelism)
 */
typedef struct IcebergParallelTask {
    uint32_t    file_index;         /* Index into file list */
    uint32_t    chunk_index;        /* Row group (Parquet) / Stripe (ORC) / 0 (Avro) */
    uint32_t    chunk_count;        /* Total chunks in file (1 for Avro) */
    uint8_t     file_format;        /* IcebergFileFormat */
    uint8_t     padding[3];         /* Alignment padding */
} IcebergParallelTask;

/*
 * File metadata for parallel scan
 */
typedef struct IcebergFileInfo {
    char        path[ICEBERG_MAX_PATH_LENGTH];
    int64_t     file_size;
    uint32_t    chunk_count;        /* Row groups (Parquet) / Stripes (ORC) / 1 (Avro) */
    uint8_t     file_format;        /* IcebergFileFormat */
    uint8_t     padding[3];         /* Alignment padding */
} IcebergFileInfo;

/*
 * Parallel scan shared state (stored in PostgreSQL DSM)
 * This is the C-compatible layout that will be copied to shared memory
 */
typedef struct IcebergParallelState {
    uint32_t    magic;              /* Validation magic number */
    int64_t     snapshot_id;        /* Iceberg snapshot being read */
    uint32_t    total_files;        /* Number of data files */
    uint32_t    total_tasks;        /* Total number of tasks */
    /* Variable length data follows:
     * - IcebergFileInfo files[total_files]
     * - IcebergParallelTask tasks[total_tasks]
     */
} IcebergParallelState;

/*
 * Opaque handle for parallel scan planning
 */
typedef struct IcebergParallelPlan IcebergParallelPlan;

/*
 * Create a parallel scan plan
 * Analyzes all files and creates task list
 *
 * Returns: Plan handle, or NULL on failure
 */
IcebergParallelPlan *iceberg_bridge_parallel_plan_create(
    IcebergTableHandle *table,
    int64_t snapshot_id,
    IcebergError *error
);

/*
 * Get the size needed for parallel state in shared memory
 */
size_t iceberg_bridge_parallel_plan_get_size(IcebergParallelPlan *plan);

/*
 * Serialize parallel plan to shared memory buffer
 *
 * Parameters:
 *   plan   - The parallel plan
 *   buffer - Pre-allocated buffer (size from get_size)
 *   size   - Size of buffer
 *
 * Returns: Actual bytes written, or 0 on failure
 */
size_t iceberg_bridge_parallel_plan_serialize(
    IcebergParallelPlan *plan,
    void *buffer,
    size_t size
);

/*
 * Free a parallel plan
 */
void iceberg_bridge_parallel_plan_free(IcebergParallelPlan *plan);

/*
 * Get number of files in plan
 */
uint32_t iceberg_bridge_parallel_plan_get_file_count(IcebergParallelPlan *plan);

/*
 * Get number of tasks in plan
 */
uint32_t iceberg_bridge_parallel_plan_get_task_count(IcebergParallelPlan *plan);

/*
 * Get file info from serialized state
 */
const IcebergFileInfo *iceberg_bridge_parallel_state_get_file(
    const IcebergParallelState *state,
    uint32_t file_index
);

/*
 * Get task from serialized state
 */
const IcebergParallelTask *iceberg_bridge_parallel_state_get_task(
    const IcebergParallelState *state,
    uint32_t task_index
);

/*
 * Begin a parallel scan for a specific task
 *
 * Parameters:
 *   table      - Table handle
 *   state      - Shared parallel state
 *   task_index - Index of task to execute
 *   error      - Error information if function fails
 *
 * Returns: Scan handle for this task, or NULL on failure
 */
IcebergScanHandle *iceberg_bridge_parallel_scan_begin(
    IcebergTableHandle *table,
    const IcebergParallelState *state,
    uint32_t task_index,
    IcebergError *error
);

/*
 * Detect file format from file path extension
 */
IcebergFileFormat iceberg_bridge_detect_file_format(const char *file_path);

/*
 * Get chunk count for a file based on its format
 *
 * Parquet: returns number of row groups
 * ORC: returns number of stripes
 * Avro: returns 1 (file-level only)
 *
 * Returns 0 on error
 */
uint32_t iceberg_bridge_get_file_chunk_count(
    const char *file_path,
    IcebergFileFormat format,
    IcebergError *error
);

/*
 * Check if format supports sub-file parallelism
 */
bool iceberg_bridge_format_supports_chunks(IcebergFileFormat format);

/*
 * Get format name for logging
 */
const char *iceberg_bridge_format_name(IcebergFileFormat format);

#ifdef __cplusplus
}
#endif

#endif /* ICEBERG_BRIDGE_H */
