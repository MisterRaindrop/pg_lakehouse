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
 * iceberg_bridge_internal.h
 *      Internal C++ types for the iceberg-cpp bridge layer.
 *
 * This header is ONLY included by C++ files (.cpp).
 * It defines the concrete structs behind the opaque C handles
 * declared in iceberg_bridge.h and catalog_bridge.h.
 */

#ifndef ICEBERG_BRIDGE_INTERNAL_H
#define ICEBERG_BRIDGE_INTERNAL_H

#include "bridge/iceberg_bridge.h"

#include <memory>
#include <string>
#include <vector>

#include <iceberg/catalog.h>
#include <iceberg/file_io.h>
#include <iceberg/schema.h>
#include <iceberg/table.h>
#include <iceberg/table_scan.h>
#include <iceberg/transaction.h>

// Arrow C Data Interface
#include <iceberg/arrow_c_data.h>

/* ============================================================================
 * ABI version check
 * ============================================================================
 */
#define ICEBERG_BRIDGE_ABI_VERSION 1

/* ============================================================================
 * Internal handle structs
 * ============================================================================
 */

/*
 * Internal table handle  —  backing the opaque IcebergTableHandle
 */
struct IcebergTableHandle {
    std::string location;
    std::string catalog_type;
    std::string catalog_uri;
    std::string metadata_location;

    /* iceberg-cpp objects */
    std::shared_ptr<iceberg::Table>   table;
    std::shared_ptr<iceberg::Catalog> catalog;
    std::shared_ptr<iceberg::FileIO>  file_io;

    /* Cached schema info */
    std::shared_ptr<iceberg::Schema>  schema;
    std::vector<std::string>          column_names;
    std::vector<std::string>          column_types;
};

/*
 * Internal scan handle  —  backing the opaque IcebergScanHandle
 */
struct IcebergScanHandle {
    IcebergTableHandle *table;
    int64_t             snapshot_id;

    /* Scan state */
    std::unique_ptr<iceberg::TableScan>                  scan;
    std::vector<std::shared_ptr<iceberg::FileScanTask>>  tasks;
    size_t                current_task_index;

    /* Current Arrow stream for reading */
    ArrowArrayStream      arrow_stream;
    ArrowSchema           arrow_schema;
    ArrowArray            current_batch;
    int64_t               current_row_in_batch;
    bool                  has_current_batch;
    bool                  exhausted;

    /* Projected schema */
    std::shared_ptr<iceberg::Schema> projected_schema;
};

/*
 * Internal writer handle  —  backing the opaque IcebergWriterHandle
 */
struct IcebergWriterHandle {
    IcebergTableHandle               *table;
    std::vector<std::string>          column_names;
    std::string                       output_path;
    int64_t                           record_count;
    int64_t                           file_size;
    bool                              finished;

    /* TODO: Arrow/Parquet writer state */
};

/*
 * Catalog-related handles  —  used by catalog_bridge.cpp
 */
struct PgCatalogHandle {
    std::shared_ptr<iceberg::Catalog> catalog;
};

struct IcebergTransactionHandle {
    std::shared_ptr<iceberg::Transaction> transaction;
    IcebergTableHandle                   *table;  /* back pointer for reference */
};

struct IcebergAppendHandle {
    IcebergTransactionHandle *txn;
    /* TODO: FastAppend action state */
};

/* ============================================================================
 * Parallel plan internal struct
 * ============================================================================
 */
struct IcebergParallelPlan {
    int64_t                           snapshot_id;
    std::vector<IcebergFileInfo>      files;
    std::vector<IcebergParallelTask>  tasks;
    IcebergTableHandle               *table;
};

/* ============================================================================
 * Error helpers
 * ============================================================================
 */

static inline void set_iceberg_error(IcebergError *error, int code, const char *msg)
{
    if (error) {
        error->code = code;
        strncpy(error->message, msg, sizeof(error->message) - 1);
        error->message[sizeof(error->message) - 1] = '\0';
    }
}

/*
 * Macro for exception-safe C ABI entry points.
 * Wraps body in try/catch, sets IcebergError on exception.
 *
 * Usage:
 *   ICEBERG_BRIDGE_BEGIN(error)
 *       ... C++ code ...
 *       return result;
 *   ICEBERG_BRIDGE_END(error, nullptr)   // fallback return on exception
 */
#define ICEBERG_BRIDGE_BEGIN(err) \
    try {

#define ICEBERG_BRIDGE_END(err, fallback) \
    } catch (const std::exception &_e) { \
        set_iceberg_error((err), 1, _e.what()); \
        return (fallback); \
    } catch (...) { \
        set_iceberg_error((err), 1, "unknown C++ exception"); \
        return (fallback); \
    }

#endif /* ICEBERG_BRIDGE_INTERNAL_H */
