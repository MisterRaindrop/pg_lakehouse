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
 * iceberg_bridge.cpp
 *      C++ implementation of iceberg-cpp bridge
 *
 * This file provides the implementation of the C interface defined
 * in iceberg_bridge.h, using the iceberg-cpp library.
 */

#include "iceberg_bridge.h"

#ifdef HAVE_ICEBERG_CPP

#include <memory>
#include <string>
#include <vector>
#include <cstring>
#include <optional>
#include <unordered_map>

// iceberg-cpp headers
#include <iceberg/table.h>
#include <iceberg/table_scan.h>
#include <iceberg/schema.h>
#include <iceberg/type.h>
#include <iceberg/snapshot.h>
#include <iceberg/file_format.h>
#include <iceberg/file_reader.h>
#include <iceberg/file_io.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/catalog/memory/in_memory_catalog.h>
#include <iceberg/arrow/arrow_file_io.h>
#include <iceberg/parquet/parquet_register.h>
#include <iceberg/avro/avro_register.h>

// For ArrowArrayStream handling
#include <iceberg/arrow_c_data.h>

/*
 * Global state for the iceberg-cpp library
 */
static bool g_initialized = false;
static std::unique_ptr<iceberg::FileIO> g_file_io;

/*
 * Internal structures wrapping iceberg-cpp objects
 */
struct IcebergTableHandle {
    std::string location;
    std::string catalog_type;
    std::string catalog_uri;
    std::string metadata_location;

    // Iceberg objects
    std::shared_ptr<iceberg::Table> table;
    std::shared_ptr<iceberg::Catalog> catalog;
    std::shared_ptr<iceberg::FileIO> file_io;

    // Schema information (cached)
    std::shared_ptr<iceberg::Schema> schema;
    std::vector<std::string> column_names;
    std::vector<std::string> column_types;
};

struct IcebergScanHandle {
    IcebergTableHandle *table;
    int64_t snapshot_id;

    // Scan state
    std::unique_ptr<iceberg::TableScan> scan;
    std::vector<std::shared_ptr<iceberg::FileScanTask>> tasks;
    size_t current_task_index;

    // Current Arrow stream for reading
    ArrowArrayStream arrow_stream;
    ArrowSchema arrow_schema;
    ArrowArray current_batch;
    int64_t current_row_in_batch;
    bool has_current_batch;
    bool exhausted;

    // Projected schema
    std::shared_ptr<iceberg::Schema> projected_schema;
};

/*
 * Helper to set error message
 */
static void set_error(IcebergError *error, int code, const char *msg)
{
    if (error) {
        error->code = code;
        strncpy(error->message, msg, sizeof(error->message) - 1);
        error->message[sizeof(error->message) - 1] = '\0';
    }
}

/*
 * Helper to set error from iceberg::Error
 */
static void set_error_from_iceberg(IcebergError *error, const iceberg::Error &e)
{
    if (error) {
        error->code = static_cast<int>(e.kind);
        strncpy(error->message, e.message.c_str(), sizeof(error->message) - 1);
        error->message[sizeof(error->message) - 1] = '\0';
    }
}

/*
 * Convert iceberg TypeId to string
 */
static std::string type_id_to_string(iceberg::TypeId type_id)
{
    switch (type_id) {
        case iceberg::TypeId::kBoolean:   return "boolean";
        case iceberg::TypeId::kInt:       return "int";
        case iceberg::TypeId::kLong:      return "long";
        case iceberg::TypeId::kFloat:     return "float";
        case iceberg::TypeId::kDouble:    return "double";
        case iceberg::TypeId::kDecimal:   return "decimal";
        case iceberg::TypeId::kDate:      return "date";
        case iceberg::TypeId::kTime:      return "time";
        case iceberg::TypeId::kTimestamp: return "timestamp";
        case iceberg::TypeId::kTimestampTz: return "timestamptz";
        case iceberg::TypeId::kString:    return "string";
        case iceberg::TypeId::kBinary:    return "binary";
        case iceberg::TypeId::kFixed:     return "fixed";
        case iceberg::TypeId::kUuid:      return "uuid";
        case iceberg::TypeId::kStruct:    return "struct";
        case iceberg::TypeId::kList:      return "list";
        case iceberg::TypeId::kMap:       return "map";
        default:                          return "unknown";
    }
}

/*
 * Cache schema information from iceberg Table
 */
static void cache_schema_info(IcebergTableHandle *handle)
{
    if (!handle || !handle->schema) return;

    handle->column_names.clear();
    handle->column_types.clear();

    for (const auto &field : handle->schema->fields()) {
        handle->column_names.push_back(std::string(field.name()));
        handle->column_types.push_back(type_id_to_string(field.type()->type_id()));
    }
}

/*
 * Release ArrowArrayStream resources
 */
static void release_arrow_stream(IcebergScanHandle *scan)
{
    if (scan->has_current_batch && scan->current_batch.release) {
        scan->current_batch.release(&scan->current_batch);
        scan->has_current_batch = false;
    }
    if (scan->arrow_stream.release) {
        scan->arrow_stream.release(&scan->arrow_stream);
    }
    if (scan->arrow_schema.release) {
        scan->arrow_schema.release(&scan->arrow_schema);
    }
}

/*
 * Initialize the iceberg-cpp library
 */
extern "C" int
iceberg_bridge_init(IcebergError *error)
{
    if (g_initialized) {
        return 0;
    }

    try {
        // Register file format readers
        iceberg::parquet::RegisterAll();
        iceberg::avro::RegisterAll();

        // Create default file IO (local file system)
        g_file_io = iceberg::arrow::MakeLocalFileIO();

        g_initialized = true;
        return 0;

    } catch (const std::exception &e) {
        set_error(error, 1, e.what());
        return 1;
    }
}

/*
 * Cleanup the iceberg-cpp library
 */
extern "C" void
iceberg_bridge_shutdown(void)
{
    if (!g_initialized) return;

    g_file_io.reset();
    g_initialized = false;
}

/*
 * Open an Iceberg table
 */
extern "C" IcebergTableHandle *
iceberg_bridge_table_open(const char *location,
                   const char *catalog_type,
                   const char *catalog_uri,
                   IcebergError *error)
{
    if (!g_initialized) {
        set_error(error, 1, "iceberg-cpp not initialized");
        return nullptr;
    }

    try {
        auto handle = new IcebergTableHandle();
        handle->location = location ? location : "";
        handle->catalog_type = catalog_type ? catalog_type : "static";
        handle->catalog_uri = catalog_uri ? catalog_uri : "";

        // Create FileIO based on location scheme
        // For now, use local file IO. S3 support will be added later.
        handle->file_io = iceberg::arrow::MakeLocalFileIO();

        // Handle different catalog types
        if (handle->catalog_type == "static" || handle->catalog_type == "hadoop") {
            // Static table: location is the metadata.json file path
            // We use StaticTable::Make which loads directly from metadata file

            // First, read the table metadata
            auto read_result = handle->file_io->ReadFile(handle->location, std::nullopt);
            if (!read_result.has_value()) {
                set_error_from_iceberg(error, read_result.error());
                delete handle;
                return nullptr;
            }

            // Parse the metadata JSON (this is simplified - real impl needs json parsing)
            // For now, we'll use the in-memory catalog approach
            const std::unordered_map<std::string, std::string> properties;
            std::string warehouse_location = handle->location.substr(
                0, handle->location.rfind('/'));

            handle->catalog = iceberg::InMemoryCatalog::Make(
                "pg_lakehouse",
                std::move(handle->file_io),
                warehouse_location,
                properties);

            // Try to register and load the table
            auto table_id = iceberg::TableIdentifier{.ns = {}, .name = "imported_table"};
            auto register_result = handle->catalog->RegisterTable(
                table_id, handle->location);
            if (!register_result.has_value()) {
                set_error_from_iceberg(error, register_result.error());
                delete handle;
                return nullptr;
            }

            auto load_result = handle->catalog->LoadTable(table_id);
            if (!load_result.has_value()) {
                set_error_from_iceberg(error, load_result.error());
                delete handle;
                return nullptr;
            }

            handle->table = std::move(load_result.value());
        }
        else if (handle->catalog_type == "rest") {
            // REST catalog support - requires additional configuration
            set_error(error, 1, "REST catalog not yet implemented");
            delete handle;
            return nullptr;
        }
        else {
            set_error(error, 1, "Unknown catalog type");
            delete handle;
            return nullptr;
        }

        // Get and cache schema
        auto schema_result = handle->table->schema();
        if (!schema_result.has_value()) {
            set_error_from_iceberg(error, schema_result.error());
            delete handle;
            return nullptr;
        }
        handle->schema = schema_result.value();
        cache_schema_info(handle);

        return handle;

    } catch (const std::exception &e) {
        set_error(error, 1, e.what());
        return nullptr;
    }
}

/*
 * Close an Iceberg table handle
 */
extern "C" void
iceberg_bridge_table_close(IcebergTableHandle *table)
{
    delete table;
}

/*
 * Get table metadata
 */
extern "C" int
iceberg_bridge_table_get_info(IcebergTableHandle *table,
                       IcebergTableInfo *info,
                       IcebergError *error)
{
    if (!table || !info) {
        set_error(error, 1, "Invalid arguments");
        return 1;
    }

    try {
        info->location = strdup(table->location.c_str());

        auto snapshot_result = table->table->current_snapshot();
        if (snapshot_result.has_value()) {
            auto snapshot = snapshot_result.value();
            info->snapshot_id = snapshot->snapshot_id;

            // Get summary fields
            auto it = snapshot->summary.find(iceberg::SnapshotSummaryFields::kTotalRecords);
            if (it != snapshot->summary.end()) {
                info->total_records = std::stoll(it->second);
            } else {
                info->total_records = 0;
            }

            it = snapshot->summary.find(iceberg::SnapshotSummaryFields::kTotalDataFiles);
            if (it != snapshot->summary.end()) {
                info->total_files = std::stoi(it->second);
            } else {
                info->total_files = 0;
            }

            it = snapshot->summary.find(iceberg::SnapshotSummaryFields::kTotalFileSize);
            if (it != snapshot->summary.end()) {
                info->total_size_bytes = std::stoll(it->second);
            } else {
                info->total_size_bytes = 0;
            }
        } else {
            info->snapshot_id = 0;
            info->total_records = 0;
            info->total_files = 0;
            info->total_size_bytes = 0;
        }

        if (table->schema) {
            info->schema_id = table->schema->schema_id();
        } else {
            info->schema_id = 0;
        }

        return 0;

    } catch (const std::exception &e) {
        set_error(error, 1, e.what());
        return 1;
    }
}

/*
 * Begin a table scan
 */
extern "C" IcebergScanHandle *
iceberg_bridge_scan_begin(IcebergTableHandle *table,
                          int64_t snapshot_id,
                          const char **column_names,
                          int num_columns,
                          IcebergError *error)
{
    if (!table || !table->table) {
        set_error(error, 1, "Invalid table handle");
        return nullptr;
    }

    try {
        auto scan = new IcebergScanHandle();
        scan->table = table;
        scan->snapshot_id = snapshot_id;
        scan->current_task_index = 0;
        scan->current_row_in_batch = 0;
        scan->has_current_batch = false;
        scan->exhausted = false;
        std::memset(&scan->arrow_stream, 0, sizeof(scan->arrow_stream));
        std::memset(&scan->arrow_schema, 0, sizeof(scan->arrow_schema));
        std::memset(&scan->current_batch, 0, sizeof(scan->current_batch));

        // Create scan builder
        auto scan_builder_result = table->table->NewScan();
        if (!scan_builder_result.has_value()) {
            set_error_from_iceberg(error, scan_builder_result.error());
            delete scan;
            return nullptr;
        }

        auto &scan_builder = scan_builder_result.value();

        // Set snapshot if specified
        if (snapshot_id > 0) {
            scan_builder->UseSnapshot(snapshot_id);
        }

        // Select columns if specified
        if (column_names && num_columns > 0) {
            std::vector<std::string> cols;
            for (int i = 0; i < num_columns; i++) {
                cols.push_back(column_names[i]);
            }
            scan_builder->Select(cols);
        }

        // Build the scan
        auto scan_result = scan_builder->Build();
        if (!scan_result.has_value()) {
            set_error_from_iceberg(error, scan_result.error());
            delete scan;
            return nullptr;
        }
        scan->scan = std::move(scan_result.value());

        // Get projected schema
        auto schema_result = scan->scan->schema();
        if (schema_result.has_value()) {
            scan->projected_schema = schema_result.value();
        }

        // Plan files to get all scan tasks
        auto plan_result = scan->scan->PlanFiles();
        if (!plan_result.has_value()) {
            set_error_from_iceberg(error, plan_result.error());
            delete scan;
            return nullptr;
        }
        scan->tasks = std::move(plan_result.value());

        return scan;

    } catch (const std::exception &e) {
        set_error(error, 1, e.what());
        return nullptr;
    }
}

/*
 * Helper: Start reading from the next task
 */
static bool start_next_task(IcebergScanHandle *scan, IcebergError *error)
{
    // Release previous stream if any
    release_arrow_stream(scan);

    while (scan->current_task_index < scan->tasks.size()) {
        auto &task = scan->tasks[scan->current_task_index];

        // Get ArrowArrayStream from the task
        auto stream_result = task->ToArrow(
            scan->table->file_io,
            scan->projected_schema);

        if (!stream_result.has_value()) {
            scan->current_task_index++;
            continue;  // Try next task
        }

        scan->arrow_stream = std::move(stream_result.value());
        scan->current_task_index++;

        // Get schema from stream
        if (scan->arrow_stream.get_schema) {
            int ret = scan->arrow_stream.get_schema(&scan->arrow_stream, &scan->arrow_schema);
            if (ret != 0) {
                const char *err_msg = scan->arrow_stream.get_last_error(&scan->arrow_stream);
                set_error(error, ret, err_msg ? err_msg : "Failed to get schema");
                continue;
            }
        }

        return true;
    }

    scan->exhausted = true;
    return false;
}

/*
 * Helper: Read next batch from current stream
 */
static bool read_next_batch(IcebergScanHandle *scan, IcebergError *error)
{
    if (scan->has_current_batch && scan->current_batch.release) {
        scan->current_batch.release(&scan->current_batch);
        scan->has_current_batch = false;
    }

    if (!scan->arrow_stream.get_next) {
        return false;
    }

    std::memset(&scan->current_batch, 0, sizeof(scan->current_batch));
    int ret = scan->arrow_stream.get_next(&scan->arrow_stream, &scan->current_batch);

    if (ret != 0) {
        const char *err_msg = scan->arrow_stream.get_last_error(&scan->arrow_stream);
        set_error(error, ret, err_msg ? err_msg : "Failed to read batch");
        return false;
    }

    // Check if stream is exhausted (release is null when no more data)
    if (scan->current_batch.release == nullptr) {
        return false;
    }

    scan->has_current_batch = true;
    scan->current_row_in_batch = 0;
    return true;
}

/*
 * Helper: Convert Arrow value to IcebergValue
 */
static void convert_arrow_value(const ArrowArray *array,
                                const ArrowSchema *schema,
                                int64_t row,
                                IcebergValue *value)
{
    // Check for null
    if (array->null_count > 0 && array->buffers[0]) {
        const uint8_t *validity = static_cast<const uint8_t *>(array->buffers[0]);
        int64_t idx = row + array->offset;
        if (!(validity[idx / 8] & (1 << (idx % 8)))) {
            value->type = ICEBERG_TYPE_NULL;
            value->is_null = true;
            return;
        }
    }

    value->is_null = false;

    // Parse format string to determine type
    const char *format = schema->format;
    if (!format) {
        value->type = ICEBERG_TYPE_NULL;
        value->is_null = true;
        return;
    }

    int64_t idx = row + array->offset;

    if (strcmp(format, "b") == 0) {
        // Boolean
        const uint8_t *data = static_cast<const uint8_t *>(array->buffers[1]);
        value->type = ICEBERG_TYPE_BOOL;
        value->value.bool_val = (data[idx / 8] & (1 << (idx % 8))) != 0;
    }
    else if (strcmp(format, "i") == 0) {
        // Int32
        const int32_t *data = static_cast<const int32_t *>(array->buffers[1]);
        value->type = ICEBERG_TYPE_INT32;
        value->value.int32_val = data[idx];
    }
    else if (strcmp(format, "l") == 0) {
        // Int64
        const int64_t *data = static_cast<const int64_t *>(array->buffers[1]);
        value->type = ICEBERG_TYPE_INT64;
        value->value.int64_val = data[idx];
    }
    else if (strcmp(format, "f") == 0) {
        // Float
        const float *data = static_cast<const float *>(array->buffers[1]);
        value->type = ICEBERG_TYPE_FLOAT;
        value->value.float_val = data[idx];
    }
    else if (strcmp(format, "g") == 0) {
        // Double
        const double *data = static_cast<const double *>(array->buffers[1]);
        value->type = ICEBERG_TYPE_DOUBLE;
        value->value.double_val = data[idx];
    }
    else if (strcmp(format, "u") == 0 || strcmp(format, "U") == 0) {
        // String (utf8 or large_utf8)
        value->type = ICEBERG_TYPE_STRING;
        if (strcmp(format, "u") == 0) {
            const int32_t *offsets = static_cast<const int32_t *>(array->buffers[1]);
            const char *data = static_cast<const char *>(array->buffers[2]);
            value->value.string_val.data = data + offsets[idx];
            value->value.string_val.len = offsets[idx + 1] - offsets[idx];
        } else {
            const int64_t *offsets = static_cast<const int64_t *>(array->buffers[1]);
            const char *data = static_cast<const char *>(array->buffers[2]);
            value->value.string_val.data = data + offsets[idx];
            value->value.string_val.len = static_cast<int32_t>(offsets[idx + 1] - offsets[idx]);
        }
    }
    else if (strcmp(format, "z") == 0 || strcmp(format, "Z") == 0) {
        // Binary
        value->type = ICEBERG_TYPE_BINARY;
        if (strcmp(format, "z") == 0) {
            const int32_t *offsets = static_cast<const int32_t *>(array->buffers[1]);
            const char *data = static_cast<const char *>(array->buffers[2]);
            value->value.string_val.data = data + offsets[idx];
            value->value.string_val.len = offsets[idx + 1] - offsets[idx];
        } else {
            const int64_t *offsets = static_cast<const int64_t *>(array->buffers[1]);
            const char *data = static_cast<const char *>(array->buffers[2]);
            value->value.string_val.data = data + offsets[idx];
            value->value.string_val.len = static_cast<int32_t>(offsets[idx + 1] - offsets[idx]);
        }
    }
    else if (strcmp(format, "tdD") == 0) {
        // Date (days since epoch)
        const int32_t *data = static_cast<const int32_t *>(array->buffers[1]);
        value->type = ICEBERG_TYPE_DATE;
        value->value.date_val = data[idx];
    }
    else if (strncmp(format, "ts", 2) == 0) {
        // Timestamp
        const int64_t *data = static_cast<const int64_t *>(array->buffers[1]);
        value->type = ICEBERG_TYPE_TIMESTAMP;
        // Convert based on unit (u=microseconds, n=nanoseconds, s=seconds, m=milliseconds)
        char unit = format[2];
        switch (unit) {
            case 'u':  // microseconds
                value->value.timestamp_val = data[idx];
                break;
            case 'n':  // nanoseconds
                value->value.timestamp_val = data[idx] / 1000;
                break;
            case 'm':  // milliseconds
                value->value.timestamp_val = data[idx] * 1000;
                break;
            case 's':  // seconds
                value->value.timestamp_val = data[idx] * 1000000;
                break;
            default:
                value->value.timestamp_val = data[idx];
        }
    }
    else {
        // Unknown type - treat as null for now
        value->type = ICEBERG_TYPE_NULL;
        value->is_null = true;
    }
}

/*
 * Get next row from scan
 */
extern "C" bool
iceberg_bridge_scan_next(IcebergScanHandle *scan,
                         IcebergValue *values,
                         int num_cols,
                         IcebergError *error)
{
    if (!scan || !values) {
        set_error(error, 1, "Invalid arguments");
        return false;
    }

    if (scan->exhausted) {
        return false;
    }

    try {
        // Loop until we have a row or are exhausted
        while (true) {
            // Try to get a row from current batch
            if (scan->has_current_batch &&
                scan->current_row_in_batch < scan->current_batch.length) {

                // Convert each column
                for (int i = 0; i < num_cols && i < scan->current_batch.n_children; i++) {
                    ArrowArray *child_array = scan->current_batch.children[i];
                    ArrowSchema *child_schema = scan->arrow_schema.children[i];

                    convert_arrow_value(child_array, child_schema,
                                       scan->current_row_in_batch, &values[i]);
                }

                // Fill remaining columns with nulls
                for (int i = scan->current_batch.n_children; i < num_cols; i++) {
                    values[i].type = ICEBERG_TYPE_NULL;
                    values[i].is_null = true;
                }

                scan->current_row_in_batch++;
                return true;
            }

            // Need a new batch
            if (!read_next_batch(scan, error)) {
                // No more batches in this stream, try next task
                if (!start_next_task(scan, error)) {
                    // No more tasks
                    return false;
                }
                continue;
            }
        }

    } catch (const std::exception &e) {
        set_error(error, 1, e.what());
        scan->exhausted = true;
        return false;
    }
}

/*
 * End a table scan
 */
extern "C" void
iceberg_bridge_scan_end(IcebergScanHandle *scan)
{
    if (scan) {
        release_arrow_stream(scan);
        delete scan;
    }
}

/*
 * Get column count
 */
extern "C" int
iceberg_bridge_table_get_column_count(IcebergTableHandle *table)
{
    if (!table) return 0;
    return static_cast<int>(table->column_names.size());
}

/*
 * Get column name
 */
extern "C" const char *
iceberg_bridge_table_get_column_name(IcebergTableHandle *table, int column_index)
{
    if (!table || column_index < 0 ||
        column_index >= static_cast<int>(table->column_names.size())) {
        return nullptr;
    }
    return table->column_names[column_index].c_str();
}

/*
 * Get column type
 */
extern "C" const char *
iceberg_bridge_table_get_column_type(IcebergTableHandle *table, int column_index)
{
    if (!table || column_index < 0 ||
        column_index >= static_cast<int>(table->column_types.size())) {
        return nullptr;
    }
    return table->column_types[column_index].c_str();
}

/* ============================================================================
 * Parallel Scan Implementation
 * ============================================================================
 */

/*
 * Internal structure for parallel plan
 */
struct IcebergParallelPlan {
    int64_t snapshot_id;
    std::vector<IcebergFileInfo> files;
    std::vector<IcebergParallelTask> tasks;
    IcebergTableHandle *table;
};

/*
 * Helper to get file extension (lowercase)
 */
static std::string get_file_extension(const char *file_path)
{
    if (!file_path) return "";

    std::string path(file_path);
    size_t dot_pos = path.rfind('.');
    if (dot_pos == std::string::npos) return "";

    std::string ext = path.substr(dot_pos);
    for (auto &c : ext) c = std::tolower(c);
    return ext;
}

/*
 * Convert iceberg FileFormatType to our enum
 */
static IcebergFileFormat convert_file_format(iceberg::FileFormatType format)
{
    switch (format) {
        case iceberg::FileFormatType::kParquet:
            return ICEBERG_FORMAT_PARQUET;
        case iceberg::FileFormatType::kOrc:
            return ICEBERG_FORMAT_ORC;
        case iceberg::FileFormatType::kAvro:
            return ICEBERG_FORMAT_AVRO;
        default:
            return ICEBERG_FORMAT_UNKNOWN;
    }
}

/*
 * Detect file format from file path extension
 */
extern "C" IcebergFileFormat
iceberg_bridge_detect_file_format(const char *file_path)
{
    std::string ext = get_file_extension(file_path);

    if (ext == ".parquet" || ext == ".parq") {
        return ICEBERG_FORMAT_PARQUET;
    } else if (ext == ".orc") {
        return ICEBERG_FORMAT_ORC;
    } else if (ext == ".avro") {
        return ICEBERG_FORMAT_AVRO;
    }

    return ICEBERG_FORMAT_UNKNOWN;
}

/*
 * Check if format supports sub-file parallelism (row groups / stripes)
 */
extern "C" bool
iceberg_bridge_format_supports_chunks(IcebergFileFormat format)
{
    switch (format) {
        case ICEBERG_FORMAT_PARQUET:
        case ICEBERG_FORMAT_ORC:
            return true;
        case ICEBERG_FORMAT_AVRO:
        case ICEBERG_FORMAT_UNKNOWN:
        default:
            return false;
    }
}

/*
 * Get format name for logging
 */
extern "C" const char *
iceberg_bridge_format_name(IcebergFileFormat format)
{
    switch (format) {
        case ICEBERG_FORMAT_PARQUET: return "Parquet";
        case ICEBERG_FORMAT_ORC:     return "ORC";
        case ICEBERG_FORMAT_AVRO:    return "Avro";
        default:                     return "Unknown";
    }
}

/*
 * Get chunk count for a file based on its format
 *
 * Parquet: returns number of row groups
 * ORC: returns number of stripes
 * Avro: returns 1 (file-level only)
 */
extern "C" uint32_t
iceberg_bridge_get_file_chunk_count(const char *file_path,
                              IcebergFileFormat format,
                              IcebergError *error)
{
    if (!file_path) {
        set_error(error, 1, "NULL file path");
        return 0;
    }

    try {
        switch (format) {
            case ICEBERG_FORMAT_PARQUET:
            case ICEBERG_FORMAT_ORC:
                // TODO: Open the file and read metadata to get actual count
                // For now, return 1 (single chunk per file)
                // Real implementation would use Arrow/Parquet APIs:
                //
                // auto fs = arrow::fs::FileSystemFromUri(file_path);
                // auto file = fs->OpenInputFile(file_path);
                // auto reader = parquet::ParquetFileReader::Open(file);
                // return reader->metadata()->num_row_groups();
                return 1;

            case ICEBERG_FORMAT_AVRO:
                // Avro: always file-level parallelism
                return 1;

            default:
                set_error(error, 1, "Unknown file format");
                return 0;
        }
    } catch (const std::exception &e) {
        set_error(error, 1, e.what());
        return 0;
    }
}

/*
 * Create a parallel scan plan
 */
extern "C" IcebergParallelPlan *
iceberg_bridge_parallel_plan_create(IcebergTableHandle *table,
                              int64_t snapshot_id,
                              IcebergError *error)
{
    if (!table || !table->table) {
        set_error(error, 1, "NULL table handle");
        return nullptr;
    }

    try {
        auto plan = new IcebergParallelPlan();
        plan->snapshot_id = snapshot_id;
        plan->table = table;

        // Create a scan to get the file list
        auto scan_builder_result = table->table->NewScan();
        if (!scan_builder_result.has_value()) {
            set_error_from_iceberg(error, scan_builder_result.error());
            delete plan;
            return nullptr;
        }

        auto &scan_builder = scan_builder_result.value();
        if (snapshot_id > 0) {
            scan_builder->UseSnapshot(snapshot_id);
        }

        auto scan_result = scan_builder->Build();
        if (!scan_result.has_value()) {
            set_error_from_iceberg(error, scan_result.error());
            delete plan;
            return nullptr;
        }

        auto scan = std::move(scan_result.value());
        auto plan_result = scan->PlanFiles();
        if (!plan_result.has_value()) {
            set_error_from_iceberg(error, plan_result.error());
            delete plan;
            return nullptr;
        }

        auto tasks = std::move(plan_result.value());

        // Build file list and task list from scan tasks
        for (size_t i = 0; i < tasks.size(); i++) {
            auto &task = tasks[i];
            auto data_file = task->data_file();

            IcebergFileInfo file_info = {};
            strncpy(file_info.path, data_file->file_path.c_str(),
                    ICEBERG_MAX_PATH_LENGTH - 1);
            file_info.file_size = data_file->file_size_in_bytes;
            file_info.file_format = static_cast<uint8_t>(
                convert_file_format(data_file->file_format));

            // Get chunk count based on format
            IcebergFileFormat format = static_cast<IcebergFileFormat>(file_info.file_format);
            file_info.chunk_count = iceberg_bridge_get_file_chunk_count(
                file_info.path, format, error);
            if (file_info.chunk_count == 0) {
                file_info.chunk_count = 1;
            }

            plan->files.push_back(file_info);

            // Create tasks based on chunk count
            if (iceberg_bridge_format_supports_chunks(format) && file_info.chunk_count > 1) {
                for (uint32_t chunk = 0; chunk < file_info.chunk_count; chunk++) {
                    IcebergParallelTask task_info = {};
                    task_info.file_index = static_cast<uint32_t>(i);
                    task_info.chunk_index = chunk;
                    task_info.chunk_count = file_info.chunk_count;
                    task_info.file_format = file_info.file_format;
                    plan->tasks.push_back(task_info);
                }
            } else {
                // Single task for entire file
                IcebergParallelTask task_info = {};
                task_info.file_index = static_cast<uint32_t>(i);
                task_info.chunk_index = 0;
                task_info.chunk_count = 1;
                task_info.file_format = file_info.file_format;
                plan->tasks.push_back(task_info);
            }
        }

        return plan;

    } catch (const std::exception &e) {
        set_error(error, 1, e.what());
        return nullptr;
    }
}

/*
 * Get size needed for serialized parallel state
 */
extern "C" size_t
iceberg_bridge_parallel_plan_get_size(IcebergParallelPlan *plan)
{
    if (!plan) return 0;

    size_t size = sizeof(IcebergParallelState);
    size += plan->files.size() * sizeof(IcebergFileInfo);
    size += plan->tasks.size() * sizeof(IcebergParallelTask);

    return size;
}

/*
 * Serialize parallel plan to buffer
 */
extern "C" size_t
iceberg_bridge_parallel_plan_serialize(IcebergParallelPlan *plan,
                                 void *buffer,
                                 size_t size)
{
    if (!plan || !buffer) return 0;

    size_t needed = iceberg_bridge_parallel_plan_get_size(plan);
    if (size < needed) return 0;

    char *ptr = static_cast<char *>(buffer);

    // Write header
    IcebergParallelState *state = reinterpret_cast<IcebergParallelState *>(ptr);
    state->magic = ICEBERG_PARALLEL_MAGIC;
    state->snapshot_id = plan->snapshot_id;
    state->total_files = static_cast<uint32_t>(plan->files.size());
    state->total_tasks = static_cast<uint32_t>(plan->tasks.size());
    ptr += sizeof(IcebergParallelState);

    // Write file infos
    if (!plan->files.empty()) {
        memcpy(ptr, plan->files.data(),
               plan->files.size() * sizeof(IcebergFileInfo));
        ptr += plan->files.size() * sizeof(IcebergFileInfo);
    }

    // Write tasks
    if (!plan->tasks.empty()) {
        memcpy(ptr, plan->tasks.data(),
               plan->tasks.size() * sizeof(IcebergParallelTask));
        ptr += plan->tasks.size() * sizeof(IcebergParallelTask);
    }

    return static_cast<size_t>(ptr - static_cast<char *>(buffer));
}

/*
 * Free parallel plan
 */
extern "C" void
iceberg_bridge_parallel_plan_free(IcebergParallelPlan *plan)
{
    delete plan;
}

/*
 * Get file count from plan
 */
extern "C" uint32_t
iceberg_bridge_parallel_plan_get_file_count(IcebergParallelPlan *plan)
{
    return plan ? static_cast<uint32_t>(plan->files.size()) : 0;
}

/*
 * Get task count from plan
 */
extern "C" uint32_t
iceberg_bridge_parallel_plan_get_task_count(IcebergParallelPlan *plan)
{
    return plan ? static_cast<uint32_t>(plan->tasks.size()) : 0;
}

/*
 * Get file info from serialized state
 */
extern "C" const IcebergFileInfo *
iceberg_bridge_parallel_state_get_file(const IcebergParallelState *state,
                                 uint32_t file_index)
{
    if (!state || state->magic != ICEBERG_PARALLEL_MAGIC) return nullptr;
    if (file_index >= state->total_files) return nullptr;

    const char *ptr = reinterpret_cast<const char *>(state);
    ptr += sizeof(IcebergParallelState);

    const IcebergFileInfo *files = reinterpret_cast<const IcebergFileInfo *>(ptr);
    return &files[file_index];
}

/*
 * Get task from serialized state
 */
extern "C" const IcebergParallelTask *
iceberg_bridge_parallel_state_get_task(const IcebergParallelState *state,
                                 uint32_t task_index)
{
    if (!state || state->magic != ICEBERG_PARALLEL_MAGIC) return nullptr;
    if (task_index >= state->total_tasks) return nullptr;

    const char *ptr = reinterpret_cast<const char *>(state);
    ptr += sizeof(IcebergParallelState);
    ptr += state->total_files * sizeof(IcebergFileInfo);

    const IcebergParallelTask *tasks = reinterpret_cast<const IcebergParallelTask *>(ptr);
    return &tasks[task_index];
}

/*
 * Begin parallel scan for a specific task
 */
extern "C" IcebergScanHandle *
iceberg_bridge_parallel_scan_begin(IcebergTableHandle *table,
                             const IcebergParallelState *state,
                             uint32_t task_index,
                             IcebergError *error)
{
    if (!table || !state) {
        set_error(error, 1, "NULL table or state");
        return nullptr;
    }

    const IcebergParallelTask *task = iceberg_bridge_parallel_state_get_task(state, task_index);
    if (!task) {
        set_error(error, 1, "Invalid task index");
        return nullptr;
    }

    const IcebergFileInfo *file = iceberg_bridge_parallel_state_get_file(state, task->file_index);
    if (!file) {
        set_error(error, 1, "Invalid file index");
        return nullptr;
    }

    try {
        auto scan = new IcebergScanHandle();
        scan->table = table;
        scan->snapshot_id = state->snapshot_id;
        scan->current_task_index = 0;
        scan->current_row_in_batch = 0;
        scan->has_current_batch = false;
        scan->exhausted = false;
        std::memset(&scan->arrow_stream, 0, sizeof(scan->arrow_stream));
        std::memset(&scan->arrow_schema, 0, sizeof(scan->arrow_schema));
        std::memset(&scan->current_batch, 0, sizeof(scan->current_batch));

        // Get projected schema
        auto schema_result = table->table->schema();
        if (schema_result.has_value()) {
            scan->projected_schema = schema_result.value();
        }

        // Open the specific file and chunk
        IcebergFileFormat format = static_cast<IcebergFileFormat>(task->file_format);

        // Create reader options
        iceberg::ReaderOptions options;
        options.path = file->path;
        options.io = table->file_io;
        options.projection = scan->projected_schema;

        // Set split for chunk-level parallelism (if applicable)
        // TODO: Calculate actual split offset/length based on row group/stripe
        // For now, we read the entire file

        // Get the appropriate reader
        auto reader_result = iceberg::ReaderFactoryRegistry::Open(
            static_cast<iceberg::FileFormatType>(format), options);

        if (!reader_result.has_value()) {
            set_error_from_iceberg(error, reader_result.error());
            delete scan;
            return nullptr;
        }

        // We don't have a direct way to get ArrowArrayStream from Reader
        // For parallel scan, we'll use a different approach: read batches directly

        // For now, mark the scan as ready and let iceberg_scan_next handle reading
        return scan;

    } catch (const std::exception &e) {
        set_error(error, 1, e.what());
        return nullptr;
    }
}

#else /* !HAVE_ICEBERG_CPP */

/*
 * Stub implementations when iceberg-cpp is not available
 */

extern "C" int iceberg_bridge_init(IcebergError *error)
{
    if (error) {
        error->code = 1;
        strncpy(error->message, "iceberg-cpp not available", sizeof(error->message));
    }
    return 1;
}

extern "C" void iceberg_bridge_shutdown(void) {}

extern "C" IcebergTableHandle *
iceberg_bridge_table_open(const char *location,
                          const char *catalog_type,
                          const char *catalog_uri,
                          IcebergError *error)
{
    if (error) {
        error->code = 1;
        strncpy(error->message, "iceberg-cpp not available", sizeof(error->message));
    }
    return nullptr;
}

extern "C" void iceberg_bridge_table_close(IcebergTableHandle *table) {}

extern "C" int
iceberg_bridge_table_get_info(IcebergTableHandle *table,
                              IcebergTableInfo *info,
                              IcebergError *error)
{
    return 1;
}

extern "C" IcebergScanHandle *
iceberg_bridge_scan_begin(IcebergTableHandle *table,
                          int64_t snapshot_id,
                          const char **column_names,
                          int num_columns,
                          IcebergError *error)
{
    return nullptr;
}

extern "C" bool
iceberg_bridge_scan_next(IcebergScanHandle *scan,
                         IcebergValue *values,
                         int num_cols,
                         IcebergError *error)
{
    return false;
}

extern "C" void iceberg_bridge_scan_end(IcebergScanHandle *scan) {}

extern "C" int iceberg_bridge_table_get_column_count(IcebergTableHandle *table)
{
    return 0;
}

extern "C" const char *
iceberg_bridge_table_get_column_name(IcebergTableHandle *table, int column_index)
{
    return nullptr;
}

extern "C" const char *
iceberg_bridge_table_get_column_type(IcebergTableHandle *table, int column_index)
{
    return nullptr;
}

/* Parallel scan stubs */

extern "C" IcebergFileFormat
iceberg_bridge_detect_file_format(const char *file_path)
{
    return ICEBERG_FORMAT_UNKNOWN;
}

extern "C" bool
iceberg_bridge_format_supports_chunks(IcebergFileFormat format)
{
    return false;
}

extern "C" const char *
iceberg_bridge_format_name(IcebergFileFormat format)
{
    return "Unknown";
}

extern "C" uint32_t
iceberg_bridge_get_file_chunk_count(const char *file_path,
                                    IcebergFileFormat format,
                                    IcebergError *error)
{
    return 0;
}

extern "C" IcebergParallelPlan *
iceberg_bridge_parallel_plan_create(IcebergTableHandle *table,
                                    int64_t snapshot_id,
                                    IcebergError *error)
{
    return nullptr;
}

extern "C" size_t iceberg_bridge_parallel_plan_get_size(IcebergParallelPlan *plan)
{
    return 0;
}

extern "C" size_t
iceberg_bridge_parallel_plan_serialize(IcebergParallelPlan *plan,
                                       void *buffer,
                                       size_t size)
{
    return 0;
}

extern "C" void iceberg_bridge_parallel_plan_free(IcebergParallelPlan *plan) {}

extern "C" uint32_t iceberg_bridge_parallel_plan_get_file_count(IcebergParallelPlan *plan)
{
    return 0;
}

extern "C" uint32_t iceberg_bridge_parallel_plan_get_task_count(IcebergParallelPlan *plan)
{
    return 0;
}

extern "C" const IcebergFileInfo *
iceberg_bridge_parallel_state_get_file(const IcebergParallelState *state,
                                       uint32_t file_index)
{
    return nullptr;
}

extern "C" const IcebergParallelTask *
iceberg_bridge_parallel_state_get_task(const IcebergParallelState *state,
                                       uint32_t task_index)
{
    return nullptr;
}

extern "C" IcebergScanHandle *
iceberg_bridge_parallel_scan_begin(IcebergTableHandle *table,
                                   const IcebergParallelState *state,
                                   uint32_t task_index,
                                   IcebergError *error)
{
    return nullptr;
}

#endif /* HAVE_ICEBERG_CPP */
