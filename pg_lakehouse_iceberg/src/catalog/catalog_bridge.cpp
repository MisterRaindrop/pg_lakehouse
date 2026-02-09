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
 * catalog_bridge.cpp
 *      C ABI implementation for PgCatalog
 *
 * Wraps PgCatalog (C++) into C-callable functions with opaque handles.
 * Uses RAII and exception-safe wrappers for safe C++/C interop.
 *
 * Handle types (PgCatalogHandle, IcebergTransactionHandle, IcebergAppendHandle,
 * IcebergTableHandle) are defined in bridge/iceberg_bridge_internal.h to keep
 * a single source of truth shared across bridge/ and catalog/ layers.
 */

#include "catalog/catalog_bridge.h"
#include "catalog/pg_catalog.h"
#include "bridge/iceberg_bridge_internal.h"

#include <memory>
#include <cstring>

#include <iceberg/arrow/arrow_file_io.h>
#include <iceberg/schema.h>
#include <iceberg/transaction.h>
#include <iceberg/table.h>

/*
 * We use the macros from iceberg_bridge_internal.h:
 *   ICEBERG_BRIDGE_BEGIN(error)  /  ICEBERG_BRIDGE_END(error, fallback)
 * to guarantee exception-safe C ABI boundaries.
 */

/* ═══ Catalog lifecycle ═══ */

extern "C" PgCatalogHandle *
pg_catalog_create(const char *warehouse_location, IcebergError *error)
{
    ICEBERG_BRIDGE_BEGIN(error)

    auto file_io = iceberg::arrow::MakeLocalFileIO();
    std::string warehouse = warehouse_location ? warehouse_location : "";

    auto catalog_result = pg_lakehouse::PgCatalog::Make(
        std::move(file_io), std::move(warehouse));

    if (!catalog_result.has_value()) {
        set_iceberg_error(error, -1, catalog_result.error().message.c_str());
        return nullptr;
    }

    auto handle = new PgCatalogHandle();
    handle->catalog = std::move(catalog_result.value());
    return handle;

    ICEBERG_BRIDGE_END(error, nullptr)
}

extern "C" void
pg_catalog_destroy(PgCatalogHandle *catalog)
{
    delete catalog;
}

/* ═══ Table operations ═══ */

extern "C" int
pg_catalog_create_table(PgCatalogHandle *catalog,
                        const char *namespace_,
                        const char *table_name,
                        const char *schema_json,
                        const char *location,
                        IcebergError *error)
{
    if (!catalog || !namespace_ || !table_name) {
        set_iceberg_error(error, -1, "Invalid arguments");
        return -1;
    }

    ICEBERG_BRIDGE_BEGIN(error)

    iceberg::TableIdentifier id{
        .ns = {namespace_},
        .name = table_name
    };

    /* TODO: Parse schema_json into iceberg::Schema */
    auto schema = std::make_shared<iceberg::Schema>(std::vector<iceberg::SchemaField>{});

    auto spec = std::make_shared<iceberg::PartitionSpec>();
    auto order = std::make_shared<iceberg::SortOrder>();
    std::string loc = location ? location : "";
    std::unordered_map<std::string, std::string> props;

    auto result = catalog->catalog->CreateTable(id, schema, spec, order, loc, props);
    if (!result.has_value()) {
        set_iceberg_error(error, -1, result.error().message.c_str());
        return -1;
    }

    return 0;

    ICEBERG_BRIDGE_END(error, -1)
}

extern "C" IcebergTableHandle *
pg_catalog_load_table(PgCatalogHandle *catalog,
                      const char *namespace_,
                      const char *table_name,
                      IcebergError *error)
{
    if (!catalog || !namespace_ || !table_name) {
        set_iceberg_error(error, -1, "Invalid arguments");
        return nullptr;
    }

    ICEBERG_BRIDGE_BEGIN(error)

    iceberg::TableIdentifier id{
        .ns = {namespace_},
        .name = table_name
    };

    auto result = catalog->catalog->LoadTable(id);
    if (!result.has_value()) {
        set_iceberg_error(error, -1, result.error().message.c_str());
        return nullptr;
    }

    /*
     * Create an IcebergTableHandle from the loaded Table.
     * The handle type is defined in iceberg_bridge_internal.h
     * and is shared with the bridge layer.
     */
    auto handle = new IcebergTableHandle();
    handle->table = result.value();

    /* Populate metadata from the loaded table */
    auto loc_result = handle->table->location();
    if (loc_result.has_value()) {
        handle->location = std::string(loc_result.value());
    }

    auto schema_result = handle->table->schema();
    if (schema_result.has_value()) {
        handle->schema = schema_result.value();
        /* Cache column names and types */
        for (const auto &field : handle->schema->fields()) {
            handle->column_names.push_back(std::string(field.name()));
        }
    }

    return handle;

    ICEBERG_BRIDGE_END(error, nullptr)
}

extern "C" int
pg_catalog_drop_table(PgCatalogHandle *catalog,
                      const char *namespace_,
                      const char *table_name,
                      bool purge,
                      IcebergError *error)
{
    if (!catalog || !namespace_ || !table_name) {
        set_iceberg_error(error, -1, "Invalid arguments");
        return -1;
    }

    ICEBERG_BRIDGE_BEGIN(error)

    iceberg::TableIdentifier id{
        .ns = {namespace_},
        .name = table_name
    };

    auto status = catalog->catalog->DropTable(id, purge);
    if (!status.ok()) {
        set_iceberg_error(error, -1, status.message().c_str());
        return -1;
    }

    return 0;

    ICEBERG_BRIDGE_END(error, -1)
}

extern "C" int
pg_catalog_table_exists(PgCatalogHandle *catalog,
                        const char *namespace_,
                        const char *table_name,
                        IcebergError *error)
{
    if (!catalog || !namespace_ || !table_name) {
        set_iceberg_error(error, -1, "Invalid arguments");
        return -1;
    }

    ICEBERG_BRIDGE_BEGIN(error)

    iceberg::TableIdentifier id{
        .ns = {namespace_},
        .name = table_name
    };

    auto result = catalog->catalog->TableExists(id);
    if (!result.has_value()) {
        set_iceberg_error(error, -1, result.error().message.c_str());
        return -1;
    }

    return result.value() ? 1 : 0;

    ICEBERG_BRIDGE_END(error, -1)
}

extern "C" int
pg_catalog_rename_table(PgCatalogHandle *catalog,
                        const char *from_namespace,
                        const char *from_name,
                        const char *to_namespace,
                        const char *to_name,
                        IcebergError *error)
{
    if (!catalog) {
        set_iceberg_error(error, -1, "Invalid arguments");
        return -1;
    }

    ICEBERG_BRIDGE_BEGIN(error)

    iceberg::TableIdentifier from_id{
        .ns = {from_namespace ? from_namespace : "public"},
        .name = from_name ? from_name : ""
    };
    iceberg::TableIdentifier to_id{
        .ns = {to_namespace ? to_namespace : "public"},
        .name = to_name ? to_name : ""
    };

    auto status = catalog->catalog->RenameTable(from_id, to_id);
    if (!status.ok()) {
        set_iceberg_error(error, -1, status.message().c_str());
        return -1;
    }

    return 0;

    ICEBERG_BRIDGE_END(error, -1)
}

/* ═══ Transaction operations ═══ */

extern "C" IcebergTransactionHandle *
pg_catalog_new_transaction(IcebergTableHandle *table, IcebergError *error)
{
    if (!table || !table->table) {
        set_iceberg_error(error, -1, "Invalid table handle");
        return nullptr;
    }

    ICEBERG_BRIDGE_BEGIN(error)

    auto txn_result = table->table->NewTransaction();
    if (!txn_result.has_value()) {
        set_iceberg_error(error, -1, txn_result.error().message.c_str());
        return nullptr;
    }

    auto handle = new IcebergTransactionHandle();
    handle->transaction = std::move(txn_result.value());
    handle->table = table;
    return handle;

    ICEBERG_BRIDGE_END(error, nullptr)
}

extern "C" int
pg_catalog_transaction_commit(IcebergTransactionHandle *txn, IcebergError *error)
{
    if (!txn || !txn->transaction) {
        set_iceberg_error(error, -1, "Invalid transaction handle");
        return -1;
    }

    ICEBERG_BRIDGE_BEGIN(error)

    auto result = txn->transaction->Commit();
    if (!result.has_value()) {
        set_iceberg_error(error, -1, result.error().message.c_str());
        return -1;
    }

    return 0;

    ICEBERG_BRIDGE_END(error, -1)
}

extern "C" void
pg_catalog_transaction_abort(IcebergTransactionHandle *txn)
{
    /* RAII handles cleanup — just delete the handle */
    delete txn;
}

/* ═══ FastAppend operations ═══ */

extern "C" IcebergAppendHandle *
pg_catalog_new_fast_append(IcebergTransactionHandle *txn, IcebergError *error)
{
    if (!txn || !txn->transaction) {
        set_iceberg_error(error, -1, "Invalid transaction handle");
        return nullptr;
    }

    ICEBERG_BRIDGE_BEGIN(error)

    auto append_result = txn->transaction->NewFastAppend();
    if (!append_result.has_value()) {
        set_iceberg_error(error, -1, append_result.error().message.c_str());
        return nullptr;
    }

    auto handle = new IcebergAppendHandle();
    handle->txn = txn;
    /* TODO: Store FastAppend action */
    return handle;

    ICEBERG_BRIDGE_END(error, nullptr)
}

extern "C" int
pg_catalog_append_data_file(IcebergAppendHandle *append,
                            const char *file_path,
                            int64_t file_size,
                            int64_t record_count,
                            IcebergError *error)
{
    if (!append || !file_path) {
        set_iceberg_error(error, -1, "Invalid arguments");
        return -1;
    }

    ICEBERG_BRIDGE_BEGIN(error)

    /*
     * TODO: Build DataFile and append to FastAppend action
     *
     * iceberg::DataFile data_file;
     * data_file.file_path = file_path;
     * data_file.file_format = iceberg::FileFormatType::kParquet;
     * data_file.file_size_in_bytes = file_size;
     * data_file.record_count = record_count;
     * append->fast_append->AppendFile(std::move(data_file));
     */

    set_iceberg_error(error, -1, "append_data_file not yet fully implemented");
    return -1;

    ICEBERG_BRIDGE_END(error, -1)
}

extern "C" int
pg_catalog_append_commit(IcebergAppendHandle *append, IcebergError *error)
{
    if (!append) {
        set_iceberg_error(error, -1, "Invalid append handle");
        return -1;
    }

    ICEBERG_BRIDGE_BEGIN(error)

    /*
     * TODO: Commit the FastAppend action
     *
     * auto status = append->fast_append->Commit();
     * if (!status.ok()) { ... }
     */

    set_iceberg_error(error, -1, "append_commit not yet fully implemented");
    return -1;

    ICEBERG_BRIDGE_END(error, -1)
}
