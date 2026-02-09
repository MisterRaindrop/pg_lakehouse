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
 * pg_catalog.cpp
 *      PostgreSQL internal Iceberg catalog implementation
 *
 * Uses SPI to interact with pg_lakehouse_catalog.lakehouse_tables
 * and iceberg-cpp's FileIO for S3/local metadata file operations.
 */

#include "catalog/pg_catalog.h"

#include <cstring>
#include <sstream>

#include <iceberg/schema.h>
#include <iceberg/partition_spec.h>
#include <iceberg/sort_order.h>
#include <iceberg/table_metadata.h>
#include <iceberg/table_metadata_builder.h>
#include <iceberg/transaction.h>

extern "C" {
#include "postgres.h"
#include "executor/spi.h"
#include "utils/builtins.h"
}

namespace pg_lakehouse {

// ═══════════════════════════════════════════════════════════
// Construction / Destruction
// ═══════════════════════════════════════════════════════════

PgCatalog::PgCatalog(std::shared_ptr<iceberg::FileIO> file_io,
                     std::string warehouse_location)
    : file_io_(std::move(file_io)),
      warehouse_location_(std::move(warehouse_location)) {}

PgCatalog::~PgCatalog() = default;

iceberg::Result<std::shared_ptr<PgCatalog>> PgCatalog::Make(
    std::shared_ptr<iceberg::FileIO> file_io,
    std::string warehouse_location)
{
    if (!file_io) {
        return iceberg::unexpected(
            iceberg::Error{iceberg::ErrorKind::kInvalidArgument,
                           "FileIO must not be null"});
    }

    auto catalog = std::shared_ptr<PgCatalog>(
        new PgCatalog(std::move(file_io), std::move(warehouse_location)));
    return catalog;
}

// ═══════════════════════════════════════════════════════════
// Namespace operations (PG Schema = Iceberg Namespace)
// ═══════════════════════════════════════════════════════════

iceberg::Status PgCatalog::CreateNamespace(
    const iceberg::Namespace& /*ns*/,
    const std::unordered_map<std::string, std::string>& /*properties*/)
{
    return iceberg::NotSupported("Use 'CREATE SCHEMA' for namespace management");
}

iceberg::Result<std::vector<iceberg::Namespace>> PgCatalog::ListNamespaces(
    const iceberg::Namespace& /*ns*/) const
{
    /*
     * TODO: Query pg_namespace to list schemas
     *
     * SELECT nspname FROM pg_namespace
     * WHERE nspname NOT IN ('pg_catalog', 'information_schema', ...)
     */
    return std::vector<iceberg::Namespace>{};
}

iceberg::Result<std::unordered_map<std::string, std::string>>
PgCatalog::GetNamespaceProperties(const iceberg::Namespace& /*ns*/) const
{
    return std::unordered_map<std::string, std::string>{};
}

iceberg::Status PgCatalog::DropNamespace(const iceberg::Namespace& /*ns*/)
{
    return iceberg::NotSupported("Use 'DROP SCHEMA' for namespace management");
}

iceberg::Result<bool> PgCatalog::NamespaceExists(
    const iceberg::Namespace& /*ns*/) const
{
    /* Assume namespace exists; PG will report error if it doesn't */
    return true;
}

iceberg::Status PgCatalog::UpdateNamespaceProperties(
    const iceberg::Namespace& /*ns*/,
    const std::unordered_map<std::string, std::string>& /*updates*/,
    const std::unordered_set<std::string>& /*removals*/)
{
    return iceberg::NotSupported("Namespace properties not supported in PG catalog");
}

// ═══════════════════════════════════════════════════════════
// Table operations (core)
// ═══════════════════════════════════════════════════════════

iceberg::Result<std::vector<iceberg::TableIdentifier>>
PgCatalog::ListTables(const iceberg::Namespace& ns) const
{
    std::vector<iceberg::TableIdentifier> tables;

    /*
     * TODO: SPI query
     *
     * SELECT schemaname, tablename FROM pg_lakehouse_catalog.lakehouse_tables
     * WHERE schemaname = $1 AND format = 'iceberg';
     */

    return tables;
}

iceberg::Result<std::shared_ptr<iceberg::Table>> PgCatalog::CreateTable(
    const iceberg::TableIdentifier& identifier,
    const std::shared_ptr<iceberg::Schema>& schema,
    const std::shared_ptr<iceberg::PartitionSpec>& spec,
    const std::shared_ptr<iceberg::SortOrder>& order,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties)
{
    // 1. Check if table already exists
    auto exists_result = TableExists(identifier);
    if (exists_result.has_value() && exists_result.value()) {
        return iceberg::unexpected(
            iceberg::Error{iceberg::ErrorKind::kAlreadyExists,
                           "Table already exists: " + identifier.name});
    }

    // 2. Determine table location
    std::string table_location = location;
    if (table_location.empty()) {
        std::string ns_path;
        for (const auto& part : identifier.ns) {
            if (!ns_path.empty()) ns_path += "/";
            ns_path += part;
        }
        table_location = warehouse_location_ + "/" + ns_path + "/" + identifier.name;
    }

    // 3. Build initial TableMetadata
    auto metadata_result = iceberg::TableMetadata::Make(
        schema, spec, order, table_location, properties);
    if (!metadata_result.has_value()) {
        return iceberg::unexpected(metadata_result.error());
    }
    auto metadata = std::move(metadata_result.value());

    // 4. Write metadata.json to storage
    auto write_result = WriteMetadata(identifier, *metadata);
    if (!write_result.has_value()) {
        return iceberg::unexpected(write_result.error());
    }
    std::string metadata_location = write_result.value();

    // 5. Insert catalog entry
    CatalogEntry entry;
    entry.schemaname = identifier.ns.empty() ? "public" : identifier.ns[0];
    entry.tablename = identifier.name;
    entry.format = "iceberg";
    entry.metadata_location = metadata_location;
    entry.table_uuid = metadata->uuid();
    entry.last_updated_ms = 0;  /* TODO: get from metadata */
    entry.metadata_version = 0;

    auto insert_status = InsertCatalogEntry(entry);
    if (!insert_status.ok()) {
        /* Clean up metadata file on failure */
        file_io_->DeleteFile(metadata_location);
        return iceberg::unexpected(
            iceberg::Error{iceberg::ErrorKind::kUnknown,
                           "Failed to insert catalog entry"});
    }

    // 6. Return Table instance
    return iceberg::Table::Make(
        identifier, std::move(metadata), std::move(metadata_location),
        file_io_, shared_from_this());
}

iceberg::Result<std::shared_ptr<iceberg::Transaction>> PgCatalog::StageCreateTable(
    const iceberg::TableIdentifier& identifier,
    const std::shared_ptr<iceberg::Schema>& schema,
    const std::shared_ptr<iceberg::PartitionSpec>& spec,
    const std::shared_ptr<iceberg::SortOrder>& order,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties)
{
    // 1. Check if table already exists
    auto exists_result = TableExists(identifier);
    if (exists_result.has_value() && exists_result.value()) {
        return iceberg::unexpected(
            iceberg::Error{iceberg::ErrorKind::kAlreadyExists,
                           "Table already exists: " + identifier.name});
    }

    // 2. Build initial metadata (without writing)
    std::string table_location = location;
    if (table_location.empty()) {
        std::string ns_path;
        for (const auto& part : identifier.ns) {
            if (!ns_path.empty()) ns_path += "/";
            ns_path += part;
        }
        table_location = warehouse_location_ + "/" + ns_path + "/" + identifier.name;
    }

    auto metadata_result = iceberg::TableMetadata::Make(
        schema, spec, order, table_location, properties);
    if (!metadata_result.has_value()) {
        return iceberg::unexpected(metadata_result.error());
    }

    // 3. Create StagedTable (not committed to catalog yet)
    auto staged = iceberg::StagedTable::Make(
        identifier, std::move(metadata_result.value()),
        "" /* no metadata location yet */,
        file_io_, shared_from_this());
    if (!staged.has_value()) {
        return iceberg::unexpected(staged.error());
    }

    // 4. Return Transaction for deferred commit
    return iceberg::Transaction::Make(
        std::move(staged.value()),
        iceberg::Transaction::Kind::kCreate,
        false /* not auto-commit */);
}

iceberg::Result<std::shared_ptr<iceberg::Table>> PgCatalog::UpdateTable(
    const iceberg::TableIdentifier& identifier,
    const std::vector<std::unique_ptr<iceberg::TableRequirement>>& requirements,
    const std::vector<std::unique_ptr<iceberg::TableUpdate>>& updates)
{
    // 1. SELECT ... FOR UPDATE (row-level lock)
    auto entry_result = QueryCatalogForUpdate(identifier);
    if (!entry_result.has_value()) {
        return iceberg::unexpected(entry_result.error());
    }
    auto entry = entry_result.value();

    // 2. Load current metadata
    auto metadata_result = LoadMetadata(entry.metadata_location);
    if (!metadata_result.has_value()) {
        return iceberg::unexpected(metadata_result.error());
    }
    auto current_metadata = std::move(metadata_result.value());

    // 3. Validate requirements against current metadata
    for (const auto& req : requirements) {
        auto status = req->Validate(*current_metadata);
        if (!status.ok()) {
            return iceberg::unexpected(
                iceberg::Error{iceberg::ErrorKind::kCommitFailed,
                               "Requirement validation failed: " + status.message()});
        }
    }

    // 4. Apply updates to build new metadata
    auto builder_result = iceberg::TableMetadataBuilder::FromMetadata(*current_metadata);
    if (!builder_result.has_value()) {
        return iceberg::unexpected(builder_result.error());
    }
    auto builder = std::move(builder_result.value());

    for (const auto& update : updates) {
        auto status = update->Apply(*builder);
        if (!status.ok()) {
            return iceberg::unexpected(
                iceberg::Error{iceberg::ErrorKind::kValidationFailed,
                               "Update failed: " + status.message()});
        }
    }

    auto new_metadata = builder->Build();
    if (!new_metadata.has_value()) {
        return iceberg::unexpected(new_metadata.error());
    }

    // 5. Write new metadata.json
    auto write_result = WriteMetadata(identifier, *new_metadata.value());
    if (!write_result.has_value()) {
        return iceberg::unexpected(write_result.error());
    }
    std::string new_metadata_location = write_result.value();

    // 6. UPDATE lakehouse_tables SET metadata_location = ...
    entry.metadata_location = new_metadata_location;
    entry.metadata_version++;
    auto update_status = UpdateCatalogEntry(entry);
    if (!update_status.ok()) {
        /* Clean up new metadata file on failure */
        file_io_->DeleteFile(new_metadata_location);
        return iceberg::unexpected(
            iceberg::Error{iceberg::ErrorKind::kCommitFailed,
                           "Failed to update catalog entry"});
    }

    // 7. Return new Table instance
    return iceberg::Table::Make(
        identifier, std::move(new_metadata.value()),
        std::move(new_metadata_location),
        file_io_, shared_from_this());
}

iceberg::Result<std::shared_ptr<iceberg::Table>> PgCatalog::LoadTable(
    const iceberg::TableIdentifier& identifier)
{
    // 1. Query catalog for metadata location
    auto entry_result = QueryCatalogEntry(identifier);
    if (!entry_result.has_value()) {
        return iceberg::unexpected(
            iceberg::Error{iceberg::ErrorKind::kNoSuchTable,
                           "Table not found: " + identifier.name});
    }
    auto entry = entry_result.value();

    // 2. Load metadata from storage
    auto metadata_result = LoadMetadata(entry.metadata_location);
    if (!metadata_result.has_value()) {
        return iceberg::unexpected(metadata_result.error());
    }

    // 3. Return Table instance
    return iceberg::Table::Make(
        identifier, std::move(metadata_result.value()),
        entry.metadata_location,
        file_io_, shared_from_this());
}

iceberg::Result<std::shared_ptr<iceberg::Table>> PgCatalog::RegisterTable(
    const iceberg::TableIdentifier& identifier,
    const std::string& metadata_file_location)
{
    // 1. Check if table already exists
    auto exists_result = TableExists(identifier);
    if (exists_result.has_value() && exists_result.value()) {
        return iceberg::unexpected(
            iceberg::Error{iceberg::ErrorKind::kAlreadyExists,
                           "Table already exists: " + identifier.name});
    }

    // 2. Load metadata to validate
    auto metadata_result = LoadMetadata(metadata_file_location);
    if (!metadata_result.has_value()) {
        return iceberg::unexpected(metadata_result.error());
    }
    auto metadata = std::move(metadata_result.value());

    // 3. Insert catalog entry
    CatalogEntry entry;
    entry.schemaname = identifier.ns.empty() ? "public" : identifier.ns[0];
    entry.tablename = identifier.name;
    entry.format = "iceberg";
    entry.metadata_location = metadata_file_location;
    entry.table_uuid = metadata->uuid();
    entry.last_updated_ms = 0;
    entry.metadata_version = 0;

    auto insert_status = InsertCatalogEntry(entry);
    if (!insert_status.ok()) {
        return iceberg::unexpected(
            iceberg::Error{iceberg::ErrorKind::kUnknown,
                           "Failed to insert catalog entry"});
    }

    // 4. Return Table instance
    return iceberg::Table::Make(
        identifier, std::move(metadata),
        metadata_file_location,
        file_io_, shared_from_this());
}

iceberg::Result<bool> PgCatalog::TableExists(
    const iceberg::TableIdentifier& identifier) const
{
    /*
     * TODO: SPI query
     *
     * SELECT 1 FROM pg_lakehouse_catalog.lakehouse_tables
     * WHERE schemaname = $1 AND tablename = $2;
     */
    return false;  /* Placeholder */
}

iceberg::Status PgCatalog::DropTable(
    const iceberg::TableIdentifier& identifier, bool purge)
{
    // 1. Lock and get metadata location
    auto entry_result = QueryCatalogForUpdate(identifier);
    if (!entry_result.has_value()) {
        return iceberg::NotFound("Table not found: " + identifier.name);
    }
    auto entry = entry_result.value();

    // 2. Delete catalog entry
    auto delete_status = DeleteCatalogEntry(identifier);
    if (!delete_status.ok()) {
        return delete_status;
    }

    // 3. Optionally purge data files
    if (purge) {
        /*
         * TODO: Delete all data files + metadata files from S3
         *
         * Steps:
         *   1. Load metadata to get all data files
         *   2. Delete each data file via file_io_
         *   3. Delete metadata files
         */
    }

    return {};
}

iceberg::Status PgCatalog::RenameTable(
    const iceberg::TableIdentifier& from,
    const iceberg::TableIdentifier& to)
{
    // 1. Lock the source entry
    auto entry_result = QueryCatalogForUpdate(from);
    if (!entry_result.has_value()) {
        return iceberg::NotFound("Table not found: " + from.name);
    }
    auto entry = entry_result.value();

    // 2. Check target doesn't exist
    auto exists_result = TableExists(to);
    if (exists_result.has_value() && exists_result.value()) {
        return iceberg::AlreadyExists("Target table already exists: " + to.name);
    }

    // 3. Update the catalog entry
    entry.schemaname = to.ns.empty() ? "public" : to.ns[0];
    entry.tablename = to.name;
    auto update_status = UpdateCatalogEntry(entry);
    if (!update_status.ok()) {
        return update_status;
    }

    return {};
}

// ═══════════════════════════════════════════════════════════
// Private: PG catalog table operations (via SPI)
// ═══════════════════════════════════════════════════════════

iceberg::Result<CatalogEntry> PgCatalog::QueryCatalogForUpdate(
    const iceberg::TableIdentifier& id)
{
    std::string schema = id.ns.empty() ? "public" : id.ns[0];
    std::string query =
        "SELECT schemaname, tablename, format, metadata_location, "
        "table_uuid, last_updated_ms, metadata_version "
        "FROM pg_lakehouse_catalog.lakehouse_tables "
        "WHERE schemaname = '" + EscapeLiteral(schema) + "' "
        "AND tablename = '" + EscapeLiteral(id.name) + "' "
        "FOR UPDATE";

    int ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        return iceberg::unexpected(
            iceberg::Error{iceberg::ErrorKind::kUnknown, "SPI_connect failed"});
    }

    ret = SPI_execute(query.c_str(), true /* read-only: false for FOR UPDATE */,
                      1 /* max rows */);

    if (ret != SPI_OK_SELECT || SPI_processed == 0) {
        SPI_finish();
        return iceberg::unexpected(
            iceberg::Error{iceberg::ErrorKind::kNoSuchTable,
                           "Table not found: " + id.name});
    }

    /* Extract entry from SPI result */
    CatalogEntry entry;
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    HeapTuple tuple = SPI_tuptable->vals[0];

    bool isnull;
    entry.schemaname = SPI_getvalue(tuple, tupdesc, 1);
    entry.tablename = SPI_getvalue(tuple, tupdesc, 2);
    entry.format = SPI_getvalue(tuple, tupdesc, 3);
    entry.metadata_location = SPI_getvalue(tuple, tupdesc, 4);
    entry.table_uuid = SPI_getvalue(tuple, tupdesc, 5) ?: "";

    Datum val = SPI_getbinval(tuple, tupdesc, 6, &isnull);
    entry.last_updated_ms = isnull ? 0 : DatumGetInt64(val);

    val = SPI_getbinval(tuple, tupdesc, 7, &isnull);
    entry.metadata_version = isnull ? 0 : DatumGetInt32(val);

    SPI_finish();
    return entry;
}

iceberg::Result<CatalogEntry> PgCatalog::QueryCatalogEntry(
    const iceberg::TableIdentifier& id) const
{
    std::string schema = id.ns.empty() ? "public" : id.ns[0];
    std::string query =
        "SELECT schemaname, tablename, format, metadata_location, "
        "table_uuid, last_updated_ms, metadata_version "
        "FROM pg_lakehouse_catalog.lakehouse_tables "
        "WHERE schemaname = '" + EscapeLiteral(schema) + "' "
        "AND tablename = '" + EscapeLiteral(id.name) + "'";

    int ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        return iceberg::unexpected(
            iceberg::Error{iceberg::ErrorKind::kUnknown, "SPI_connect failed"});
    }

    ret = SPI_execute(query.c_str(), true, 1);

    if (ret != SPI_OK_SELECT || SPI_processed == 0) {
        SPI_finish();
        return iceberg::unexpected(
            iceberg::Error{iceberg::ErrorKind::kNoSuchTable,
                           "Table not found: " + id.name});
    }

    CatalogEntry entry;
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    HeapTuple tuple = SPI_tuptable->vals[0];

    entry.schemaname = SPI_getvalue(tuple, tupdesc, 1);
    entry.tablename = SPI_getvalue(tuple, tupdesc, 2);
    entry.format = SPI_getvalue(tuple, tupdesc, 3);
    entry.metadata_location = SPI_getvalue(tuple, tupdesc, 4);
    entry.table_uuid = SPI_getvalue(tuple, tupdesc, 5) ?: "";

    bool isnull;
    Datum val = SPI_getbinval(tuple, tupdesc, 6, &isnull);
    entry.last_updated_ms = isnull ? 0 : DatumGetInt64(val);

    val = SPI_getbinval(tuple, tupdesc, 7, &isnull);
    entry.metadata_version = isnull ? 0 : DatumGetInt32(val);

    SPI_finish();
    return entry;
}

iceberg::Status PgCatalog::InsertCatalogEntry(const CatalogEntry& entry)
{
    std::string cmd =
        "INSERT INTO pg_lakehouse_catalog.lakehouse_tables "
        "(schemaname, tablename, format, metadata_location, table_uuid, "
        "last_updated_ms, metadata_version) VALUES ("
        "'" + EscapeLiteral(entry.schemaname) + "', "
        "'" + EscapeLiteral(entry.tablename) + "', "
        "'" + EscapeLiteral(entry.format) + "', "
        "'" + EscapeLiteral(entry.metadata_location) + "', "
        "'" + EscapeLiteral(entry.table_uuid) + "', "
        + std::to_string(entry.last_updated_ms) + ", "
        + std::to_string(entry.metadata_version) + ")";

    int ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        return iceberg::Status(iceberg::ErrorKind::kUnknown, "SPI_connect failed");
    }

    ret = SPI_execute(cmd.c_str(), false, 0);

    SPI_finish();

    if (ret != SPI_OK_INSERT) {
        return iceberg::Status(iceberg::ErrorKind::kUnknown,
                               "Failed to insert catalog entry");
    }

    return {};
}

iceberg::Status PgCatalog::UpdateCatalogEntry(const CatalogEntry& entry)
{
    std::string cmd =
        "UPDATE pg_lakehouse_catalog.lakehouse_tables SET "
        "metadata_location = '" + EscapeLiteral(entry.metadata_location) + "', "
        "last_updated_ms = " + std::to_string(entry.last_updated_ms) + ", "
        "metadata_version = " + std::to_string(entry.metadata_version) + " "
        "WHERE schemaname = '" + EscapeLiteral(entry.schemaname) + "' "
        "AND tablename = '" + EscapeLiteral(entry.tablename) + "'";

    int ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        return iceberg::Status(iceberg::ErrorKind::kUnknown, "SPI_connect failed");
    }

    ret = SPI_execute(cmd.c_str(), false, 0);

    SPI_finish();

    if (ret != SPI_OK_UPDATE) {
        return iceberg::Status(iceberg::ErrorKind::kCommitFailed,
                               "Failed to update catalog entry");
    }

    return {};
}

iceberg::Status PgCatalog::DeleteCatalogEntry(const iceberg::TableIdentifier& id)
{
    std::string schema = id.ns.empty() ? "public" : id.ns[0];
    std::string cmd =
        "DELETE FROM pg_lakehouse_catalog.lakehouse_tables "
        "WHERE schemaname = '" + EscapeLiteral(schema) + "' "
        "AND tablename = '" + EscapeLiteral(id.name) + "'";

    int ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        return iceberg::Status(iceberg::ErrorKind::kUnknown, "SPI_connect failed");
    }

    ret = SPI_execute(cmd.c_str(), false, 0);

    SPI_finish();

    if (ret != SPI_OK_DELETE) {
        return iceberg::Status(iceberg::ErrorKind::kUnknown,
                               "Failed to delete catalog entry");
    }

    return {};
}

// ═══════════════════════════════════════════════════════════
// Private: Metadata file operations
// ═══════════════════════════════════════════════════════════

iceberg::Result<std::shared_ptr<iceberg::TableMetadata>>
PgCatalog::LoadMetadata(const std::string& location)
{
    return iceberg::TableMetadata::ReadMetadata(*file_io_, location);
}

iceberg::Result<std::string> PgCatalog::WriteMetadata(
    const iceberg::TableIdentifier& id,
    const iceberg::TableMetadata& metadata)
{
    /* Generate location for new metadata file */
    std::string new_location = GenerateMetadataLocation(id, 0 /* TODO: version */);

    /* Write metadata to storage */
    auto status = iceberg::TableMetadata::WriteMetadata(metadata, *file_io_, new_location);
    if (!status.ok()) {
        return iceberg::unexpected(
            iceberg::Error{iceberg::ErrorKind::kIOError,
                           "Failed to write metadata: " + status.message()});
    }

    return new_location;
}

std::string PgCatalog::GenerateMetadataLocation(
    const iceberg::TableIdentifier& id, int version)
{
    std::string ns_path;
    for (const auto& part : id.ns) {
        if (!ns_path.empty()) ns_path += "/";
        ns_path += part;
    }

    return warehouse_location_ + "/" + ns_path + "/" + id.name +
           "/metadata/v" + std::to_string(version) + ".metadata.json";
}

// ═══════════════════════════════════════════════════════════
// Private: SPI helpers
// ═══════════════════════════════════════════════════════════

int PgCatalog::ExecuteSpiQuery(const char* query, void** result)
{
    int ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) return ret;

    ret = SPI_execute(query, true, 0);
    if (result) *result = SPI_tuptable;

    SPI_finish();
    return ret;
}

int PgCatalog::ExecuteSpiCommand(const char* command)
{
    int ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) return ret;

    ret = SPI_execute(command, false, 0);

    SPI_finish();
    return ret;
}

std::string PgCatalog::EscapeLiteral(const std::string& str)
{
    /* Simple SQL escaping: double single quotes */
    std::string escaped;
    escaped.reserve(str.size());
    for (char c : str) {
        if (c == '\'') escaped += "''";
        else escaped += c;
    }
    return escaped;
}

}  // namespace pg_lakehouse
