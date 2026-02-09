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
 * pg_catalog.h
 *      PostgreSQL internal Iceberg catalog implementation
 *
 * PgCatalog implements iceberg::Catalog using PostgreSQL's own tables
 * (pg_lakehouse_catalog.lakehouse_tables) to store Iceberg metadata pointers.
 *
 * Key design decisions:
 *   - Namespace = PG Schema (managed by native DDL, not Iceberg)
 *   - Row-level locking (SELECT ... FOR UPDATE) for concurrent access
 *   - SPI for SQL execution within the extension
 *   - FileIO from iceberg-cpp for S3/local storage access
 */

#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <iceberg/catalog.h>
#include <iceberg/file_io.h>
#include <iceberg/result.h>
#include <iceberg/table.h>
#include <iceberg/table_identifier.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>

namespace pg_lakehouse {

/*
 * Entry from the lakehouse_tables catalog table
 */
struct CatalogEntry {
    std::string schemaname;
    std::string tablename;
    std::string format;             /* "iceberg" */
    std::string metadata_location;  /* S3 path to metadata.json */
    std::string table_uuid;
    int64_t     last_updated_ms;
    int32_t     metadata_version;
};

/*
 * PgCatalog - PostgreSQL internal Iceberg catalog
 *
 * Implements the iceberg::Catalog interface using PG tables for metadata.
 * All SQL operations are performed via SPI (Server Programming Interface).
 *
 * Thread safety: NOT thread-safe. Must be used within a single PG backend.
 */
class PgCatalog : public iceberg::Catalog,
                  public std::enable_shared_from_this<PgCatalog> {
 public:
    /*
     * Create a new PgCatalog instance.
     *
     * @param file_io  FileIO for reading/writing metadata files
     * @param warehouse_location  Base location for table data
     */
    static iceberg::Result<std::shared_ptr<PgCatalog>> Make(
        std::shared_ptr<iceberg::FileIO> file_io,
        std::string warehouse_location);

    ~PgCatalog() override;

    // ═══ Catalog interface ═══

    std::string_view name() const override { return "pg_catalog"; }

    // ═══ Namespace operations (stub: PG Schema = Iceberg Namespace) ═══

    iceberg::Status CreateNamespace(
        const iceberg::Namespace& ns,
        const std::unordered_map<std::string, std::string>& properties) override;

    iceberg::Result<std::vector<iceberg::Namespace>> ListNamespaces(
        const iceberg::Namespace& ns) const override;

    iceberg::Result<std::unordered_map<std::string, std::string>>
    GetNamespaceProperties(const iceberg::Namespace& ns) const override;

    iceberg::Status DropNamespace(const iceberg::Namespace& ns) override;

    iceberg::Result<bool> NamespaceExists(
        const iceberg::Namespace& ns) const override;

    iceberg::Status UpdateNamespaceProperties(
        const iceberg::Namespace& ns,
        const std::unordered_map<std::string, std::string>& updates,
        const std::unordered_set<std::string>& removals) override;

    // ═══ Table operations (core) ═══

    iceberg::Result<std::vector<iceberg::TableIdentifier>> ListTables(
        const iceberg::Namespace& ns) const override;

    iceberg::Result<std::shared_ptr<iceberg::Table>> CreateTable(
        const iceberg::TableIdentifier& identifier,
        const std::shared_ptr<iceberg::Schema>& schema,
        const std::shared_ptr<iceberg::PartitionSpec>& spec,
        const std::shared_ptr<iceberg::SortOrder>& order,
        const std::string& location,
        const std::unordered_map<std::string, std::string>& properties) override;

    iceberg::Result<std::shared_ptr<iceberg::Table>> UpdateTable(
        const iceberg::TableIdentifier& identifier,
        const std::vector<std::unique_ptr<iceberg::TableRequirement>>& requirements,
        const std::vector<std::unique_ptr<iceberg::TableUpdate>>& updates) override;

    iceberg::Result<std::shared_ptr<iceberg::Transaction>> StageCreateTable(
        const iceberg::TableIdentifier& identifier,
        const std::shared_ptr<iceberg::Schema>& schema,
        const std::shared_ptr<iceberg::PartitionSpec>& spec,
        const std::shared_ptr<iceberg::SortOrder>& order,
        const std::string& location,
        const std::unordered_map<std::string, std::string>& properties) override;

    iceberg::Result<bool> TableExists(
        const iceberg::TableIdentifier& identifier) const override;

    iceberg::Status DropTable(
        const iceberg::TableIdentifier& identifier, bool purge) override;

    iceberg::Status RenameTable(
        const iceberg::TableIdentifier& from,
        const iceberg::TableIdentifier& to) override;

    iceberg::Result<std::shared_ptr<iceberg::Table>> LoadTable(
        const iceberg::TableIdentifier& identifier) override;

    iceberg::Result<std::shared_ptr<iceberg::Table>> RegisterTable(
        const iceberg::TableIdentifier& identifier,
        const std::string& metadata_file_location) override;

 private:
    PgCatalog(std::shared_ptr<iceberg::FileIO> file_io,
              std::string warehouse_location);

    // ═══ PG catalog table operations (via SPI) ═══

    iceberg::Result<CatalogEntry> QueryCatalogForUpdate(
        const iceberg::TableIdentifier& id);

    iceberg::Result<CatalogEntry> QueryCatalogEntry(
        const iceberg::TableIdentifier& id) const;

    iceberg::Status InsertCatalogEntry(const CatalogEntry& entry);

    iceberg::Status UpdateCatalogEntry(const CatalogEntry& entry);

    iceberg::Status DeleteCatalogEntry(const iceberg::TableIdentifier& id);

    // ═══ Metadata file operations ═══

    iceberg::Result<std::shared_ptr<iceberg::TableMetadata>> LoadMetadata(
        const std::string& location);

    iceberg::Result<std::string> WriteMetadata(
        const iceberg::TableIdentifier& id,
        const iceberg::TableMetadata& metadata);

    std::string GenerateMetadataLocation(
        const iceberg::TableIdentifier& id, int version);

    // ═══ SPI helpers ═══

    static int ExecuteSpiQuery(const char* query, void** result);
    static int ExecuteSpiCommand(const char* command);
    static std::string EscapeLiteral(const std::string& str);

    // ═══ Member variables ═══

    std::shared_ptr<iceberg::FileIO> file_io_;
    std::string warehouse_location_;
};

}  // namespace pg_lakehouse
