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
 * catalog_bridge.h
 *      C ABI for PgCatalog (C++ → C bridge)
 *
 * This header provides a C-compatible interface to PgCatalog
 * so that operations/*.c files can call catalog functions without
 * requiring C++ compilation.
 *
 * All opaque handles are forward-declared structs.
 * Error information is returned via IcebergError (from iceberg_bridge.h).
 */

#ifndef ICEBERG_CATALOG_BRIDGE_H
#define ICEBERG_CATALOG_BRIDGE_H

#include "bridge/iceberg_bridge.h"  /* For IcebergError, IcebergTableHandle */

#ifdef __cplusplus
extern "C" {
#endif

/* ═══ Opaque handle types ═══ */

typedef struct PgCatalogHandle PgCatalogHandle;
typedef struct IcebergTransactionHandle IcebergTransactionHandle;
typedef struct IcebergAppendHandle IcebergAppendHandle;

/* ═══ Catalog lifecycle ═══ */

/*
 * Create a PgCatalog instance.
 *
 * @param warehouse_location  Base S3/local path (e.g., "s3://bucket/warehouse")
 * @param error               Error info on failure
 * @return Handle, or NULL on failure
 */
PgCatalogHandle *pg_catalog_create(const char *warehouse_location,
                                   IcebergError *error);

/*
 * Destroy a PgCatalog instance and free resources.
 */
void pg_catalog_destroy(PgCatalogHandle *catalog);

/* ═══ Table operations ═══ */

/*
 * Create a new Iceberg table in the catalog.
 *
 * Writes initial metadata.json and inserts into lakehouse_tables.
 *
 * @return 0 on success, non-zero on failure
 */
int pg_catalog_create_table(PgCatalogHandle *catalog,
                            const char *namespace_,
                            const char *table_name,
                            const char *schema_json,
                            const char *location,
                            IcebergError *error);

/*
 * Load an existing table from the catalog.
 *
 * Returns an IcebergTableHandle that can be used for scan/write operations.
 */
IcebergTableHandle *pg_catalog_load_table(PgCatalogHandle *catalog,
                                          const char *namespace_,
                                          const char *table_name,
                                          IcebergError *error);

/*
 * Drop a table from the catalog.
 *
 * @param purge  If true, delete all data files from storage
 * @return 0 on success
 */
int pg_catalog_drop_table(PgCatalogHandle *catalog,
                          const char *namespace_,
                          const char *table_name,
                          bool purge,
                          IcebergError *error);

/*
 * Check if a table exists in the catalog.
 *
 * @return 1 if exists, 0 if not, -1 on error
 */
int pg_catalog_table_exists(PgCatalogHandle *catalog,
                            const char *namespace_,
                            const char *table_name,
                            IcebergError *error);

/*
 * Rename a table in the catalog.
 */
int pg_catalog_rename_table(PgCatalogHandle *catalog,
                            const char *from_namespace,
                            const char *from_name,
                            const char *to_namespace,
                            const char *to_name,
                            IcebergError *error);

/* ═══ Transaction operations ═══ */

/*
 * Create a new Iceberg Transaction for a table.
 *
 * The transaction accumulates updates and commits atomically.
 */
IcebergTransactionHandle *pg_catalog_new_transaction(
    IcebergTableHandle *table,
    IcebergError *error);

/*
 * Commit the transaction.
 *
 * Calls Transaction::Commit() which internally calls Catalog::UpdateTable().
 * This updates metadata.json and lakehouse_tables atomically.
 *
 * @return 0 on success
 */
int pg_catalog_transaction_commit(IcebergTransactionHandle *txn,
                                  IcebergError *error);

/*
 * Abort the transaction and release resources.
 */
void pg_catalog_transaction_abort(IcebergTransactionHandle *txn);

/* ═══ FastAppend operations ═══ */

/*
 * Create a new FastAppend action within a transaction.
 *
 * FastAppend adds data files to a new snapshot without reading existing data.
 */
IcebergAppendHandle *pg_catalog_new_fast_append(
    IcebergTransactionHandle *txn,
    IcebergError *error);

/*
 * Add a data file to the FastAppend.
 *
 * @param file_path     Path to the Parquet file (S3 or local)
 * @param file_size     Size of the file in bytes
 * @param record_count  Number of records in the file
 * @return 0 on success
 */
int pg_catalog_append_data_file(IcebergAppendHandle *append,
                                const char *file_path,
                                int64_t file_size,
                                int64_t record_count,
                                IcebergError *error);

/*
 * Commit the FastAppend action (applies to the transaction).
 *
 * @return 0 on success
 */
int pg_catalog_append_commit(IcebergAppendHandle *append,
                             IcebergError *error);

#ifdef __cplusplus
}
#endif

#endif /* ICEBERG_CATALOG_BRIDGE_H */
