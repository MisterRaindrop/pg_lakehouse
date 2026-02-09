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
 * table_ops.h
 *      Iceberg DDL operations (create, open, drop table)
 *
 * These functions implement the DDL portion of FormatHandler for Iceberg.
 * They are called by handler.c and ultimately by pg_lakehouse_table's DDL layer.
 */

#ifndef ICEBERG_TABLE_OPS_H
#define ICEBERG_TABLE_OPS_H

#include "postgres.h"

#include "access/tableam.h"
#include "nodes/pg_list.h"
#include "utils/rel.h"

/*
 * Parsed Iceberg table options from WITH(...)
 */
typedef struct IcebergTableOptions
{
    char   *location;           /* e.g., "s3://bucket/warehouse/db/table" */
    char   *warehouse;          /* e.g., "s3://bucket/warehouse" */
    char   *catalog_type;       /* "internal" (PG) / "rest" / "hadoop" */
    char   *catalog_uri;        /* REST catalog URI (if applicable) */
    char   *file_format;        /* "parquet" / "orc" / "avro", default: parquet */
    int     target_file_size;   /* target file size in bytes, default: 128MB */
} IcebergTableOptions;

/*
 * Runtime table handle (per-session state for an opened Iceberg table)
 */
typedef struct IcebergTableState
{
    Oid     relid;              /* PostgreSQL relation OID */
    IcebergTableOptions opts;   /* Parsed table options */

    /* Catalog bridge handle (opaque, from catalog_bridge.h) */
    void   *catalog_handle;

    /* Bridge table handle (opaque, from iceberg_bridge.h) */
    void   *bridge_handle;

    bool    is_open;            /* Whether the table has been opened */
} IcebergTableState;

/*
 * Validate WITH(...) options for CREATE TABLE ... USING lakehouse_am
 *
 * Checks that required options (location) are present and valid.
 * Fills in IcebergTableOptions on success.
 *
 * Returns 0 on success, non-zero on error (sets errmsg).
 */
extern int iceberg_validate_options(List *options,
                                    IcebergTableOptions *out,
                                    char **errmsg);

/*
 * Create a new Iceberg table.
 *
 * Called during CREATE TABLE ... USING lakehouse_am WITH (format='iceberg', ...)
 *
 * Steps:
 *   1. Validate options
 *   2. Create initial metadata via catalog_bridge (PgCatalog::CreateTable)
 *   3. Open the table handle
 *   4. Register in lakehouse_tables (done by PgCatalog internally)
 *
 * Returns 0 on success.
 */
extern int iceberg_create_table(Relation rel, List *options);

/*
 * Open an existing Iceberg table.
 *
 * Loads metadata from lakehouse_tables via catalog_bridge.
 * Sets up IcebergTableState for subsequent scan/write operations.
 *
 * Returns table state, or NULL on failure.
 */
extern IcebergTableState *iceberg_open_table(Oid relid);

/*
 * Close an Iceberg table state (free resources).
 */
extern void iceberg_close_table(IcebergTableState *state);

/*
 * Drop an Iceberg table.
 *
 * Steps:
 *   1. Remove from lakehouse_tables via catalog_bridge
 *   2. Optionally purge data files from S3
 *
 * Returns 0 on success.
 */
extern int iceberg_drop_table(Oid relid, bool purge);

/*
 * Get metadata location for an Iceberg table.
 *
 * Returns the S3 path to the current metadata.json file.
 * Caller must pfree the returned string.
 */
extern int iceberg_get_metadata_location(Oid relid, char **location);

/*
 * Internal helper: parse reloptions into IcebergTableOptions
 */
extern int iceberg_parse_options(List *options, IcebergTableOptions *out);

/*
 * Internal helper: generate a UUID string for new table
 */
extern void iceberg_generate_uuid(char *uuid_out, size_t size);

#endif /* ICEBERG_TABLE_OPS_H */
