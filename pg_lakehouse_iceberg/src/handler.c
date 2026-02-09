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
 * handler.c
 *      FormatHandler vtable for Iceberg and extension initialization.
 *
 * This file defines:
 *   1. The Iceberg FormatHandler function-pointer table.
 *   2. _PG_init() which registers the handler, the transaction callbacks
 *      and the iceberg-cpp bridge.
 *
 * The FormatHandler is the central abstraction that pg_lakehouse_table
 * (the shared Table-AM layer) calls to perform format-specific operations.
 */

#include "postgres.h"
#include "fmgr.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "utils/guc.h"

/* Operations */
#include "operations/table_ops.h"
#include "operations/scan_ops.h"
#include "operations/write_ops.h"
#include "operations/commit_ops.h"

/* Bridge */
#ifdef HAVE_ICEBERG_CPP
#include "bridge/iceberg_bridge.h"
#include "catalog/catalog_bridge.h"
#endif

PG_MODULE_MAGIC;

/* ============================================================================
 * GUC variables
 * ============================================================================
 */

/* Default warehouse location for new Iceberg tables */
static char *iceberg_warehouse_location = NULL;

/* Default catalog type (pg / rest / hadoop) */
static char *iceberg_catalog_type = NULL;

/* Compaction threshold (number of data files triggering merge) */
static int iceberg_compaction_threshold = 64;

/* ============================================================================
 * FormatHandler vtable
 *
 * Each function pointer delegates to the corresponding operations/ module.
 * This is the contract between pg_lakehouse_table and pg_lakehouse_iceberg.
 * ============================================================================
 */

/*
 * Validate CREATE TABLE ... USING iceberg WITH (...) options.
 */
static bool
iceberg_validate_options(List *options)
{
    /*
     * TODO: Validate that mandatory options are present:
     *   - location (S3 path)
     * Optional:
     *   - catalog_type, catalog_uri, partition_spec_json, ...
     */
    elog(DEBUG1, "iceberg_validate_options");
    return true;
}

/*
 * Create a new Iceberg table.
 */
static void
iceberg_create_table(Relation relation, const char *location, List *options)
{
    iceberg_ops_create_table(relation, location, options);
}

/*
 * Open an existing Iceberg table and return an opaque handle.
 */
static void *
iceberg_open_table(Relation relation)
{
    /* TODO: Delegate to bridge to open table and return IcebergTableHandle */
    elog(DEBUG1, "iceberg_open_table: %s", RelationGetRelationName(relation));
    return NULL;
}

/*
 * Drop an Iceberg table.
 */
static void
iceberg_drop_table(Relation relation)
{
    iceberg_ops_drop_table(relation, false /* cascade */);
}

/*
 * Plan fragment list for a table (for parallel scan).
 */
static List *
iceberg_plan_fragments(void *table_handle)
{
    /* TODO: Delegate to bridge parallel_plan_create */
    elog(DEBUG1, "iceberg_plan_fragments");
    return NIL;
}

/*
 * Scan callbacks — delegated to scan_ops.c
 */
static TableScanDesc
iceberg_scan_begin_wrapper(Relation relation, Snapshot snapshot,
                           int nkeys, ScanKey key,
                           ParallelTableScanDesc parallel_scan,
                           uint32 flags)
{
    return iceberg_ops_scan_begin(relation, snapshot, nkeys, key,
                                 parallel_scan, flags);
}

static bool
iceberg_scan_next_wrapper(TableScanDesc sscan, ScanDirection direction,
                          TupleTableSlot *slot)
{
    return iceberg_ops_scan_getnextslot(sscan, direction, slot);
}

static void
iceberg_scan_end_wrapper(TableScanDesc sscan)
{
    iceberg_ops_scan_end(sscan);
}

/*
 * Write callbacks — delegated to write_ops.c
 */
static void
iceberg_write_begin(void *table_handle)
{
    /* TODO: Allocate writer state for batch buffering */
    elog(DEBUG1, "iceberg_write_begin");
}

static void
iceberg_write_tuple(void *table_handle, TupleTableSlot *slot)
{
    /* TODO: Buffer tuple for batch writing */
    elog(DEBUG1, "iceberg_write_tuple");
}

static void
iceberg_write_flush(void *table_handle)
{
    /* TODO: Flush buffered tuples to Parquet via bridge writer */
    elog(DEBUG1, "iceberg_write_flush");
}

/*
 * Commit / abort — delegated to commit_ops.c
 */
static void
iceberg_commit(void *table_handle)
{
    iceberg_ops_commit_transaction(XACT_EVENT_PRE_COMMIT, table_handle);
}

static void
iceberg_abort(void *table_handle)
{
    iceberg_ops_abort_transaction(XACT_EVENT_ABORT, table_handle);
}

/*
 * Get the metadata.json location for the table.
 */
static const char *
iceberg_get_metadata_location(void *table_handle)
{
    /* TODO: Return metadata_location from IcebergTableHandle */
    elog(DEBUG1, "iceberg_get_metadata_location");
    return NULL;
}

/* ============================================================================
 * FormatHandler struct
 * ============================================================================
 */

typedef struct FormatHandler
{
    const char *format_name;

    /* Validation */
    bool       (*validate_options)(List *options);

    /* DDL */
    void       (*create_table)(Relation relation, const char *location, List *options);
    void       *(*open_table)(Relation relation);
    void       (*drop_table)(Relation relation);

    /* Planning */
    List       *(*plan_fragments)(void *table_handle);

    /* Scan */
    TableScanDesc (*scan_begin)(Relation relation, Snapshot snapshot,
                                int nkeys, ScanKey key,
                                ParallelTableScanDesc parallel_scan,
                                uint32 flags);
    bool       (*scan_next)(TableScanDesc sscan, ScanDirection direction,
                            TupleTableSlot *slot);
    void       (*scan_end)(TableScanDesc sscan);

    /* Write */
    void       (*write_begin)(void *table_handle);
    void       (*write_tuple)(void *table_handle, TupleTableSlot *slot);
    void       (*write_flush)(void *table_handle);

    /* Transaction */
    void       (*commit)(void *table_handle);
    void       (*abort)(void *table_handle);

    /* Metadata */
    const char *(*get_metadata_location)(void *table_handle);

} FormatHandler;

static const FormatHandler iceberg_handler = {
    .format_name           = "iceberg",

    .validate_options      = iceberg_validate_options,
    .create_table          = iceberg_create_table,
    .open_table            = iceberg_open_table,
    .drop_table            = iceberg_drop_table,

    .plan_fragments        = iceberg_plan_fragments,

    .scan_begin            = iceberg_scan_begin_wrapper,
    .scan_next             = iceberg_scan_next_wrapper,
    .scan_end              = iceberg_scan_end_wrapper,

    .write_begin           = iceberg_write_begin,
    .write_tuple           = iceberg_write_tuple,
    .write_flush           = iceberg_write_flush,

    .commit                = iceberg_commit,
    .abort                 = iceberg_abort,

    .get_metadata_location = iceberg_get_metadata_location,
};

/*
 * Accessor so that iceberg_am.c can retrieve the handler.
 */
const FormatHandler *
iceberg_get_format_handler(void)
{
    return &iceberg_handler;
}

/* ============================================================================
 * Transaction callback (registered once in _PG_init)
 * ============================================================================
 */

static void
iceberg_xact_callback(XactEvent event, void *arg)
{
    switch (event)
    {
        case XACT_EVENT_PRE_COMMIT:
            iceberg_ops_commit_transaction(event, arg);
            break;
        case XACT_EVENT_ABORT:
            iceberg_ops_abort_transaction(event, arg);
            break;
        default:
            break;
    }
}

/* ============================================================================
 * Extension initialization
 * ============================================================================
 */

void
_PG_init(void)
{
    elog(LOG, "pg_lakehouse_iceberg: initializing Iceberg Table Access Method");

    /* ── GUC: warehouse location ── */
    DefineCustomStringVariable(
        "pg_lakehouse_iceberg.warehouse_location",
        "Default warehouse location for Iceberg tables (e.g., s3://bucket/warehouse).",
        NULL,
        &iceberg_warehouse_location,
        "",               /* default */
        PGC_SUSET,
        0,
        NULL, NULL, NULL);

    /* ── GUC: catalog type ── */
    DefineCustomStringVariable(
        "pg_lakehouse_iceberg.catalog_type",
        "Default catalog type: pg, rest, or hadoop.",
        NULL,
        &iceberg_catalog_type,
        "pg",             /* default */
        PGC_SUSET,
        0,
        NULL, NULL, NULL);

    /* ── GUC: compaction threshold ── */
    DefineCustomIntVariable(
        "pg_lakehouse_iceberg.compaction_threshold",
        "Number of data files before auto-compaction triggers.",
        NULL,
        &iceberg_compaction_threshold,
        64,               /* default */
        1,                /* min */
        10000,            /* max */
        PGC_SUSET,
        0,
        NULL, NULL, NULL);

    /* ── Register transaction callback ── */
    RegisterXactCallback(iceberg_xact_callback, NULL);

    /* ── Initialize iceberg-cpp bridge ── */
#ifdef HAVE_ICEBERG_CPP
    {
        IcebergError error = {0};
        if (iceberg_bridge_init(&error) != 0)
        {
            elog(WARNING,
                 "pg_lakehouse_iceberg: failed to initialize iceberg-cpp: %s",
                 error.message);
        }
        else
        {
            elog(LOG, "pg_lakehouse_iceberg: iceberg-cpp initialized successfully");
        }
    }
#else
    elog(LOG, "pg_lakehouse_iceberg: built without iceberg-cpp support");
#endif

    elog(LOG, "pg_lakehouse_iceberg: format handler '%s' registered",
         iceberg_handler.format_name);
}

void
_PG_fini(void)
{
    elog(LOG, "pg_lakehouse_iceberg: shutting down");

    UnregisterXactCallback(iceberg_xact_callback, NULL);

#ifdef HAVE_ICEBERG_CPP
    iceberg_bridge_shutdown();
#endif
}
