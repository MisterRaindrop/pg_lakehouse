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
 * iceberg_am.c
 *      Iceberg Table Access Method implementation for PostgreSQL
 *
 * This file implements a custom Table Access Method (AM) that allows
 * PostgreSQL to read and write Apache Iceberg tables stored on S3
 * or other object storage systems.
 */

#include "postgres.h"

#include "access/tableam.h"
#include "access/heapam.h"
#include "access/amapi.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#ifdef HAVE_ICEBERG_CPP
#include "iceberg_bridge.h"
#endif

PG_MODULE_MAGIC;

/* GUC variables */
/* TODO: Add configuration variables like default S3 endpoint, etc. */

/*
 * Iceberg parallel scan shared state (stored in DSM)
 * This wraps the IcebergParallelState from the bridge plus atomic counter
 */
typedef struct IcebergParallelScanDescData
{
    pg_atomic_uint32    next_task;      /* Next task to assign (atomic) */
    uint32              initialized;    /* Magic to verify initialization */
    /* IcebergParallelState follows (variable length) */
} IcebergParallelScanDescData;

#define ICEBERG_PSCAN_MAGIC 0x49435053  /* "ICPS" */

/*
 * Iceberg scan descriptor - holds state for table scans
 */
typedef struct IcebergScanDescData
{
    TableScanDescData rs_base;  /* Base class - must be first */

    /* Iceberg-specific scan state */
    bool        scan_started;
    int64       current_row;
    int64       total_rows;

    /* Parallel scan state (per-worker) */
    bool        is_parallel;
    uint32      current_task_id;    /* Current task being processed */
    bool        task_exhausted;     /* Current task has no more rows */

#ifdef HAVE_ICEBERG_CPP
    /* iceberg-cpp bridge handles */
    IcebergTableHandle *table_handle;
    IcebergScanHandle  *scan_handle;
    IcebergValue       *row_values;
    int                 num_columns;

    /* Parallel plan (only set for leader during planning) */
    IcebergParallelPlan *parallel_plan;
#endif

} IcebergScanDescData;

typedef IcebergScanDescData *IcebergScanDesc;

/* Forward declarations */
static const TableAmRoutine iceberg_am_methods;

/* ----------------------------------------------------------------
 *                      Scan callbacks
 * ----------------------------------------------------------------
 */

static TableScanDesc
iceberg_scan_begin(Relation relation, Snapshot snapshot,
                   int nkeys, ScanKey key,
                   ParallelTableScanDesc parallel_scan,
                   uint32 flags)
{
    IcebergScanDesc scan;

    scan = (IcebergScanDesc) palloc0(sizeof(IcebergScanDescData));

    scan->rs_base.rs_rd = relation;
    scan->rs_base.rs_snapshot = snapshot;
    scan->rs_base.rs_nkeys = nkeys;
    scan->rs_base.rs_flags = flags;
    scan->rs_base.rs_parallel = parallel_scan;

    scan->scan_started = false;
    scan->current_row = 0;
    scan->total_rows = 0;

    /* Initialize parallel scan state */
    scan->is_parallel = (parallel_scan != NULL);
    scan->current_task_id = 0;
    scan->task_exhausted = true;  /* Force getting first task */

#ifdef HAVE_ICEBERG_CPP
    {
        IcebergError error = {0};

        /*
         * TODO: Parse table options from reloptions
         * For now, use hardcoded values for testing
         */
        const char *location = NULL;     /* Get from reloptions */
        const char *catalog_type = NULL; /* Get from reloptions */
        const char *catalog_uri = NULL;  /* Get from reloptions */

        /* Open the Iceberg table */
        scan->table_handle = iceberg_table_open(location, catalog_type,
                                                 catalog_uri, &error);
        if (scan->table_handle == NULL)
        {
            elog(WARNING, "iceberg_scan_begin: failed to open table: %s",
                 error.message);
        }
        else
        {
            /* Get column count for value array allocation */
            scan->num_columns = RelationGetNumberOfAttributes(relation);
            scan->row_values = (IcebergValue *)
                palloc0(sizeof(IcebergValue) * scan->num_columns);

            /*
             * For parallel scans, don't start the scan here.
             * Each worker will get tasks and open scans in getnextslot.
             *
             * For serial scans, start the scan immediately.
             */
            if (!scan->is_parallel)
            {
                scan->scan_handle = iceberg_scan_begin(scan->table_handle,
                                                        0, /* latest snapshot */
                                                        NULL, /* all columns */
                                                        0,
                                                        &error);
                if (scan->scan_handle == NULL)
                {
                    elog(WARNING, "iceberg_scan_begin: failed to begin scan: %s",
                         error.message);
                }
            }
        }
    }
#endif

    elog(DEBUG1, "iceberg_scan_begin: relation=%s, parallel=%d",
         RelationGetRelationName(relation), scan->is_parallel);

    return (TableScanDesc) scan;
}

static void
iceberg_scan_end(TableScanDesc sscan)
{
    IcebergScanDesc scan = (IcebergScanDesc) sscan;

    elog(DEBUG1, "iceberg_scan_end");

#ifdef HAVE_ICEBERG_CPP
    /* Cleanup iceberg-cpp handles */
    if (scan->scan_handle)
        iceberg_scan_end(scan->scan_handle);

    if (scan->table_handle)
        iceberg_table_close(scan->table_handle);

    if (scan->row_values)
        pfree(scan->row_values);
#endif

    pfree(scan);
}

static void
iceberg_scan_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
                    bool allow_strat, bool allow_sync, bool allow_pagemode)
{
    IcebergScanDesc scan = (IcebergScanDesc) sscan;

    elog(DEBUG1, "iceberg_scan_rescan: parallel=%d", scan->is_parallel);

    scan->scan_started = false;
    scan->current_row = 0;

#ifdef HAVE_ICEBERG_CPP
    /* Close current scan handle */
    if (scan->scan_handle)
    {
        iceberg_scan_end(scan->scan_handle);
        scan->scan_handle = NULL;
    }

    if (scan->is_parallel)
    {
        /*
         * For parallel scans, reset task state.
         * The shared next_task counter is reset by parallelscan_reinitialize.
         */
        scan->current_task_id = 0;
        scan->task_exhausted = true;
    }
    else
    {
        /* For serial scans, reopen the scan */
        IcebergError error = {0};
        scan->scan_handle = iceberg_scan_begin(scan->table_handle,
                                                0, NULL, 0, &error);
    }
#endif
}

/*
 * Helper function to convert IcebergValue to PostgreSQL Datum
 */
#ifdef HAVE_ICEBERG_CPP
static bool
iceberg_convert_values_to_slot(IcebergScanDesc scan, TupleTableSlot *slot)
{
    TupleDesc tupdesc = RelationGetDescr(scan->rs_base.rs_rd);
    int natts = tupdesc->natts;
    Datum *values = slot->tts_values;
    bool *isnull = slot->tts_isnull;
    int i;

    /* Convert IcebergValue to PostgreSQL Datum */
    for (i = 0; i < natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        IcebergValue *val = &scan->row_values[i];

        if (val->is_null)
        {
            isnull[i] = true;
            values[i] = (Datum) 0;
        }
        else
        {
            isnull[i] = false;

            /*
             * Convert based on PostgreSQL target type
             */
            switch (attr->atttypid)
            {
                case INT8OID:
                    values[i] = Int64GetDatum(val->value.int64_val);
                    break;
                case INT4OID:
                    values[i] = Int32GetDatum(val->value.int32_val);
                    break;
                case FLOAT8OID:
                    values[i] = Float8GetDatum(val->value.double_val);
                    break;
                case FLOAT4OID:
                    values[i] = Float4GetDatum(val->value.float_val);
                    break;
                case BOOLOID:
                    values[i] = BoolGetDatum(val->value.bool_val);
                    break;
                case TEXTOID:
                case VARCHAROID:
                    if (val->value.string_val.data)
                    {
                        values[i] = PointerGetDatum(
                            cstring_to_text_with_len(
                                val->value.string_val.data,
                                val->value.string_val.len));
                    }
                    else
                    {
                        isnull[i] = true;
                        values[i] = (Datum) 0;
                    }
                    break;
                case TIMESTAMPOID:
                    /* Iceberg timestamp is microseconds since epoch */
                    values[i] = TimestampGetDatum(
                        val->value.timestamp_val -
                        ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) *
                         USECS_PER_DAY));
                    break;
                default:
                    /* Unsupported type - return null */
                    elog(WARNING, "iceberg_scan: unsupported type %u for column %s",
                         attr->atttypid, NameStr(attr->attname));
                    isnull[i] = true;
                    values[i] = (Datum) 0;
                    break;
            }
        }
    }

    scan->current_row++;
    ExecStoreVirtualTuple(slot);
    return true;
}

/*
 * Get next task for parallel scan
 * Returns task_id, or UINT32_MAX if no more tasks
 */
static uint32
iceberg_parallel_get_next_task(IcebergScanDesc scan)
{
    IcebergParallelScanDescData *pscan;
    const IcebergParallelState *state;
    uint32 task_id;

    if (!scan->rs_base.rs_parallel)
        return UINT32_MAX;

    pscan = (IcebergParallelScanDescData *) scan->rs_base.rs_parallel;

    if (pscan->initialized != ICEBERG_PSCAN_MAGIC)
        return UINT32_MAX;

    /* Get shared state (follows the header) */
    state = (const IcebergParallelState *)
        ((char *) pscan + sizeof(IcebergParallelScanDescData));

    /* Atomically get next task */
    task_id = pg_atomic_fetch_add_u32(&pscan->next_task, 1);

    if (task_id >= state->total_tasks)
        return UINT32_MAX;

    elog(DEBUG2, "iceberg_parallel_get_next_task: worker got task %u of %u",
         task_id, state->total_tasks);

    return task_id;
}

/*
 * Open scan for a specific parallel task
 */
static bool
iceberg_parallel_open_task(IcebergScanDesc scan, uint32 task_id)
{
    IcebergParallelScanDescData *pscan;
    const IcebergParallelState *state;
    IcebergError error = {0};

    pscan = (IcebergParallelScanDescData *) scan->rs_base.rs_parallel;
    state = (const IcebergParallelState *)
        ((char *) pscan + sizeof(IcebergParallelScanDescData));

    /* Close previous task's scan if any */
    if (scan->scan_handle)
    {
        iceberg_scan_end(scan->scan_handle);
        scan->scan_handle = NULL;
    }

    /* Open scan for this task */
    scan->scan_handle = iceberg_parallel_scan_begin(
        scan->table_handle,
        state,
        task_id,
        &error
    );

    if (!scan->scan_handle)
    {
        elog(WARNING, "iceberg_parallel_open_task: failed to open task %u: %s",
             task_id, error.message);
        return false;
    }

    scan->current_task_id = task_id;
    scan->task_exhausted = false;

    return true;
}
#endif /* HAVE_ICEBERG_CPP */

static bool
iceberg_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
                         TupleTableSlot *slot)
{
    IcebergScanDesc scan = (IcebergScanDesc) sscan;

    elog(DEBUG2, "iceberg_scan_getnextslot: row=%ld, is_parallel=%d",
         scan->current_row, scan->is_parallel);

    /* Clear any previous tuple */
    ExecClearTuple(slot);

#ifdef HAVE_ICEBERG_CPP
    if (!scan->row_values || !scan->table_handle)
        return false;

    /*
     * Parallel scan path
     */
    if (scan->is_parallel)
    {
        IcebergError error = {0};
        int natts = RelationGetNumberOfAttributes(scan->rs_base.rs_rd);

        for (;;)
        {
            /* Try to get a row from current task */
            if (scan->scan_handle && !scan->task_exhausted)
            {
                if (iceberg_scan_next(scan->scan_handle, scan->row_values,
                                      natts, &error))
                {
                    return iceberg_convert_values_to_slot(scan, slot);
                }
                /* Current task exhausted */
                scan->task_exhausted = true;
            }

            /* Get next task */
            uint32 task_id = iceberg_parallel_get_next_task(scan);
            if (task_id == UINT32_MAX)
            {
                /* No more tasks */
                return false;
            }

            /* Open the new task */
            if (!iceberg_parallel_open_task(scan, task_id))
            {
                /* Failed to open task, try next one */
                scan->task_exhausted = true;
                continue;
            }
        }
    }

    /*
     * Serial scan path
     */
    if (scan->scan_handle)
    {
        IcebergError error = {0};
        int natts = RelationGetNumberOfAttributes(scan->rs_base.rs_rd);

        if (iceberg_scan_next(scan->scan_handle, scan->row_values, natts, &error))
        {
            return iceberg_convert_values_to_slot(scan, slot);
        }
    }
#endif

    /* No more rows */
    return false;
}

/* ----------------------------------------------------------------
 *                      Tuple manipulation callbacks
 * ----------------------------------------------------------------
 */

static void
iceberg_tuple_insert(Relation relation, TupleTableSlot *slot,
                     CommandId cid, int options, BulkInsertState bistate)
{
    elog(DEBUG1, "iceberg_tuple_insert: relation=%s",
         RelationGetRelationName(relation));

    /*
     * TODO: Implement INSERT
     * - Convert TupleTableSlot to Arrow RecordBatch
     * - Buffer rows for batch writing
     * - On transaction commit, write Parquet file and update Iceberg metadata
     */

    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("INSERT not yet implemented for Iceberg tables")));
}

static void
iceberg_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
                                 CommandId cid, int options,
                                 BulkInsertState bistate, uint32 specToken)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("speculative insertion not supported for Iceberg tables")));
}

static void
iceberg_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
                                   uint32 specToken, bool succeeded)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("speculative insertion not supported for Iceberg tables")));
}

static TM_Result
iceberg_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
                     Snapshot snapshot, Snapshot crosscheck, bool wait,
                     TM_FailureData *tmfd, bool changingPart)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("DELETE not yet implemented for Iceberg tables")));

    return TM_Ok;  /* unreachable */
}

static TM_Result
iceberg_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
                     CommandId cid, Snapshot snapshot, Snapshot crosscheck,
                     bool wait, TM_FailureData *tmfd,
                     LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("UPDATE not yet implemented for Iceberg tables")));

    return TM_Ok;  /* unreachable */
}

static TM_Result
iceberg_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
                   TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
                   LockWaitPolicy wait_policy, uint8 flags,
                   TM_FailureData *tmfd)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("tuple locking not supported for Iceberg tables")));

    return TM_Ok;  /* unreachable */
}

static bool
iceberg_fetch_row_version(Relation relation, ItemPointer tid,
                          Snapshot snapshot, TupleTableSlot *slot)
{
    /*
     * TODO: Implement fetching a specific row by TID
     * For Iceberg, TID could encode file_id + row_offset
     */
    return false;
}

static void
iceberg_get_latest_tid(TableScanDesc sscan, ItemPointer tid)
{
    /* Not meaningful for Iceberg tables */
    ItemPointerSetInvalid(tid);
}

static bool
iceberg_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
    return ItemPointerIsValid(tid);
}

static bool
iceberg_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
                                 Snapshot snapshot)
{
    /*
     * Iceberg handles its own snapshot isolation via table snapshots
     * For now, assume all visible rows satisfy the snapshot
     */
    return true;
}

static TransactionId
iceberg_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("index operations not supported for Iceberg tables")));

    return InvalidTransactionId;
}

/* ----------------------------------------------------------------
 *                      DDL callbacks
 * ----------------------------------------------------------------
 */

static void
iceberg_relation_set_new_filelocator(Relation rel,
                                     const RelFileLocator *newrlocator,
                                     char persistence,
                                     TransactionId *freezeXid,
                                     MultiXactId *minmulti)
{
    elog(DEBUG1, "iceberg_relation_set_new_filelocator: relation=%s",
         RelationGetRelationName(rel));

    /*
     * TODO: Initialize a new Iceberg table
     * - Parse WITH options (location, catalog_type, uri)
     * - Create table in Iceberg catalog
     * - Initialize metadata.json
     */

    *freezeXid = InvalidTransactionId;
    *minmulti = InvalidMultiXactId;
}

static void
iceberg_relation_nontransactional_truncate(Relation rel)
{
    elog(DEBUG1, "iceberg_relation_nontransactional_truncate");

    /*
     * TODO: Implement TRUNCATE
     * - Create new empty snapshot in Iceberg
     */

    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("TRUNCATE not yet implemented for Iceberg tables")));
}

static void
iceberg_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
    /* Used for CLUSTER, not implemented */
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("CLUSTER not supported for Iceberg tables")));
}

static void
iceberg_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
                                  Relation OldIndex, bool use_sort,
                                  TransactionId OldestXmin,
                                  TransactionId *xid_cutoff,
                                  MultiXactId *multi_cutoff,
                                  double *num_tuples,
                                  double *tups_vacuumed,
                                  double *tups_recently_dead)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("CLUSTER not supported for Iceberg tables")));
}

static void
iceberg_vacuum_rel(Relation rel, VacuumParams *params,
                   BufferAccessStrategy bstrategy)
{
    elog(DEBUG1, "iceberg_vacuum_rel: relation=%s",
         RelationGetRelationName(rel));

    /*
     * TODO: Implement VACUUM for Iceberg
     * - Expire old snapshots
     * - Remove orphan files
     * - Compact small files (optional)
     */
}

static bool
iceberg_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
                                BufferAccessStrategy bstrategy)
{
    /* For ANALYZE sampling - not block-based for Iceberg */
    return false;
}

static bool
iceberg_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
                                double *liverows, double *deadrows,
                                TupleTableSlot *slot)
{
    /* For ANALYZE - sample tuples */
    return false;
}

static double
iceberg_index_build_range_scan(Relation tableRelation,
                               Relation indexRelation,
                               IndexInfo *indexInfo,
                               bool allow_sync,
                               bool anyvisible,
                               bool progress,
                               BlockNumber start_blockno,
                               BlockNumber numblocks,
                               IndexBuildCallback callback,
                               void *callback_state,
                               TableScanDesc scan)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("indexes not supported for Iceberg tables")));

    return 0;
}

static void
iceberg_index_validate_scan(Relation tableRelation,
                            Relation indexRelation,
                            IndexInfo *indexInfo,
                            Snapshot snapshot,
                            ValidateIndexState *state)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("indexes not supported for Iceberg tables")));
}

/* ----------------------------------------------------------------
 *                      Planner support callbacks
 * ----------------------------------------------------------------
 */

static Size
iceberg_parallelscan_estimate(Relation rel)
{
#ifdef HAVE_ICEBERG_CPP
    Size        size;

    /*
     * Estimate the size needed for parallel scan state.
     * This is called during planning to determine how much shared memory
     * to allocate in the DSM segment.
     *
     * We need:
     * - IcebergParallelScanDescData header (with atomic counter)
     * - IcebergParallelState (header + files + tasks)
     *
     * We estimate conservatively since we don't know exact file count yet.
     */
    size = sizeof(IcebergParallelScanDescData);

    /*
     * Estimate for typical Iceberg table:
     * - Up to 1000 files
     * - Up to 10 row groups per file = 10000 tasks
     * This is just an estimate; actual size computed in initialize.
     */
    size += sizeof(IcebergParallelState);
    size += 1000 * sizeof(IcebergFileInfo);
    size += 10000 * sizeof(IcebergParallelTask);

    elog(DEBUG1, "iceberg_parallelscan_estimate: size=%zu", size);

    return size;
#else
    return 0;
#endif
}

static Size
iceberg_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
#ifdef HAVE_ICEBERG_CPP
    IcebergParallelScanDescData *ipscan = (IcebergParallelScanDescData *) pscan;
    IcebergError error = {0};
    IcebergTableHandle *table;
    IcebergParallelPlan *plan;
    Size actual_size;
    char *state_buffer;

    elog(DEBUG1, "iceberg_parallelscan_initialize: relation=%s",
         RelationGetRelationName(rel));

    /*
     * Initialize the atomic task counter
     */
    pg_atomic_init_u32(&ipscan->next_task, 0);

    /*
     * Open the Iceberg table to get file list
     * TODO: Get table options from relation
     */
    table = iceberg_table_open(NULL, NULL, NULL, &error);
    if (!table)
    {
        elog(WARNING, "iceberg_parallelscan_initialize: failed to open table: %s",
             error.message);
        ipscan->initialized = 0;
        return sizeof(IcebergParallelScanDescData);
    }

    /*
     * Create parallel scan plan (analyzes files, creates tasks)
     */
    plan = iceberg_parallel_plan_create(table, 0 /* latest snapshot */, &error);
    if (!plan)
    {
        elog(WARNING, "iceberg_parallelscan_initialize: failed to create plan: %s",
             error.message);
        iceberg_table_close(table);
        ipscan->initialized = 0;
        return sizeof(IcebergParallelScanDescData);
    }

    /*
     * Serialize the plan to shared memory (after header)
     */
    state_buffer = (char *) pscan + sizeof(IcebergParallelScanDescData);
    actual_size = iceberg_parallel_plan_serialize(
        plan,
        state_buffer,
        iceberg_parallel_plan_get_size(plan)
    );

    elog(DEBUG1, "iceberg_parallelscan_initialize: files=%u, tasks=%u, size=%zu",
         iceberg_parallel_plan_get_file_count(plan),
         iceberg_parallel_plan_get_task_count(plan),
         actual_size);

    /*
     * Cleanup
     */
    iceberg_parallel_plan_free(plan);
    iceberg_table_close(table);

    /*
     * Mark as initialized
     */
    ipscan->initialized = ICEBERG_PSCAN_MAGIC;

    return sizeof(IcebergParallelScanDescData) + actual_size;
#else
    return 0;
#endif
}

static void
iceberg_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
#ifdef HAVE_ICEBERG_CPP
    IcebergParallelScanDescData *ipscan = (IcebergParallelScanDescData *) pscan;

    elog(DEBUG1, "iceberg_parallelscan_reinitialize");

    /*
     * Reset the task counter to 0 for rescan
     */
    if (ipscan->initialized == ICEBERG_PSCAN_MAGIC)
    {
        pg_atomic_write_u32(&ipscan->next_task, 0);
    }
#endif
}

/* ----------------------------------------------------------------
 *                      Relation size callbacks
 * ----------------------------------------------------------------
 */

static uint64
iceberg_relation_size(Relation rel, ForkNumber forkNumber)
{
    elog(DEBUG1, "iceberg_relation_size: relation=%s",
         RelationGetRelationName(rel));

    /*
     * TODO: Return actual size from Iceberg metadata
     * - Sum of all data files in current snapshot
     */
    return 0;
}

static bool
iceberg_relation_needs_toast_table(Relation rel)
{
    /* Iceberg stores data externally, no TOAST needed */
    return false;
}

static Oid
iceberg_relation_toast_am(Relation rel)
{
    return InvalidOid;
}

/* ----------------------------------------------------------------
 *                      Miscellaneous callbacks
 * ----------------------------------------------------------------
 */

static void
iceberg_estimate_rel_size(Relation rel, int32 *attr_widths,
                          BlockNumber *pages, double *tuples,
                          double *allvisfrac)
{
    elog(DEBUG1, "iceberg_estimate_rel_size: relation=%s",
         RelationGetRelationName(rel));

    /*
     * TODO: Get statistics from Iceberg metadata
     * - row count from snapshot
     * - file sizes for page estimate
     */

    *pages = 1;
    *tuples = 0;
    *allvisfrac = 1.0;
}

static bool
iceberg_scan_bitmap_next_block(TableScanDesc scan,
                               TBMIterateResult *tbmres)
{
    /* Bitmap scans not supported for Iceberg */
    return false;
}

static bool
iceberg_scan_bitmap_next_tuple(TableScanDesc scan,
                               TBMIterateResult *tbmres,
                               TupleTableSlot *slot)
{
    return false;
}

static bool
iceberg_scan_sample_next_block(TableScanDesc scan,
                               SampleScanState *scanstate)
{
    return false;
}

static bool
iceberg_scan_sample_next_tuple(TableScanDesc scan,
                               SampleScanState *scanstate,
                               TupleTableSlot *slot)
{
    return false;
}

/* ----------------------------------------------------------------
 *                      TableAmRoutine definition
 * ----------------------------------------------------------------
 */

static const TableAmRoutine iceberg_am_methods = {
    .type = T_TableAmRoutine,

    /* Slot operations */
    .slot_callbacks = heap_slot_callbacks,  /* Use heap slot for now */

    /* Scan operations */
    .scan_begin = iceberg_scan_begin,
    .scan_end = iceberg_scan_end,
    .scan_rescan = iceberg_scan_rescan,
    .scan_getnextslot = iceberg_scan_getnextslot,

    /* Parallel scan - not implemented yet */
    .parallelscan_estimate = iceberg_parallelscan_estimate,
    .parallelscan_initialize = iceberg_parallelscan_initialize,
    .parallelscan_reinitialize = iceberg_parallelscan_reinitialize,

    /* Index scan - not supported */
    .index_fetch_begin = NULL,
    .index_fetch_reset = NULL,
    .index_fetch_end = NULL,
    .index_fetch_tuple = NULL,

    /* Tuple operations */
    .tuple_fetch_row_version = iceberg_fetch_row_version,
    .tuple_tid_valid = iceberg_tuple_tid_valid,
    .tuple_get_latest_tid = iceberg_get_latest_tid,
    .tuple_satisfies_snapshot = iceberg_tuple_satisfies_snapshot,
    .index_delete_tuples = iceberg_index_delete_tuples,

    /* DML operations */
    .tuple_insert = iceberg_tuple_insert,
    .tuple_insert_speculative = iceberg_tuple_insert_speculative,
    .tuple_complete_speculative = iceberg_tuple_complete_speculative,
    .multi_insert = NULL,  /* Use tuple_insert for now */
    .tuple_delete = iceberg_tuple_delete,
    .tuple_update = iceberg_tuple_update,
    .tuple_lock = iceberg_tuple_lock,

    /* DDL operations */
    .relation_set_new_filelocator = iceberg_relation_set_new_filelocator,
    .relation_nontransactional_truncate = iceberg_relation_nontransactional_truncate,
    .relation_copy_data = iceberg_relation_copy_data,
    .relation_copy_for_cluster = iceberg_relation_copy_for_cluster,
    .relation_vacuum = iceberg_vacuum_rel,
    .scan_analyze_next_block = iceberg_scan_analyze_next_block,
    .scan_analyze_next_tuple = iceberg_scan_analyze_next_tuple,
    .index_build_range_scan = iceberg_index_build_range_scan,
    .index_validate_scan = iceberg_index_validate_scan,

    /* Planner support */
    .relation_size = iceberg_relation_size,
    .relation_needs_toast_table = iceberg_relation_needs_toast_table,
    .relation_toast_am = iceberg_relation_toast_am,
    .relation_fetch_toast_slice = NULL,

    .relation_estimate_size = iceberg_estimate_rel_size,

    /* Bitmap/sample scans - not supported */
    .scan_bitmap_next_block = iceberg_scan_bitmap_next_block,
    .scan_bitmap_next_tuple = iceberg_scan_bitmap_next_tuple,
    .scan_sample_next_block = iceberg_scan_sample_next_block,
    .scan_sample_next_tuple = iceberg_scan_sample_next_tuple,
};

/* ----------------------------------------------------------------
 *                      Handler function
 * ----------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(iceberg_am_handler);

/*
 * iceberg_am_handler - Table Access Method handler function
 *
 * This is the entry point that PostgreSQL calls to get the
 * TableAmRoutine for Iceberg tables.
 */
Datum
iceberg_am_handler(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(&iceberg_am_methods);
}

/* ----------------------------------------------------------------
 *                      Extension initialization
 * ----------------------------------------------------------------
 */

void
_PG_init(void)
{
    elog(LOG, "pg_lakehouse_iceberg: initializing Iceberg Table Access Method");

#ifdef HAVE_ICEBERG_CPP
    {
        IcebergError error = {0};
        if (iceberg_init(&error) != 0)
        {
            elog(WARNING, "pg_lakehouse_iceberg: failed to initialize iceberg-cpp: %s",
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

    /*
     * TODO: Register GUC variables
     * - pg_lakehouse_iceberg.default_s3_endpoint
     * - pg_lakehouse_iceberg.default_catalog_uri
     * - etc.
     */
}

void
_PG_fini(void)
{
    elog(LOG, "pg_lakehouse_iceberg: shutting down");

#ifdef HAVE_ICEBERG_CPP
    iceberg_shutdown();
#endif
}
