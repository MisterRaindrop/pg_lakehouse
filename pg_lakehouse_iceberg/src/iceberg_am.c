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
 *      PostgreSQL Table Access Method (AM) for Iceberg tables.
 *
 * This file implements the TableAmRoutine and its handler function.
 * All format-specific logic is delegated to the operations/ modules:
 *   - operations/scan_ops.c    – scan begin / getnextslot / end / rescan
 *   - operations/write_ops.c   – tuple_insert / delete / update
 *   - operations/table_ops.c   – DDL: create, drop, truncate, rename
 *   - operations/commit_ops.c  – transaction commit / abort
 */

#include "postgres.h"

#include "access/tableam.h"
#include "access/heapam.h"
#include "access/amapi.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"

/* Delegated operations */
#include "operations/scan_ops.h"
#include "operations/write_ops.h"
#include "operations/table_ops.h"

/* Forward declarations */
static const TableAmRoutine iceberg_am_methods;

/* ================================================================
 *                      Slot callbacks
 * ================================================================
 */

static const TupleTableSlotOps *
iceberg_slot_callbacks(Relation relation)
{
    /*
     * We use virtual tuple slots because Iceberg rows are synthesised
     * from Parquet/ORC/Avro columns, not from PG heap buffers.
     */
    return &TTSOpsVirtual;
}

/* ================================================================
 *                      Scan callbacks
 *
 * Delegated to operations/scan_ops.c
 * ================================================================
 */

static TableScanDesc
iceberg_scan_begin(Relation relation, Snapshot snapshot,
                   int nkeys, ScanKey key,
                   ParallelTableScanDesc parallel_scan,
                   uint32 flags)
{
    return iceberg_ops_scan_begin(relation, snapshot, nkeys, key,
                                 parallel_scan, flags);
}

static void
iceberg_scan_end(TableScanDesc sscan)
{
    iceberg_ops_scan_end(sscan);
}

static void
iceberg_scan_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
                    bool allow_strat, bool allow_sync, bool allow_pagemode)
{
    iceberg_ops_scan_rescan(sscan, key, set_params,
                            allow_strat, allow_sync, allow_pagemode);
}

static bool
iceberg_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
                         TupleTableSlot *slot)
{
    return iceberg_ops_scan_getnextslot(sscan, direction, slot);
}

/* ================================================================
 *                      Tuple manipulation callbacks
 *
 * Delegated to operations/write_ops.c
 * ================================================================
 */

static void
iceberg_tuple_insert(Relation relation, TupleTableSlot *slot,
                     CommandId cid, int options, BulkInsertState bistate)
{
    iceberg_ops_tuple_insert(relation, slot, cid, options, bistate);
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
    return iceberg_ops_tuple_delete(relation, tid, cid, snapshot,
                                    crosscheck, wait, tmfd, changingPart);
}

static TM_Result
iceberg_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
                     CommandId cid, Snapshot snapshot, Snapshot crosscheck,
                     bool wait, TM_FailureData *tmfd,
                     LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
    return iceberg_ops_tuple_update(relation, otid, slot, cid, snapshot,
                                    crosscheck, wait, tmfd,
                                    lockmode, update_indexes);
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
    return TM_Ok; /* unreachable */
}

/* ================================================================
 *                      Row-version / TID callbacks
 * ================================================================
 */

static bool
iceberg_fetch_row_version(Relation relation, ItemPointer tid,
                          Snapshot snapshot, TupleTableSlot *slot)
{
    /* Iceberg does not support row-version fetch by TID */
    return false;
}

static void
iceberg_get_latest_tid(TableScanDesc sscan, ItemPointer tid)
{
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
     * Iceberg handles snapshot isolation at the table-metadata level;
     * once a row reaches the executor it is always "visible".
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

/* ================================================================
 *                      DDL callbacks
 *
 * Delegated to operations/table_ops.c
 * ================================================================
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
     * Called during CREATE TABLE.
     * Actual Iceberg table creation is handled in table_ops.c
     * via the event trigger / utility hook path.
     */

    *freezeXid = InvalidTransactionId;
    *minmulti = InvalidMultiXactId;
}

static void
iceberg_relation_nontransactional_truncate(Relation rel)
{
    iceberg_ops_truncate_table(rel);
}

static void
iceberg_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
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
     * TODO: Implement VACUUM for Iceberg:
     *   - Expire old snapshots
     *   - Remove orphan files
     *   - Compact small files (optional)
     */
}

/* ================================================================
 *                      ANALYZE callbacks
 * ================================================================
 */

static bool
iceberg_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
                                BufferAccessStrategy bstrategy)
{
    return false;
}

static bool
iceberg_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
                                double *liverows, double *deadrows,
                                TupleTableSlot *slot)
{
    return false;
}

/* ================================================================
 *                      Index callbacks (not supported)
 * ================================================================
 */

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

/* ================================================================
 *                      Parallel scan callbacks
 * ================================================================
 */

static Size
iceberg_parallelscan_estimate(Relation rel)
{
    /* TODO: Delegate to scan_ops for real size estimation */
    return 0;
}

static Size
iceberg_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
    /* TODO: Delegate to scan_ops */
    return 0;
}

static void
iceberg_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
    /* TODO: Delegate to scan_ops */
}

/* ================================================================
 *                      Relation size / TOAST
 * ================================================================
 */

static uint64
iceberg_relation_size(Relation rel, ForkNumber forkNumber)
{
    elog(DEBUG1, "iceberg_relation_size: relation=%s",
         RelationGetRelationName(rel));
    /* TODO: Sum data file sizes from Iceberg metadata */
    return 0;
}

static bool
iceberg_relation_needs_toast_table(Relation rel)
{
    return false;
}

static Oid
iceberg_relation_toast_am(Relation rel)
{
    return InvalidOid;
}

/* ================================================================
 *                      Planner support
 * ================================================================
 */

static void
iceberg_estimate_rel_size(Relation rel, int32 *attr_widths,
                          BlockNumber *pages, double *tuples,
                          double *allvisfrac)
{
    elog(DEBUG1, "iceberg_estimate_rel_size: relation=%s",
         RelationGetRelationName(rel));

    /* TODO: Get statistics from Iceberg metadata */
    *pages = 1;
    *tuples = 0;
    *allvisfrac = 1.0;
}

/* ================================================================
 *                      Bitmap / sample scans (not supported)
 * ================================================================
 */

static bool
iceberg_scan_bitmap_next_block(TableScanDesc scan,
                               TBMIterateResult *tbmres)
{
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

/* ================================================================
 *                      TableAmRoutine definition
 * ================================================================
 */

static const TableAmRoutine iceberg_am_methods = {
    .type = T_TableAmRoutine,

    /* Slot */
    .slot_callbacks = iceberg_slot_callbacks,

    /* Scan */
    .scan_begin = iceberg_scan_begin,
    .scan_end = iceberg_scan_end,
    .scan_rescan = iceberg_scan_rescan,
    .scan_getnextslot = iceberg_scan_getnextslot,

    /* Parallel scan */
    .parallelscan_estimate = iceberg_parallelscan_estimate,
    .parallelscan_initialize = iceberg_parallelscan_initialize,
    .parallelscan_reinitialize = iceberg_parallelscan_reinitialize,

    /* Index fetch — not supported */
    .index_fetch_begin = NULL,
    .index_fetch_reset = NULL,
    .index_fetch_end = NULL,
    .index_fetch_tuple = NULL,

    /* Row version / TID */
    .tuple_fetch_row_version = iceberg_fetch_row_version,
    .tuple_tid_valid = iceberg_tuple_tid_valid,
    .tuple_get_latest_tid = iceberg_get_latest_tid,
    .tuple_satisfies_snapshot = iceberg_tuple_satisfies_snapshot,
    .index_delete_tuples = iceberg_index_delete_tuples,

    /* DML */
    .tuple_insert = iceberg_tuple_insert,
    .tuple_insert_speculative = iceberg_tuple_insert_speculative,
    .tuple_complete_speculative = iceberg_tuple_complete_speculative,
    .multi_insert = NULL,
    .tuple_delete = iceberg_tuple_delete,
    .tuple_update = iceberg_tuple_update,
    .tuple_lock = iceberg_tuple_lock,

    /* DDL */
    .relation_set_new_filelocator = iceberg_relation_set_new_filelocator,
    .relation_nontransactional_truncate = iceberg_relation_nontransactional_truncate,
    .relation_copy_data = iceberg_relation_copy_data,
    .relation_copy_for_cluster = iceberg_relation_copy_for_cluster,
    .relation_vacuum = iceberg_vacuum_rel,
    .scan_analyze_next_block = iceberg_scan_analyze_next_block,
    .scan_analyze_next_tuple = iceberg_scan_analyze_next_tuple,
    .index_build_range_scan = iceberg_index_build_range_scan,
    .index_validate_scan = iceberg_index_validate_scan,

    /* Size / TOAST */
    .relation_size = iceberg_relation_size,
    .relation_needs_toast_table = iceberg_relation_needs_toast_table,
    .relation_toast_am = iceberg_relation_toast_am,
    .relation_fetch_toast_slice = NULL,

    .relation_estimate_size = iceberg_estimate_rel_size,

    /* Bitmap / sample scans — not supported */
    .scan_bitmap_next_block = iceberg_scan_bitmap_next_block,
    .scan_bitmap_next_tuple = iceberg_scan_bitmap_next_tuple,
    .scan_sample_next_block = iceberg_scan_sample_next_block,
    .scan_sample_next_tuple = iceberg_scan_sample_next_tuple,
};

/* ================================================================
 *                      Handler function
 * ================================================================
 */

PG_FUNCTION_INFO_V1(iceberg_am_handler);

/*
 * iceberg_am_handler — Table Access Method entry point.
 *
 * PostgreSQL calls this function to obtain the TableAmRoutine.
 */
Datum
iceberg_am_handler(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(&iceberg_am_methods);
}
