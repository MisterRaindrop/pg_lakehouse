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
 * scan_ops.c
 *      Iceberg scan operations implementation
 *
 * Bridges PG's tuple-at-a-time scan interface with iceberg-cpp's
 * Arrow-batch-based reading.
 *
 * Data flow:
 *   iceberg-cpp → ArrowArrayStream → ArrowArray (batch) → row-by-row → TupleTableSlot
 */

#include "postgres.h"

#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#include "operations/scan_ops.h"
#include "bridge/iceberg_bridge.h"

/*
 * Internal scan state structure
 */
struct IcebergScanState
{
    IcebergTableState  *table;
    Relation            relation;
    bool                is_parallel;

    /* Serial scan state */
    int64               current_row;

    /* Bridge handles */
    IcebergScanHandle  *bridge_scan;
    IcebergValue       *row_values;
    int                 num_columns;

    /* Parallel scan state (per-worker) */
    uint32              current_task_id;
    bool                task_exhausted;
    ParallelTableScanDesc parallel_scan;
};

/*
 * Parallel scan descriptor stored in DSM
 */
typedef struct IcebergParallelScanDescData
{
    pg_atomic_uint32    next_task;       /* Next task to assign (atomic) */
    uint32              initialized;     /* Magic to verify initialization */
    /* IcebergParallelState follows (variable length) */
} IcebergParallelScanDescData;

#define ICEBERG_PSCAN_MAGIC 0x49435053  /* "ICPS" */

/*
 * Convert IcebergValue array to TupleTableSlot
 */
static bool
convert_values_to_slot(IcebergScanState *scan, TupleTableSlot *slot)
{
    TupleDesc   tupdesc = RelationGetDescr(scan->relation);
    int         natts = tupdesc->natts;
    Datum      *values = slot->tts_values;
    bool       *isnull = slot->tts_isnull;
    int         i;

    for (i = 0; i < natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        IcebergValue *val = &scan->row_values[i];

        if (val->is_null)
        {
            isnull[i] = true;
            values[i] = (Datum) 0;
            continue;
        }

        isnull[i] = false;

        switch (attr->atttypid)
        {
            case INT8OID:
                values[i] = Int64GetDatum(val->value.int64_val);
                break;
            case INT4OID:
                values[i] = Int32GetDatum(val->value.int32_val);
                break;
            case INT2OID:
                values[i] = Int16GetDatum((int16) val->value.int32_val);
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
                /* Iceberg timestamp: microseconds since Unix epoch */
                values[i] = TimestampGetDatum(
                    val->value.timestamp_val -
                    ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) *
                     USECS_PER_DAY));
                break;
            case DATEOID:
                /* Iceberg date: days since Unix epoch */
                values[i] = DateADTGetDatum(
                    val->value.date_val -
                    (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE));
                break;
            default:
                elog(WARNING, "iceberg_scan: unsupported type %u for column %s",
                     attr->atttypid, NameStr(attr->attname));
                isnull[i] = true;
                values[i] = (Datum) 0;
                break;
        }
    }

    scan->current_row++;
    ExecStoreVirtualTuple(slot);
    return true;
}

/*
 * Begin a scan
 */
IcebergScanState *
iceberg_scan_begin(IcebergTableState *table,
                   Relation relation,
                   Snapshot snapshot,
                   int nkeys, ScanKey key,
                   ParallelTableScanDesc parallel_scan,
                   uint32 flags)
{
    IcebergScanState   *scan;
    IcebergError        error = {0};

    scan = (IcebergScanState *) palloc0(sizeof(IcebergScanState));
    scan->table = table;
    scan->relation = relation;
    scan->is_parallel = (parallel_scan != NULL);
    scan->parallel_scan = parallel_scan;
    scan->current_row = 0;
    scan->current_task_id = 0;
    scan->task_exhausted = true;

    if (table == NULL || table->bridge_handle == NULL)
    {
        elog(DEBUG1, "iceberg_scan_begin: no table handle, returning empty scan");
        return scan;
    }

    /* Allocate value array for row conversion */
    scan->num_columns = RelationGetNumberOfAttributes(relation);
    scan->row_values = (IcebergValue *)
        palloc0(sizeof(IcebergValue) * scan->num_columns);

    /* For serial scans, start scanning immediately */
    if (!scan->is_parallel)
    {
        scan->bridge_scan = iceberg_bridge_scan_begin(
            (IcebergTableHandle *) table->bridge_handle,
            0,      /* latest snapshot */
            NULL,   /* all columns */
            0,
            &error);

        if (scan->bridge_scan == NULL)
        {
            elog(WARNING, "iceberg_scan_begin: failed to begin scan: %s",
                 error.message);
        }
    }

    elog(DEBUG1, "iceberg_scan_begin: relation=%s, parallel=%d",
         RelationGetRelationName(relation), scan->is_parallel);

    return scan;
}

/*
 * Get the next row
 */
bool
iceberg_scan_next(IcebergScanState *scan,
                  ScanDirection direction,
                  TupleTableSlot *slot)
{
    IcebergError error = {0};

    ExecClearTuple(slot);

    if (!scan || !scan->row_values)
        return false;

    /* Parallel scan path */
    if (scan->is_parallel)
    {
        for (;;)
        {
            /* Try to read from current task */
            if (scan->bridge_scan && !scan->task_exhausted)
            {
                if (iceberg_bridge_scan_next(scan->bridge_scan,
                                             scan->row_values,
                                             scan->num_columns, &error))
                {
                    return convert_values_to_slot(scan, slot);
                }
                scan->task_exhausted = true;
            }

            /* Get next task */
            uint32 task_id = iceberg_parallel_next_task(scan->parallel_scan);
            if (task_id == UINT32_MAX)
                return false;  /* No more tasks */

            /* Close previous scan if any */
            if (scan->bridge_scan)
            {
                iceberg_bridge_scan_end(scan->bridge_scan);
                scan->bridge_scan = NULL;
            }

            /* Open scan for new task */
            IcebergParallelScanDescData *pscan =
                (IcebergParallelScanDescData *) scan->parallel_scan;
            const IcebergParallelState *state = (const IcebergParallelState *)
                ((char *) pscan + sizeof(IcebergParallelScanDescData));

            scan->bridge_scan = iceberg_bridge_parallel_scan_begin(
                (IcebergTableHandle *) scan->table->bridge_handle,
                state, task_id, &error);

            if (!scan->bridge_scan)
            {
                elog(WARNING, "iceberg_scan: failed to open task %u: %s",
                     task_id, error.message);
                scan->task_exhausted = true;
                continue;
            }

            scan->current_task_id = task_id;
            scan->task_exhausted = false;
        }
    }

    /* Serial scan path */
    if (scan->bridge_scan)
    {
        if (iceberg_bridge_scan_next(scan->bridge_scan,
                                     scan->row_values,
                                     scan->num_columns, &error))
        {
            return convert_values_to_slot(scan, slot);
        }
    }

    return false;
}

/*
 * End a scan
 */
void
iceberg_scan_end(IcebergScanState *scan)
{
    if (scan == NULL)
        return;

    elog(DEBUG1, "iceberg_scan_end: %ld rows scanned", scan->current_row);

    if (scan->bridge_scan)
        iceberg_bridge_scan_end(scan->bridge_scan);

    if (scan->row_values)
        pfree(scan->row_values);

    pfree(scan);
}

/*
 * Rescan
 */
void
iceberg_scan_rescan(IcebergScanState *scan)
{
    IcebergError error = {0};

    if (scan == NULL)
        return;

    scan->current_row = 0;

    if (scan->bridge_scan)
    {
        iceberg_bridge_scan_end(scan->bridge_scan);
        scan->bridge_scan = NULL;
    }

    if (scan->is_parallel)
    {
        scan->current_task_id = 0;
        scan->task_exhausted = true;
        /* Shared state reset happens in parallelscan_reinitialize */
    }
    else if (scan->table && scan->table->bridge_handle)
    {
        scan->bridge_scan = iceberg_bridge_scan_begin(
            (IcebergTableHandle *) scan->table->bridge_handle,
            0, NULL, 0, &error);
    }
}

/* ═══ Parallel Scan ═══ */

Size
iceberg_parallelscan_estimate(IcebergTableState *table)
{
    Size size;

    size = sizeof(IcebergParallelScanDescData);

    /* Conservative estimate for typical Iceberg tables */
    size += sizeof(IcebergParallelState);
    size += 1000 * sizeof(IcebergFileInfo);
    size += 10000 * sizeof(IcebergParallelTask);

    elog(DEBUG1, "iceberg_parallelscan_estimate: size=%zu", size);
    return size;
}

Size
iceberg_parallelscan_initialize(IcebergTableState *table,
                                Relation rel,
                                ParallelTableScanDesc pscan)
{
    IcebergParallelScanDescData *ipscan = (IcebergParallelScanDescData *) pscan;
    IcebergError        error = {0};
    IcebergParallelPlan *plan;
    Size                 actual_size;
    char                *state_buffer;

    elog(DEBUG1, "iceberg_parallelscan_initialize: relation=%s",
         RelationGetRelationName(rel));

    pg_atomic_init_u32(&ipscan->next_task, 0);

    if (table == NULL || table->bridge_handle == NULL)
    {
        ipscan->initialized = 0;
        return sizeof(IcebergParallelScanDescData);
    }

    plan = iceberg_bridge_parallel_plan_create(
        (IcebergTableHandle *) table->bridge_handle,
        0, &error);

    if (!plan)
    {
        elog(WARNING, "iceberg_parallelscan_initialize: failed to create plan: %s",
             error.message);
        ipscan->initialized = 0;
        return sizeof(IcebergParallelScanDescData);
    }

    state_buffer = (char *) pscan + sizeof(IcebergParallelScanDescData);
    actual_size = iceberg_bridge_parallel_plan_serialize(
        plan, state_buffer,
        iceberg_bridge_parallel_plan_get_size(plan));

    elog(DEBUG1, "iceberg_parallelscan_initialize: files=%u, tasks=%u, size=%zu",
         iceberg_bridge_parallel_plan_get_file_count(plan),
         iceberg_bridge_parallel_plan_get_task_count(plan),
         actual_size);

    iceberg_bridge_parallel_plan_free(plan);
    ipscan->initialized = ICEBERG_PSCAN_MAGIC;

    return sizeof(IcebergParallelScanDescData) + actual_size;
}

void
iceberg_parallelscan_reinitialize(ParallelTableScanDesc pscan)
{
    IcebergParallelScanDescData *ipscan = (IcebergParallelScanDescData *) pscan;

    if (ipscan->initialized == ICEBERG_PSCAN_MAGIC)
        pg_atomic_write_u32(&ipscan->next_task, 0);
}

uint32
iceberg_parallel_next_task(ParallelTableScanDesc pscan)
{
    IcebergParallelScanDescData *ipscan;
    const IcebergParallelState  *state;
    uint32 task_id;

    if (!pscan)
        return UINT32_MAX;

    ipscan = (IcebergParallelScanDescData *) pscan;

    if (ipscan->initialized != ICEBERG_PSCAN_MAGIC)
        return UINT32_MAX;

    state = (const IcebergParallelState *)
        ((char *) ipscan + sizeof(IcebergParallelScanDescData));

    task_id = pg_atomic_fetch_add_u32(&ipscan->next_task, 1);

    if (task_id >= state->total_tasks)
        return UINT32_MAX;

    elog(DEBUG2, "iceberg_parallel_next_task: got task %u of %u",
         task_id, state->total_tasks);

    return task_id;
}

/*
 * Plan fragments (for FormatHandler interface)
 */
int
iceberg_plan_fragments(IcebergTableState *table, IcebergFragmentList **out)
{
    /* TODO: Implement fragment planning for non-parallel scans */
    *out = NULL;
    return 0;
}

void
iceberg_free_fragments(IcebergFragmentList *fragments)
{
    if (fragments)
    {
        if (fragments->fragments)
            pfree(fragments->fragments);
        pfree(fragments);
    }
}
