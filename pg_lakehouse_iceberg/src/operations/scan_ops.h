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
 * scan_ops.h
 *      Iceberg scan operations (serial and parallel)
 *
 * These functions implement the read-path portion of FormatHandler for Iceberg.
 * They bridge between PG's TupleTableSlot and iceberg-cpp's Arrow-based scanning.
 */

#ifndef ICEBERG_SCAN_OPS_H
#define ICEBERG_SCAN_OPS_H

#include "postgres.h"

#include "access/tableam.h"
#include "executor/tuptable.h"
#include "utils/rel.h"

#include "operations/table_ops.h"

/*
 * Opaque scan handle (wraps iceberg-cpp scan state)
 */
typedef struct IcebergScanState IcebergScanState;

/*
 * Fragment list for scan planning
 */
typedef struct IcebergFragmentList
{
    int         num_fragments;  /* Number of file fragments */
    void      **fragments;      /* Array of opaque fragment handles */
    Size        dsm_size;       /* Estimated DSM size for parallel scan */
} IcebergFragmentList;

/*
 * Plan fragments for scanning.
 *
 * Analyzes the table's current snapshot and produces a list of file
 * fragments (data files or row groups) that need to be scanned.
 *
 * For serial scans, returns all fragments as a single list.
 * For parallel scans, fragments can be distributed to workers.
 *
 * Returns 0 on success.
 */
extern int iceberg_plan_fragments(IcebergTableState *table,
                                  IcebergFragmentList **out);

/*
 * Free a fragment list.
 */
extern void iceberg_free_fragments(IcebergFragmentList *fragments);

/*
 * Begin a scan on one fragment (or all fragments for serial scan).
 *
 * Parameters:
 *   table     - opened table state
 *   relation  - PG relation (for column info)
 *   snapshot  - PG snapshot (for MVCC, unused by Iceberg itself)
 *   nkeys     - number of scan keys (for predicate pushdown)
 *   key       - scan keys
 *   parallel_scan - parallel scan descriptor (NULL for serial)
 *   flags     - scan flags
 *
 * Returns scan state, or NULL on failure.
 */
extern IcebergScanState *iceberg_scan_begin(IcebergTableState *table,
                                            Relation relation,
                                            Snapshot snapshot,
                                            int nkeys, ScanKey key,
                                            ParallelTableScanDesc parallel_scan,
                                            uint32 flags);

/*
 * Get the next row from a scan.
 *
 * Converts Arrow columnar data to a PG TupleTableSlot row.
 *
 * Returns true if a row was produced, false if scan is exhausted.
 */
extern bool iceberg_scan_next(IcebergScanState *scan,
                              ScanDirection direction,
                              TupleTableSlot *slot);

/*
 * End a scan and release resources.
 */
extern void iceberg_scan_end(IcebergScanState *scan);

/*
 * Rescan (restart) the scan.
 */
extern void iceberg_scan_rescan(IcebergScanState *scan);

/* ═══ Parallel Scan Support ═══ */

/*
 * Estimate shared memory needed for parallel scan.
 */
extern Size iceberg_parallelscan_estimate(IcebergTableState *table);

/*
 * Initialize parallel scan state in shared memory.
 *
 * The leader creates the scan plan and serializes it to DSM.
 */
extern Size iceberg_parallelscan_initialize(IcebergTableState *table,
                                            Relation rel,
                                            ParallelTableScanDesc pscan);

/*
 * Reinitialize parallel scan (for rescan).
 */
extern void iceberg_parallelscan_reinitialize(ParallelTableScanDesc pscan);

/*
 * Get the next task for a parallel worker.
 *
 * Returns task_id, or UINT32_MAX if no more tasks.
 */
extern uint32 iceberg_parallel_next_task(ParallelTableScanDesc pscan);

#endif /* ICEBERG_SCAN_OPS_H */
