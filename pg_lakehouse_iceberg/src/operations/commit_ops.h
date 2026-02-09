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
 * commit_ops.h
 *      Iceberg transaction commit and abort operations
 *
 * These functions handle the Iceberg-level transaction lifecycle:
 *   - COMMIT: Create a new Iceberg snapshot with the data files from write_ops,
 *             then update the catalog entry via catalog_bridge.
 *   - ABORT:  Clean up orphan data files written during the transaction.
 *
 * Called by pg_lakehouse_core's xact_hooks.c during PRE_COMMIT / ABORT callbacks.
 *
 * Transaction flow:
 *   PRE_COMMIT →
 *     1. Flush remaining write buffers (write_ops.c)
 *     2. Build Iceberg snapshot with new data files
 *     3. Commit via catalog_bridge (PgCatalog::UpdateTable)
 *        - SELECT ... FOR UPDATE (lock lakehouse_tables row)
 *        - Write new metadata.json to S3
 *        - UPDATE lakehouse_tables SET metadata_location = ...
 *     4. If catalog commit fails → raise error → PG rolls back everything
 *
 *   ABORT →
 *     1. Delete any Parquet data files written during this transaction
 *     2. Release all handles
 */

#ifndef ICEBERG_COMMIT_OPS_H
#define ICEBERG_COMMIT_OPS_H

#include "postgres.h"

#include "operations/table_ops.h"
#include "operations/write_ops.h"

/*
 * Transaction context for Iceberg commit.
 *
 * Aggregates all write state from a single PG transaction,
 * potentially across multiple Iceberg tables.
 */
typedef struct IcebergTransactionContext
{
    /* Array of per-table write states (one per table written in this txn) */
    IcebergWriteState **write_states;
    int                 num_write_states;
    int                 capacity;
} IcebergTransactionContext;

/*
 * Get or create the transaction context for the current PG transaction.
 *
 * The context is allocated in TopTransactionMemoryContext and is automatically
 * cleaned up at transaction end.
 */
extern IcebergTransactionContext *iceberg_get_transaction_context(void);

/*
 * Register a write state into the current transaction context.
 *
 * Called by write_ops when a new write begins for a table.
 */
extern void iceberg_register_write_state(IcebergTransactionContext *ctx,
                                         IcebergWriteState *write);

/*
 * Commit the Iceberg transaction.
 *
 * Called during PRE_COMMIT callback.
 *
 * For each table that had writes in this transaction:
 *   1. Flush remaining write buffers
 *   2. Create Iceberg Transaction via catalog_bridge
 *   3. FastAppend all data files
 *   4. Commit the transaction (writes metadata.json + updates lakehouse_tables)
 *
 * Returns 0 on success. On failure, raises ERROR to abort PG transaction.
 */
extern int iceberg_commit(IcebergTransactionContext *ctx);

/*
 * Abort the Iceberg transaction.
 *
 * Called during ABORT callback.
 *
 * For each table that had writes:
 *   1. Delete any orphan Parquet files already written to S3
 *   2. Close all handles
 */
extern void iceberg_abort(IcebergTransactionContext *ctx);

/*
 * Clean up orphan data files from a failed write.
 *
 * Best-effort: logs warnings if files cannot be deleted.
 */
extern void iceberg_cleanup_orphan_files(IcebergWriteState *write);

#endif /* ICEBERG_COMMIT_OPS_H */
