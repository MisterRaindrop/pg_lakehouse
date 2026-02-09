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
 * commit_ops.c
 *      Iceberg transaction commit/abort implementation
 *
 * Handles the Iceberg-level transaction lifecycle:
 *   COMMIT: Build Iceberg snapshot → catalog_bridge → UpdateTable
 *   ABORT:  Delete orphan Parquet files
 *
 * This is called by pg_lakehouse_core's xact_hooks.c during
 * PRE_COMMIT / ABORT callbacks.
 */

#include "postgres.h"

#include "utils/memutils.h"

#include "operations/commit_ops.h"
#include "catalog/catalog_bridge.h"
#include "bridge/iceberg_bridge.h"

/* Per-transaction context (stored in TopTransactionMemoryContext) */
static IcebergTransactionContext *current_txn_ctx = NULL;

/*
 * Get or create the transaction context
 */
IcebergTransactionContext *
iceberg_get_transaction_context(void)
{
    if (current_txn_ctx == NULL)
    {
        MemoryContext old = MemoryContextSwitchTo(TopTransactionMemoryContext);

        current_txn_ctx = (IcebergTransactionContext *)
            palloc0(sizeof(IcebergTransactionContext));
        current_txn_ctx->capacity = 4;
        current_txn_ctx->write_states = (IcebergWriteState **)
            palloc0(sizeof(IcebergWriteState *) * current_txn_ctx->capacity);

        MemoryContextSwitchTo(old);
    }

    return current_txn_ctx;
}

/*
 * Register a write state in the transaction context
 */
void
iceberg_register_write_state(IcebergTransactionContext *ctx,
                             IcebergWriteState *write)
{
    if (ctx->num_write_states >= ctx->capacity)
    {
        ctx->capacity *= 2;
        ctx->write_states = (IcebergWriteState **)
            repalloc(ctx->write_states,
                     sizeof(IcebergWriteState *) * ctx->capacity);
    }

    ctx->write_states[ctx->num_write_states++] = write;
}

/*
 * Commit a single table's writes
 */
static int
commit_table_writes(IcebergWriteState *write)
{
    IcebergError                error = {0};
    IcebergTransactionHandle   *txn = NULL;
    IcebergAppendHandle        *append = NULL;
    int                         num_files;
    struct DataFileInfo        *files;
    int                         i;

    /* Flush any remaining buffered data */
    if (write->current_rows > 0)
    {
        if (iceberg_write_flush(write) != 0)
            return -1;
    }

    /* Get data files */
    if (iceberg_write_get_data_files(write, &num_files, &files) != 0)
        return -1;

    if (num_files == 0)
    {
        elog(DEBUG1, "iceberg_commit: no data files to commit");
        return 0;
    }

    elog(DEBUG1, "iceberg_commit: committing %d data files, %d total rows",
         num_files, write->total_rows);

    /* Create Iceberg Transaction via catalog_bridge */
    txn = pg_catalog_new_transaction(
        (IcebergTableHandle *) write->table->bridge_handle,
        &error);

    if (txn == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("iceberg: failed to create transaction: %s",
                        error.message)));
    }

    /* Create FastAppend action */
    append = pg_catalog_new_fast_append(txn, &error);
    if (append == NULL)
    {
        pg_catalog_transaction_abort(txn);
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("iceberg: failed to create fast append: %s",
                        error.message)));
    }

    /* Add all data files to the append */
    for (i = 0; i < num_files; i++)
    {
        if (pg_catalog_append_data_file(append,
                                        files[i].file_path,
                                        files[i].file_size,
                                        files[i].record_count,
                                        &error) != 0)
        {
            pg_catalog_transaction_abort(txn);
            ereport(ERROR,
                    (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                     errmsg("iceberg: failed to add data file: %s",
                            error.message)));
        }
    }

    /* Commit the append (internally calls append.Commit()) */
    if (pg_catalog_append_commit(append, &error) != 0)
    {
        pg_catalog_transaction_abort(txn);
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("iceberg: failed to commit append: %s",
                        error.message)));
    }

    /* Commit the transaction (calls Transaction::Commit() → Catalog::UpdateTable()) */
    if (pg_catalog_transaction_commit(txn, &error) != 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("iceberg: failed to commit transaction: %s",
                        error.message)));
    }

    elog(LOG, "iceberg_commit: committed %d files, %d rows",
         num_files, write->total_rows);

    return 0;
}

/*
 * Commit all Iceberg writes in this PG transaction
 */
int
iceberg_commit(IcebergTransactionContext *ctx)
{
    int i;

    if (ctx == NULL || ctx->num_write_states == 0)
        return 0;

    elog(DEBUG1, "iceberg_commit: committing %d table writes",
         ctx->num_write_states);

    for (i = 0; i < ctx->num_write_states; i++)
    {
        if (commit_table_writes(ctx->write_states[i]) != 0)
            return -1;  /* Error already reported via ereport */
    }

    /* Clean up write state handles */
    for (i = 0; i < ctx->num_write_states; i++)
    {
        iceberg_write_end(ctx->write_states[i]);
    }

    /* Reset transaction context */
    current_txn_ctx = NULL;

    return 0;
}

/*
 * Abort the Iceberg transaction
 */
void
iceberg_abort(IcebergTransactionContext *ctx)
{
    int i;

    if (ctx == NULL)
        return;

    elog(DEBUG1, "iceberg_abort: cleaning up %d table writes",
         ctx->num_write_states);

    for (i = 0; i < ctx->num_write_states; i++)
    {
        IcebergWriteState *write = ctx->write_states[i];

        /* Delete orphan data files */
        iceberg_cleanup_orphan_files(write);

        /* Close writer handle */
        iceberg_write_end(write);
    }

    /* Reset transaction context */
    current_txn_ctx = NULL;
}

/*
 * Clean up orphan data files
 */
void
iceberg_cleanup_orphan_files(IcebergWriteState *write)
{
    int     i;

    if (write == NULL || write->num_data_files == 0)
        return;

    elog(DEBUG1, "iceberg_cleanup_orphan_files: deleting %d orphan files",
         write->num_data_files);

    for (i = 0; i < write->num_data_files; i++)
    {
        struct DataFileInfo *info = &write->data_files[i];

        if (info->file_path)
        {
            /*
             * TODO: Delete the file via FileIO
             *
             * iceberg_file_io_delete(handle, info->file_path, &error);
             */
            elog(DEBUG1, "iceberg_cleanup_orphan_files: would delete %s",
                 info->file_path);
        }
    }
}
