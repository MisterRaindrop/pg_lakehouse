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
 * write_ops.h
 *      Iceberg write operations (INSERT path)
 *
 * These functions implement the write-path portion of FormatHandler for Iceberg.
 * Rows are buffered into Arrow RecordBatches and flushed as Parquet files.
 *
 * Write lifecycle:
 *   1. iceberg_write_begin()   - allocate buffers, open writer
 *   2. iceberg_write_tuple()   - buffer one row (PG Datum → Arrow)
 *   3. iceberg_write_flush()   - when buffer is full or on commit, flush to Parquet
 *
 * The actual Iceberg snapshot commit is handled by commit_ops.c.
 */

#ifndef ICEBERG_WRITE_OPS_H
#define ICEBERG_WRITE_OPS_H

#include "postgres.h"

#include "executor/tuptable.h"
#include "utils/rel.h"

#include "operations/table_ops.h"

/*
 * Write handle (per-transaction write state)
 */
typedef struct IcebergWriteState
{
    IcebergTableState  *table;          /* The table being written to */
    Relation            relation;       /* PG relation (for schema info) */

    /* Buffering state */
    int                 batch_size;     /* Max rows per batch (default: 10000) */
    int                 current_rows;   /* Rows in current buffer */
    int                 total_rows;     /* Total rows written this transaction */

    /* Arrow batch buffer (opaque, managed by bridge) */
    void               *arrow_schema;   /* ArrowSchema* */
    void               *batch_buffer;   /* Internal buffer for building batches */

    /* Writer handle from iceberg_bridge */
    void               *writer_handle;  /* IcebergWriterHandle* */

    /* Data files produced by this writer */
    int                 num_data_files;
    struct DataFileInfo {
        char   *file_path;
        int64   record_count;
        int64   file_size;
    }                  *data_files;
    int                 data_files_capacity;

    bool                is_open;
} IcebergWriteState;

/*
 * Begin writing to an Iceberg table.
 *
 * Allocates write buffers and opens the writer.
 * Called when the first INSERT is executed within a transaction.
 *
 * Returns write state, or NULL on failure.
 */
extern IcebergWriteState *iceberg_write_begin(IcebergTableState *table,
                                              Relation relation);

/*
 * Write a single tuple to the buffer.
 *
 * Converts PG TupleTableSlot to Arrow format and adds to buffer.
 * When buffer reaches batch_size, auto-flushes to Parquet.
 *
 * Returns 0 on success.
 */
extern int iceberg_write_tuple(IcebergWriteState *write,
                               TupleTableSlot *slot);

/*
 * Flush buffered data to a Parquet file.
 *
 * Writes the current Arrow batch to a new Parquet file on S3/local storage.
 * Records the produced DataFile for later snapshot commit.
 *
 * Returns 0 on success.
 */
extern int iceberg_write_flush(IcebergWriteState *write);

/*
 * Close the write state and free resources.
 *
 * Does NOT commit the Iceberg transaction. Use commit_ops for that.
 * Any unflushed data is discarded (caller should flush before closing).
 */
extern void iceberg_write_end(IcebergWriteState *write);

/*
 * Get the list of data files produced by this writer.
 *
 * Used by commit_ops to build the snapshot.
 */
extern int iceberg_write_get_data_files(IcebergWriteState *write,
                                        int *num_files,
                                        struct DataFileInfo **files);

#endif /* ICEBERG_WRITE_OPS_H */
