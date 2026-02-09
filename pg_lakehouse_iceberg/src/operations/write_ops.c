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
 * write_ops.c
 *      Iceberg write operations implementation
 *
 * Converts PG tuples to Arrow format and writes Parquet files via iceberg_bridge.
 *
 * Buffer strategy:
 *   - Tuples are collected into an in-memory Arrow RecordBatch
 *   - When batch_size is reached, the batch is flushed to a new Parquet file
 *   - The file path and stats are recorded for later snapshot commit
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "operations/write_ops.h"
#include "bridge/iceberg_bridge.h"

/* Default batch size (rows per Arrow batch) */
#define ICEBERG_DEFAULT_BATCH_SIZE  10000

/* Initial capacity for data files array */
#define ICEBERG_INITIAL_FILES_CAPACITY  16

/*
 * Begin writing to a table
 */
IcebergWriteState *
iceberg_write_begin(IcebergTableState *table, Relation relation)
{
    IcebergWriteState  *write;
    IcebergError        error = {0};

    elog(DEBUG1, "iceberg_write_begin: relation=%s",
         RelationGetRelationName(relation));

    write = (IcebergWriteState *) MemoryContextAllocZero(
        TopTransactionMemoryContext, sizeof(IcebergWriteState));

    write->table = table;
    write->relation = relation;
    write->batch_size = ICEBERG_DEFAULT_BATCH_SIZE;
    write->current_rows = 0;
    write->total_rows = 0;
    write->num_data_files = 0;
    write->data_files_capacity = ICEBERG_INITIAL_FILES_CAPACITY;
    write->data_files = (struct DataFileInfo *)
        MemoryContextAllocZero(TopTransactionMemoryContext,
                               sizeof(struct DataFileInfo) * write->data_files_capacity);

    if (table == NULL || table->bridge_handle == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("iceberg: table not opened for writing")));
    }

    /* Create writer handle via bridge */
    TupleDesc tupdesc = RelationGetDescr(relation);
    int num_cols = tupdesc->natts;

    /* Build column names array */
    const char **col_names = (const char **)
        palloc(sizeof(const char *) * num_cols);
    for (int i = 0; i < num_cols; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        col_names[i] = NameStr(attr->attname);
    }

    write->writer_handle = iceberg_bridge_writer_create(
        (IcebergTableHandle *) table->bridge_handle,
        col_names, num_cols, &error);

    pfree(col_names);

    if (write->writer_handle == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("iceberg: failed to create writer: %s", error.message)));
    }

    /* Convert PG schema to Arrow schema via bridge */
    /* TODO: iceberg_bridge_pg_to_arrow_schema(tupdesc, &write->arrow_schema, &error); */

    write->is_open = true;
    return write;
}

/*
 * Write a single tuple
 */
int
iceberg_write_tuple(IcebergWriteState *write, TupleTableSlot *slot)
{
    IcebergError error = {0};

    if (!write || !write->is_open)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("iceberg: write state not open")));
    }

    /*
     * TODO: Convert TupleTableSlot to Arrow format
     *
     * Steps:
     *   1. iceberg_bridge_slot_to_arrow_array(slot, schema, &array, &error)
     *   2. iceberg_bridge_writer_append_batch(writer, &array, &schema, &error)
     *
     * For now, we accumulate row count and will implement Arrow conversion.
     */

    write->current_rows++;
    write->total_rows++;

    /* Auto-flush when batch is full */
    if (write->current_rows >= write->batch_size)
    {
        return iceberg_write_flush(write);
    }

    return 0;
}

/*
 * Flush buffered data to Parquet file
 */
int
iceberg_write_flush(IcebergWriteState *write)
{
    IcebergError    error = {0};
    char           *data_file_path = NULL;
    int64           record_count = 0;
    int64           file_size = 0;

    if (!write || !write->is_open || write->current_rows == 0)
        return 0;

    elog(DEBUG1, "iceberg_write_flush: flushing %d rows", write->current_rows);

    /* Finish writing and get the output file info */
    if (iceberg_bridge_writer_finish(
            (IcebergWriterHandle *) write->writer_handle,
            &data_file_path, &record_count, &file_size, &error) != 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("iceberg: failed to flush writer: %s", error.message)));
    }

    /* Record the produced data file */
    if (write->num_data_files >= write->data_files_capacity)
    {
        write->data_files_capacity *= 2;
        write->data_files = (struct DataFileInfo *)
            repalloc(write->data_files,
                     sizeof(struct DataFileInfo) * write->data_files_capacity);
    }

    struct DataFileInfo *info = &write->data_files[write->num_data_files++];
    info->file_path = data_file_path ? pstrdup(data_file_path) : NULL;
    info->record_count = record_count;
    info->file_size = file_size;

    elog(DEBUG1, "iceberg_write_flush: wrote %s (records=%ld, size=%ld)",
         info->file_path ? info->file_path : "(null)",
         record_count, file_size);

    /* Reset buffer for next batch */
    write->current_rows = 0;

    /* Re-create writer for next batch */
    /* TODO: Need to re-open writer or support multi-batch writing */

    return 0;
}

/*
 * End write and free resources
 */
void
iceberg_write_end(IcebergWriteState *write)
{
    if (write == NULL)
        return;

    if (write->writer_handle)
    {
        iceberg_bridge_writer_destroy((IcebergWriterHandle *) write->writer_handle);
        write->writer_handle = NULL;
    }

    write->is_open = false;
    /* Note: write state itself is in TopTransactionMemoryContext,
     * will be freed at transaction end */
}

/*
 * Get data files produced by writer
 */
int
iceberg_write_get_data_files(IcebergWriteState *write,
                             int *num_files,
                             struct DataFileInfo **files)
{
    if (!write)
    {
        *num_files = 0;
        *files = NULL;
        return -1;
    }

    *num_files = write->num_data_files;
    *files = write->data_files;
    return 0;
}
