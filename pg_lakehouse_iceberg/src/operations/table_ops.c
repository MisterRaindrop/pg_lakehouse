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
 * table_ops.c
 *      Iceberg DDL operations implementation
 *
 * Implements create/open/drop table operations for Iceberg format.
 * Uses catalog_bridge for catalog operations and iceberg_bridge for
 * table info queries.
 */

#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "utils/builtins.h"
#include "utils/uuid.h"

#include "operations/table_ops.h"
#include "catalog/catalog_bridge.h"
#include "bridge/iceberg_bridge.h"

/* Default values */
#define ICEBERG_DEFAULT_FILE_FORMAT     "parquet"
#define ICEBERG_DEFAULT_TARGET_SIZE     (128 * 1024 * 1024)  /* 128 MB */

/*
 * Parse a single WITH option into IcebergTableOptions
 */
static int
parse_single_option(DefElem *opt, IcebergTableOptions *out, char **errmsg)
{
    const char *name = opt->defname;
    const char *val = defGetString(opt);

    if (strcmp(name, "location") == 0)
    {
        out->location = pstrdup(val);
    }
    else if (strcmp(name, "warehouse") == 0)
    {
        out->warehouse = pstrdup(val);
    }
    else if (strcmp(name, "catalog_type") == 0)
    {
        if (strcmp(val, "internal") != 0 &&
            strcmp(val, "rest") != 0 &&
            strcmp(val, "hadoop") != 0)
        {
            *errmsg = psprintf("invalid catalog_type '%s', must be 'internal', 'rest', or 'hadoop'", val);
            return -1;
        }
        out->catalog_type = pstrdup(val);
    }
    else if (strcmp(name, "catalog_uri") == 0)
    {
        out->catalog_uri = pstrdup(val);
    }
    else if (strcmp(name, "file_format") == 0)
    {
        if (strcmp(val, "parquet") != 0 &&
            strcmp(val, "orc") != 0 &&
            strcmp(val, "avro") != 0)
        {
            *errmsg = psprintf("invalid file_format '%s', must be 'parquet', 'orc', or 'avro'", val);
            return -1;
        }
        out->file_format = pstrdup(val);
    }
    else if (strcmp(name, "target_file_size") == 0)
    {
        out->target_file_size = pg_strtoint32(val);
        if (out->target_file_size <= 0)
        {
            *errmsg = pstrdup("target_file_size must be positive");
            return -1;
        }
    }
    else if (strcmp(name, "format") == 0)
    {
        /* 'format' is handled by the framework, skip */
    }
    else
    {
        *errmsg = psprintf("unrecognized iceberg option '%s'", name);
        return -1;
    }

    return 0;
}

/*
 * Parse reloptions into IcebergTableOptions
 */
int
iceberg_parse_options(List *options, IcebergTableOptions *out)
{
    ListCell *lc;

    memset(out, 0, sizeof(IcebergTableOptions));

    /* Set defaults */
    out->catalog_type = pstrdup("internal");
    out->file_format = pstrdup(ICEBERG_DEFAULT_FILE_FORMAT);
    out->target_file_size = ICEBERG_DEFAULT_TARGET_SIZE;

    foreach(lc, options)
    {
        DefElem *opt = (DefElem *) lfirst(lc);
        char *errmsg = NULL;

        if (parse_single_option(opt, out, &errmsg) != 0)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("iceberg option error: %s", errmsg)));
        }
    }

    return 0;
}

/*
 * Validate WITH(...) options
 */
int
iceberg_validate_options(List *options, IcebergTableOptions *out, char **errmsg)
{
    ListCell *lc;

    memset(out, 0, sizeof(IcebergTableOptions));
    out->catalog_type = pstrdup("internal");
    out->file_format = pstrdup(ICEBERG_DEFAULT_FILE_FORMAT);
    out->target_file_size = ICEBERG_DEFAULT_TARGET_SIZE;

    foreach(lc, options)
    {
        DefElem *opt = (DefElem *) lfirst(lc);

        if (parse_single_option(opt, out, errmsg) != 0)
            return -1;
    }

    /* location is required */
    if (out->location == NULL && out->warehouse == NULL)
    {
        *errmsg = pstrdup("either 'location' or 'warehouse' option is required for iceberg tables");
        return -1;
    }

    /* If only warehouse is specified, generate location from relation name */
    if (out->location == NULL && out->warehouse != NULL)
    {
        /* Location will be generated during create_table using schema.tablename */
    }

    /* REST catalog requires catalog_uri */
    if (out->catalog_type && strcmp(out->catalog_type, "rest") == 0 &&
        out->catalog_uri == NULL)
    {
        *errmsg = pstrdup("'catalog_uri' is required when catalog_type='rest'");
        return -1;
    }

    return 0;
}

/*
 * Generate a UUID string
 */
void
iceberg_generate_uuid(char *uuid_out, size_t size)
{
    /* Use PG's gen_random_uuid if available, else simplified version */
    snprintf(uuid_out, size,
             "%08x-%04x-%04x-%04x-%012lx",
             (uint32) random(),
             (uint16) random(),
             (uint16) (0x4000 | (random() & 0x0FFF)),  /* version 4 */
             (uint16) (0x8000 | (random() & 0x3FFF)),  /* variant 1 */
             (unsigned long) (((uint64) random() << 16) | random()));
}

/*
 * Create a new Iceberg table
 */
int
iceberg_create_table(Relation rel, List *options)
{
    IcebergTableOptions opts;
    IcebergError        error = {0};
    PgCatalogHandle    *catalog;
    char                uuid[64];
    char               *location;

    elog(DEBUG1, "iceberg_create_table: relation=%s",
         RelationGetRelationName(rel));

    /* Parse and validate options */
    {
        char *errmsg = NULL;
        if (iceberg_validate_options(options, &opts, &errmsg) != 0)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("iceberg: %s", errmsg)));
        }
    }

    /* Generate table location if only warehouse specified */
    if (opts.location == NULL)
    {
        const char *schema_name = get_namespace_name(RelationGetNamespace(rel));
        const char *table_name = RelationGetRelationName(rel);

        location = psprintf("%s/%s/%s", opts.warehouse, schema_name, table_name);
        opts.location = location;
    }

    /* Generate table UUID */
    iceberg_generate_uuid(uuid, sizeof(uuid));

    /* Create catalog handle */
    catalog = pg_catalog_create(opts.warehouse ? opts.warehouse : opts.location, &error);
    if (catalog == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("iceberg: failed to create catalog: %s", error.message)));
    }

    /* Build schema JSON from PG relation */
    /* TODO: Convert TupleDesc to Iceberg schema JSON */
    const char *schema_json = "{}";  /* Placeholder */

    /* Create table in catalog (writes metadata.json + inserts into lakehouse_tables) */
    if (pg_catalog_create_table(catalog,
                                get_namespace_name(RelationGetNamespace(rel)),
                                RelationGetRelationName(rel),
                                schema_json,
                                opts.location,
                                &error) != 0)
    {
        pg_catalog_destroy(catalog);
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("iceberg: failed to create table: %s", error.message)));
    }

    pg_catalog_destroy(catalog);

    elog(LOG, "iceberg_create_table: created %s at %s",
         RelationGetRelationName(rel), opts.location);

    return 0;
}

/*
 * Open an existing Iceberg table
 */
IcebergTableState *
iceberg_open_table(Oid relid)
{
    IcebergTableState  *state;
    IcebergError        error = {0};

    state = (IcebergTableState *) MemoryContextAllocZero(
        CurrentMemoryContext, sizeof(IcebergTableState));
    state->relid = relid;

    /*
     * TODO: Load table options from lakehouse_tables via catalog_bridge
     *
     * Steps:
     *   1. pg_catalog_load_table() → get metadata_location
     *   2. iceberg_bridge_table_open() → open table handle
     *   3. Cache schema info
     */

    /* For now, open via bridge with metadata from catalog */
    state->catalog_handle = pg_catalog_create(NULL /* TODO: get warehouse */, &error);
    if (state->catalog_handle == NULL)
    {
        elog(WARNING, "iceberg_open_table: failed to create catalog: %s",
             error.message);
        pfree(state);
        return NULL;
    }

    /* Load table from catalog */
    Relation rel = RelationIdGetRelation(relid);
    if (rel == NULL)
    {
        pfree(state);
        return NULL;
    }

    state->bridge_handle = pg_catalog_load_table(
        (PgCatalogHandle *) state->catalog_handle,
        get_namespace_name(RelationGetNamespace(rel)),
        RelationGetRelationName(rel),
        &error);

    RelationClose(rel);

    if (state->bridge_handle == NULL)
    {
        elog(WARNING, "iceberg_open_table: failed to load table: %s",
             error.message);
        pg_catalog_destroy((PgCatalogHandle *) state->catalog_handle);
        pfree(state);
        return NULL;
    }

    state->is_open = true;
    return state;
}

/*
 * Close an Iceberg table state
 */
void
iceberg_close_table(IcebergTableState *state)
{
    if (state == NULL)
        return;

    if (state->bridge_handle)
        iceberg_bridge_table_close((IcebergTableHandle *) state->bridge_handle);

    if (state->catalog_handle)
        pg_catalog_destroy((PgCatalogHandle *) state->catalog_handle);

    pfree(state);
}

/*
 * Drop an Iceberg table
 */
int
iceberg_drop_table(Oid relid, bool purge)
{
    IcebergError    error = {0};
    PgCatalogHandle *catalog;
    Relation         rel;

    rel = RelationIdGetRelation(relid);
    if (rel == NULL)
        return -1;

    catalog = pg_catalog_create(NULL /* TODO */, &error);
    if (catalog == NULL)
    {
        RelationClose(rel);
        elog(WARNING, "iceberg_drop_table: failed to create catalog: %s",
             error.message);
        return -1;
    }

    if (pg_catalog_drop_table(catalog,
                              get_namespace_name(RelationGetNamespace(rel)),
                              RelationGetRelationName(rel),
                              purge,
                              &error) != 0)
    {
        elog(WARNING, "iceberg_drop_table: failed to drop table: %s",
             error.message);
        pg_catalog_destroy(catalog);
        RelationClose(rel);
        return -1;
    }

    pg_catalog_destroy(catalog);
    RelationClose(rel);

    elog(LOG, "iceberg_drop_table: dropped table (oid=%u, purge=%d)", relid, purge);
    return 0;
}

/*
 * Get metadata location
 */
int
iceberg_get_metadata_location(Oid relid, char **location)
{
    /*
     * TODO: Query lakehouse_tables for metadata_location
     *
     * SELECT metadata_location FROM pg_lakehouse_catalog.lakehouse_tables
     * WHERE relid = $1;
     */
    *location = NULL;
    return -1;
}
