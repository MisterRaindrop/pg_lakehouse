-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- pg_lakehouse_iceberg extension installation script
-- Provides Iceberg Table Access Method for PostgreSQL

-- Complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_lakehouse_iceberg" to load this file. \quit

-- Create the Access Method handler function
CREATE OR REPLACE FUNCTION iceberg_am_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME', 'iceberg_am_handler'
LANGUAGE C STRICT;

-- Register the Iceberg Table Access Method
CREATE ACCESS METHOD iceberg_am
    TYPE TABLE
    HANDLER iceberg_am_handler;

COMMENT ON ACCESS METHOD iceberg_am IS 'Apache Iceberg table access method';

-- Helper function to check Iceberg table metadata (for debugging)
-- TODO: Implement this function
-- CREATE OR REPLACE FUNCTION iceberg_table_info(regclass)
-- RETURNS TABLE (
--     location TEXT,
--     snapshot_id BIGINT,
--     schema_id INT,
--     total_records BIGINT,
--     total_files INT,
--     total_size BIGINT
-- )
-- AS 'MODULE_PATHNAME', 'iceberg_table_info'
-- LANGUAGE C STRICT;

-- COMMENT ON FUNCTION iceberg_table_info(regclass) IS
--     'Returns metadata information for an Iceberg table';
