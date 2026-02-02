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

-- Basic regression test for pg_lakehouse_iceberg

-- Test extension loading
CREATE EXTENSION pg_lakehouse_iceberg;

-- Verify access method is registered
SELECT amname, amtype FROM pg_am WHERE amname = 'iceberg_am';

-- Test creating a table with iceberg_am
-- Note: This will fail until we implement relation_set_new_filelocator properly
-- CREATE TABLE test_iceberg (
--     id BIGINT,
--     name TEXT,
--     value DECIMAL,
--     ts TIMESTAMP
-- ) USING iceberg_am;

-- Test basic scan (empty table)
-- SELECT * FROM test_iceberg;

-- Cleanup
-- DROP TABLE IF EXISTS test_iceberg;
DROP EXTENSION pg_lakehouse_iceberg;
