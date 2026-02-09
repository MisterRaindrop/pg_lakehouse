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
 * file_io.h
 *      C interface for file I/O operations
 *
 * Provides a C-compatible FileIO abstraction that wraps iceberg-cpp's
 * FileIO for use by operations/*.c code.
 *
 * Supports local filesystem and S3 storage.
 */

#ifndef ICEBERG_FILE_IO_H
#define ICEBERG_FILE_IO_H

#include "bridge/iceberg_bridge.h"  /* For IcebergError */

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle for FileIO */
typedef struct IcebergFileIOHandle IcebergFileIOHandle;

/*
 * Create a FileIO instance.
 *
 * @param scheme    Storage scheme: "file" (local), "s3", "gs"
 * @param endpoint  Object store endpoint (for S3, NULL for local)
 * @param region    AWS region (for S3, NULL for local)
 * @param handle    Output handle
 * @param error     Error info on failure
 * @return 0 on success
 */
int iceberg_file_io_create(const char *scheme,
                           const char *endpoint,
                           const char *region,
                           IcebergFileIOHandle **handle,
                           IcebergError *error);

/*
 * Destroy a FileIO instance.
 */
void iceberg_file_io_destroy(IcebergFileIOHandle *handle);

/*
 * Read file contents.
 *
 * @param handle   FileIO handle
 * @param path     File path (local or S3 URI)
 * @param content  Output: file content (caller must free with pfree)
 * @param length   Output: content length
 * @param error    Error info on failure
 * @return 0 on success
 */
int iceberg_file_io_read(IcebergFileIOHandle *handle,
                         const char *path,
                         char **content,
                         size_t *length,
                         IcebergError *error);

/*
 * Write file contents.
 *
 * @param handle   FileIO handle
 * @param path     File path
 * @param content  Data to write
 * @param length   Data length
 * @param error    Error info on failure
 * @return 0 on success
 */
int iceberg_file_io_write(IcebergFileIOHandle *handle,
                          const char *path,
                          const char *content,
                          size_t length,
                          IcebergError *error);

/*
 * Delete a file.
 *
 * @return 0 on success
 */
int iceberg_file_io_delete(IcebergFileIOHandle *handle,
                           const char *path,
                           IcebergError *error);

/*
 * Check if a file exists.
 *
 * @return true if file exists
 */
bool iceberg_file_io_exists(IcebergFileIOHandle *handle,
                            const char *path);

#ifdef __cplusplus
}
#endif

#endif /* ICEBERG_FILE_IO_H */
