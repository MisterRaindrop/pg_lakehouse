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
 * s3_io.c
 *      S3 specific FileIO configuration
 *
 * Handles S3/MinIO credential configuration for Iceberg table access.
 * Credentials can come from:
 *   1. GUC variables (pg_lakehouse_iceberg.s3_*)
 *   2. Environment variables (AWS_ACCESS_KEY_ID, etc.)
 *   3. Instance profile / IAM role
 */

#include "postgres.h"

#include "io/file_io.h"

/*
 * Configure S3 credentials from explicit parameters.
 *
 * Used when credentials are provided via GUC or WITH options.
 */
int
iceberg_s3_configure(const char *access_key,
                     const char *secret_key,
                     const char *session_token,
                     IcebergError *error)
{
    if (!access_key || !secret_key)
    {
        if (error)
        {
            error->code = -1;
            snprintf(error->message, sizeof(error->message),
                     "access_key and secret_key are required");
        }
        return -1;
    }

    /*
     * TODO: Store credentials for use by C++ FileIO
     *
     * Options:
     *   1. Set environment variables (temporary, for Arrow S3)
     *   2. Pass to ArrowFileSystemFileIO via S3Options
     */

    elog(DEBUG1, "iceberg_s3_configure: configured S3 credentials");
    return 0;
}

/*
 * Configure S3 credentials from environment variables.
 *
 * Reads AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN.
 */
int
iceberg_s3_configure_from_env(IcebergError *error)
{
    const char *access_key = getenv("AWS_ACCESS_KEY_ID");
    const char *secret_key = getenv("AWS_SECRET_ACCESS_KEY");

    if (!access_key || !secret_key)
    {
        if (error)
        {
            error->code = -1;
            snprintf(error->message, sizeof(error->message),
                     "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY not set");
        }
        return -1;
    }

    elog(DEBUG1, "iceberg_s3_configure_from_env: using environment credentials");
    return 0;
}

/*
 * Configure S3 endpoint (for MinIO / custom S3-compatible services).
 *
 * @param endpoint    S3 endpoint URL (e.g., "http://localhost:9000")
 * @param path_style  Use path-style addressing (needed for MinIO)
 */
int
iceberg_s3_configure_endpoint(const char *endpoint,
                              bool path_style,
                              IcebergError *error)
{
    if (!endpoint)
    {
        if (error)
        {
            error->code = -1;
            snprintf(error->message, sizeof(error->message),
                     "endpoint is required");
        }
        return -1;
    }

    /*
     * TODO: Store endpoint configuration for use by C++ FileIO
     */

    elog(DEBUG1, "iceberg_s3_configure_endpoint: %s (path_style=%d)",
         endpoint, path_style);
    return 0;
}
