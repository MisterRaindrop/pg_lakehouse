#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

# Initialize PostgreSQL if running as root and data directory is empty
if [ "$(id -u)" = '0' ] && [ -z "$(ls -A /var/lib/postgresql/data 2>/dev/null)" ]; then
    echo "Initializing PostgreSQL database..."
    chown -R postgres:postgres /var/lib/postgresql
    su - postgres -c "/usr/lib/postgresql/${PG_MAJOR}/bin/initdb -D /var/lib/postgresql/data"
fi

# If first argument is 'postgres', run PostgreSQL
if [ "$1" = 'postgres' ]; then
    if [ "$(id -u)" = '0' ]; then
        exec su - postgres -c "/usr/lib/postgresql/${PG_MAJOR}/bin/postgres -D /var/lib/postgresql/data"
    else
        exec /usr/lib/postgresql/${PG_MAJOR}/bin/postgres -D /var/lib/postgresql/data
    fi
fi

# Otherwise execute the command
exec "$@"
