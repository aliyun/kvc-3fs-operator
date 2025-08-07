#!/bin/bash

sed -i "s/\${TCP_PORT}/$TCP_PORT/g" $CLICKHOUSE_CONFIG
chown -R clickhouse:clickhouse /var/lib/clickhouse
chown -R clickhouse:clickhouse /var/log/clickhouse-server

# if no args passed to `docker run` or first argument start with `--`, then the user is passing clickhouse-server arguments
if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]]; then
    # This replaces the shell script with the server:
    exec /exec_sql.sh &
    exec clickhouse su "clickhouse:clickhouse" clickhouse-server --config-file="$CLICKHOUSE_CONFIG" "$@"
fi

# Otherwise, we assume the user want to run his own process, for example a `bash` shell to explore this image
exec "$@"

# vi: ts=4: sw=4: sts=4: expandtab