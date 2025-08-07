#!/bin/bash

# 等待 ClickHouse 服务启动完成
echo "Waiting for ClickHouse to start..."
until clickhouse-client --port $TCP_PORT --query "SELECT 1"; do
    sleep 1
done

echo "ClickHouse started successfully."

# 检查是否有初始化 SQL 文件
if [ -f "/clickhouse.sql" ]; then
    echo "Executing clickhouse.sql..."
    clickhouse-client --port $TCP_PORT --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -n < /clickhouse.sql
    echo "clickhouse.sql executed successfully."
fi