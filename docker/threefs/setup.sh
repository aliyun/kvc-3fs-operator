#!/bin/bash

replace_dot_with_underscore() {
    echo "$1" | sed 's/[-.]/_/g'
}

if [ -z "$COMPONENT" ]; then
  echo "Error: no COMPONENT env!"
  exit 1
fi

NODE_NAME=$(replace_dot_with_underscore "$NODE_NAME")
echo "NODE_NAME: ${NODE_NAME}"
NODE_ID=${!NODE_NAME}
echo "NODE_ID: $NODE_ID"

case "$COMPONENT" in
  monitor)
    echo "starting monitor..."
    echo "CLICKHOUSE_HOST: ${CLICKHOUSE_HOST}"
    echo "CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}"
    echo "TCP_PORT: ${TCP_PORT}"
    echo "CLICKHOUSE_USER: ${CLICKHOUSE_USER}"
    sed -i "s|\${CLICKHOUSE_HOST}|${CLICKHOUSE_HOST}|g" /opt/3fs/etc/monitor_collector_main.toml
    sed -i "s|\${CLICKHOUSE_PASSWORD}|${CLICKHOUSE_PASSWORD}|g" /opt/3fs/etc/monitor_collector_main.toml
    sed -i "s|\${TCP_PORT}|${TCP_PORT}|g" /opt/3fs/etc/monitor_collector_main.toml
    sed -i "s|\${CLICKHOUSE_USER}|${CLICKHOUSE_USER}|g" /opt/3fs/etc/monitor_collector_main.toml

    /opt/3fs/bin/monitor_collector_main --cfg /opt/3fs/etc/monitor_collector_main.toml
    ;;
  mgmtd)
    echo "starting mgmtd..."
    echo "$FDB_CLUSTER_FILE_CONTENTS" > /opt/3fs/etc/fdb.cluster
    sed -i "s|\${NODE_ID}|${NODE_ID}|g" /opt/3fs/etc/mgmtd_main_app.toml

    /opt/3fs/bin/mgmtd_main --launcher_cfg /opt/3fs/etc/mgmtd_main_launcher.toml \
    --app-cfg /opt/3fs/etc/mgmtd_main_app.toml
    ;;
  meta)
    echo "starting meta..."
    echo "MGMTD_SERVER_ADDRESS: ${MGMTD_SERVER_ADDRESS}"
    echo "$FDB_CLUSTER_FILE_CONTENTS" > /opt/3fs/etc/fdb.cluster
    sed -i "s|\${NODE_ID}|${NODE_ID}|g" /opt/3fs/etc/meta_main_app.toml
    sed -i "s|\${MGMTD_SERVER_ADDRESS}|${MGMTD_SERVER_ADDRESS}|g" /opt/3fs/etc/meta_main_launcher.toml

    /opt/3fs/bin/meta_main --launcher_cfg /opt/3fs/etc/meta_main_launcher.toml \
    --app-cfg /opt/3fs/etc/meta_main_app.toml
    ;;
  storage)
    echo "starting storage..."
    sed -i "s|\${NODE_ID}|${NODE_ID}|g" /opt/3fs/etc/storage_main_app.toml
    sed -i "s|\${MGMTD_SERVER_ADDRESS}|${MGMTD_SERVER_ADDRESS}|g" /opt/3fs/etc/storage_main_launcher.toml

    /opt/3fs/bin/storage_main --launcher_cfg /opt/3fs/etc/storage_main_launcher.toml \
    --app-cfg /opt/3fs/etc/storage_main_app.toml
    ;;
  fuse)
    echo "starting fuse..."
    sed -i "s|\${MOUNT_POINT}|${MOUNT_POINT}|g" /opt/3fs/etc/hf3fs_fuse_main_launcher.toml
    sed -i "s|\${MGMTD_SERVER_ADDRESS}|${MGMTD_SERVER_ADDRESS}|g" /opt/3fs/etc/hf3fs_fuse_main_launcher.toml

    mkdir -p ${MOUNT_POINT}
    mkdir -p /opt/3fs/token/
    echo "$TOKEN" > /opt/3fs/token/token
    /opt/3fs/bin/hf3fs_fuse_main --launcher_cfg /opt/3fs/etc/hf3fs_fuse_main_launcher.toml
    ;;
  *)
    echo "Error: no support component '$COMPONENT'！"
    exit 1
    ;;
esac
