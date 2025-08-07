FROM vcns-registry.cn-hangzhou.cr.aliyuncs.com/vcns/clickhouse:25.1-jammy-base

COPY ./docker/clickhouse/config.xml /etc/clickhouse-server/config.xml
COPY ./docker/clickhouse/clickhouse.sql /
COPY ./docker/clickhouse/entrypoint.sh /
COPY ./docker/clickhouse/exec_sql.sh /
COPY ./docker/tini_v0.19.0 /tini

RUN chmod +x /tini
RUN chmod +x /entrypoint.sh
RUN chmod +x /exec_sql.sh

ENTRYPOINT ["/tini", "--", "/entrypoint.sh"]