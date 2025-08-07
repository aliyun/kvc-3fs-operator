FROM vcns-registry.cn-hangzhou.cr.aliyuncs.com/vcns/ubuntu:22.04

COPY ./docker/fdb/foundationdb-clients_7.3.63-1_amd64.deb /
COPY ./docker/fdb/foundationdb-server_7.3.63-1_amd64.deb /
COPY ./docker/tini_v0.19.0 /tini
COPY ./docker/fdb/entrypoint.sh /entrypoint.sh

RUN dpkg -i foundationdb-clients_7.3.63-1_amd64.deb && dpkg -i foundationdb-server_7.3.63-1_amd64.deb
COPY ./docker/fdb/foundationdb.conf /etc/foundationdb/foundationdb.conf
RUN chmod +x /tini
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/tini", "--", "/entrypoint.sh"]