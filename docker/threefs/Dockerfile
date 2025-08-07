FROM vcns-registry.cn-hangzhou.cr.aliyuncs.com/vcns/threefs:0530-base

COPY ./docker/threefs/setup.sh /
COPY ./docker/threefs/ibdev2netdev /usr/sbin/ibdev2netdev
COPY ./docker/threefs/mgmtd* /opt/3fs/etc/
COPY ./docker/threefs/meta* /opt/3fs/etc/
COPY ./docker/threefs/storage* /opt/3fs/etc/
COPY ./docker/threefs/hf3fs* /opt/3fs/etc/
COPY ./docker/threefs/monitor* /opt/3fs/etc/
COPY ./docker/threefs/check_mount.sh /
COPY ./docker/threefs/check_umount.sh /

RUN apt update && apt install gnupg wget -y && \
    wget -qO - https://mirrors.aliyun.com/erdma/GPGKEY | gpg --dearmour -o /etc/apt/trusted.gpg.d/erdma.gpg && \
    echo "deb [ ] https://mirrors.aliyun.com/erdma/apt/ubuntu jammy/erdma main" | tee /etc/apt/sources.list.d/erdma.list && \
    apt update && apt install libibverbs1 ibverbs-providers ibverbs-utils librdmacm1 -y  -y
RUN chmod +x /setup.sh
RUN chmod +x /check_mount.sh
RUN chmod +x /check_umount.sh
RUN chmod +x /usr/sbin/ibdev2netdev

ENTRYPOINT ["/tini","-s", "-g","--", "/setup.sh"]