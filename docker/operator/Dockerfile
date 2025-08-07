FROM vcns-registry.cn-hangzhou.cr.aliyuncs.com/vcns/golang:alpine AS builder

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN go mod download

COPY . .
RUN GOOS=linux GOARCH=amd64 go build -o cmd/threefs-operator/tfsc-operator cmd/threefs-operator/main.go


FROM vcns-registry.cn-hangzhou.cr.aliyuncs.com/vcns/threefs-operator-base:ubuntu22.04-cli-base

RUN mkdir -p /opt/3fs/data_placement
COPY --from=builder /app/cmd/threefs-operator/tfsc-operator /
COPY ./docker/operator/clickhouse.sql /opt/3fs/etc/clickhouse.sql
COPY ./docker/operator/meta_main.toml /opt/3fs/etc/meta_main_temp.toml
COPY ./docker/operator/mgmtd_main.toml /opt/3fs/etc/mgmtd_main_temp.toml
COPY ./docker/operator/storage_main.toml /opt/3fs/etc/storage_main_temp.toml
COPY ./docker/operator/hf3fs_fuse_main.toml /opt/3fs/etc/hf3fs_fuse_main_temp.toml
COPY ./docker/operator/data_placement.py /opt/3fs/data_placement/data_placement.py
COPY ./docker/operator/gen_chain_table.py /opt/3fs/data_placement/gen_chain_table.py

RUN chmod +x /tfsc-operator

ENTRYPOINT ["/tini","-s", "-g","--", "/tfsc-operator"]
