FROM vcns-registry.cn-hangzhou.cr.aliyuncs.com/vcns/generate-keys:base

WORKDIR /tmp

COPY docker/generate-keys/script.sh /tmp
COPY docker/generate-keys/mutate_webhook.yaml.template /tmp
COPY docker/generate-keys/validate_webhook.yaml.template /tmp
RUN chmod +x /tmp/script.sh
ENV PATH="/tmp:${PATH}"

ENTRYPOINT [ "/tmp/script.sh" ]
