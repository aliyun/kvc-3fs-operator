FROM grafana/grafana:11.6.2

USER root
RUN cd /usr/share/grafana && grafana cli plugins install grafana-clickhouse-datasource && mkdir -p /var/lib/grafana/dashboards
COPY ./docker/grafana/datasource.yml /etc/grafana/provisioning/datasources/
COPY ./docker/grafana/dashborad-sample.yml /etc/grafana/provisioning/dashboards/
COPY ./docker/grafana/dashboards/* /var/lib/grafana/dashboards/
COPY ./docker/grafana/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]