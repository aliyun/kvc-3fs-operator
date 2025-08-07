#!/bin/bash

sed -i "s|\${HOST}|${HOST}|g" /etc/grafana/provisioning/datasources/datasource.yml
sed -i "s|\${PORT}|${PORT}|g" /etc/grafana/provisioning/datasources/datasource.yml
sed -i "s|\${USERNAME}|${USERNAME}|g" /etc/grafana/provisioning/datasources/datasource.yml
sed -i "s|\${PASSWORD}|${PASSWORD}|g" /etc/grafana/provisioning/datasources/datasource.yml

grafana server --homepath=/usr/share/grafana --config=/etc/grafana/grafana.ini