#!/bin/bash
set -x

echo 'Download ElasticSearch'
mkdir /tmp/elasticsearch-dir                         && \
cd /tmp/elasticsearch-dir                             && \
curl -sS https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.0.0.zip > es.zip && \
unzip es.zip                                  && \
rm es.zip && \
./elasticsearch-6.0.0/bin/elasticsearch -d -p pid


