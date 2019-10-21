#!/bin/bash

source_name=

if [ $# -gt 0 ]; then
    echo "File name detected: $1"
    source_name=$1
else
    echo "Please provide the path to the yml config file"
fi

cd aurum-datadiscovery/ddprofiler

echo $'\n'Load db into elasticsearch$'\n'
./run.sh --sources ../../access-db-movies.yml

