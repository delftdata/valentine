#!/bin/bash

set -x

if [ $# -gt 0 ]; then
    echo "File name detected: $1"
    echo "Data name detected: $2"
    source_name=$1
    data_name=$2
else
    echo "Please provide the path to the yml config file"
fi

echo 'Run Elastic Search Script'
DIRECTORY=/tmp/elasticsearch-dir
if [[ -d "$DIRECTORY" ]]
then
  rm -rf $DIRECTORY
fi

path="$(dirname "$0")"
echo "$path"
cd "$path" && \
#./run-es.sh && \
service elasticsearch start && \

sleep 120  && \

curl localhost:9200 &&\

cd ddprofiler || exit && \
echo $'\n'Build ddprofiler$'\n' && \
./build.sh && \

pid=$(ps -ef | grep elastic | tr -s " " | cut -d " " -f 2 | head -1) && \
echo $'\n'Elastics Seach pid: "$pid"$'\n' && \

echo $'\n'Load db into elasticsearch$'\n' && \
./run.sh --sources "$source_name" && \
cd "../" && \

echo $'\n'Create model in pickle format $'\n' && \
python networkbuildercoordinator.py --opath models/"$data_name"
