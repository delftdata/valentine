#!/bin/bash

echo 'Run Elastic Search Script'
rm -rf /tmp/elasticsearch-dir
path="$(dirname "$0")"
$path/run-es.sh


cd $path/aurum-datadiscovery/ddprofiler
echo $'\n'Build ddprofiler$'\n'
./build.sh && \

#echo $'\n'Load db into elasticsearch$'\n'
#./run.sh --sources ../../../access-db-movies.yml

#cd ../
#echo $'\n'Create virtual environment and install requirements$'\n'
#source `which virtualenvwrapper.sh`
#mkvirtualenv aurum-ex && \
#workon aurum-ex && \
#pip install -r req2.txt

#echo $'\n'Create model in pickle format $'\n'
#python networkbuildercoordinator.py --opath test/testmodel/
pid=$(ps -ef | grep elastic | tr -s " " | cut -d " " -f 2 | head -1)

echo $'\n'Elastics Seach pid: "$pid"$'\n'
