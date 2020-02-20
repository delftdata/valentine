#!/bin/bash

set -x

algorithm_name=$1
num_jobs=$2

for folder in configuration_files/"$algorithm_name"/*
do
  if [ "$algorithm_name" = 'SemProp' ]
  then
    declare -a docker_arguments

    for entry in "$folder"/*
    do
      fixed=$( echo "$entry" | LC_ALL=C sed -e 's/[^a-zA-Z0-9,._+@%/-]/\\&/g; 1{$s/^$/""/}; 1!s/^/"/; $!s/$/"/')
      docker_arguments+=("export PYTHONPATH=/code/ && python ./run_job.py -c ""$fixed")
    done
    parallel --jobs "$num_jobs" ./run_semprop_docker.sh ::: "${docker_arguments[@]}"
  else
    parallel --jobs "$num_jobs" python run_job.py -c ::: "$folder"/*
  fi
done
