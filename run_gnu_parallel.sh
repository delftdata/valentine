#!/bin/bash

set -x

if [ ! -n "$1" ]; then
  echo "Give me an algorithm name."
  exit 1
fi

if [ ! -n "$2" ]; then
  echo "How many jobs should I run?"
  exit 1
fi

algorithm_name=$1
num_jobs=$2

for folder in configuration_files/"$algorithm_name"/*
do
  if [ "$algorithm_name" = 'SemProp' ]
  then

    for entry in "$folder"/*
    do
      docker_arguments+=("$entry")
    done

    # parallel --dry-run --jobs "$num_jobs" ./run_semprop_docker.sh ::: "${docker_arguments[@]}"
    parallel --jobs "$num_jobs" ./run_semprop_docker.sh ::: "${docker_arguments[@]}"
  else
    parallel --jobs "$num_jobs" python run_job.py -c ::: "$folder"/*
  fi
done
