#!/bin/bash

if [[ -z "$1" ]]; then
  echo "Give me an algorithm name."
  exit 1
fi

if [[ -z "$2" ]]; then
  echo "How many jobs should I run?"
  exit 1
fi

algorithm_name=$1
num_jobs=$2

if [[ "$algorithm_name" = 'SimilarityFlooding' ]] || [[ "$algorithm_name" = 'CorrelationClustering' ]] || \
[[ "$algorithm_name" = 'JaccardLevenMatcher' ]] || [[ "$algorithm_name" = 'Coma' ]] || [[ "$algorithm_name" = 'EmbDI' ]]
then
  parallel --will-cite  --jobs "$num_jobs" -u python run_job.py -c ::: configuration_files/"$algorithm_name"/*/*
elif [[ "$algorithm_name" = 'Cupid' ]]
then
  for folder in configuration_files/"$algorithm_name"/*
  do
    parallel --will-cite --jobs "$num_jobs" -u python run_job.py -c ::: "$folder"/*
  done
elif [[ "$algorithm_name" = 'SemProp' ]]
then
  parallel --will-cite --jobs "$num_jobs" ./run_semprop_docker.sh ::: configuration_files/"$algorithm_name"/*/*
else
  echo "Not a valid algorithm name."
  exit 1
fi
