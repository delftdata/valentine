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

declare -a docker_arguments

#for file in `find -type d configuration_files/"$algorithm_name" | xargs ls `
#do
#  if [ "$algorithm_name" = 'SemProp' ]
#  then
#    docker_arguments+=("$file")
#  fi
#done

# parallel --dry-run --jobs "$num_jobs" ./run_semprop_docker.sh ::: "${docker_arguments[@]}"
parallel --jobs "$num_jobs" ./run_semprop_docker.sh ::: configuration_files/"$algorithm_name"/*/*
