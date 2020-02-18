#!/bin/sh

algorithm_name=$1

for folder in configuration_files/"$algorithm_name"/*
do
  if [ "$algorithm_name" = 'SimilarityFlooding' ] || [ "$algorithm_name" = 'Cupid' ]
	then
	  for config in "$folder"/*
	  do
	    sbatch ./run_simple_task.sh "$config"
    done
	elif [ "$algorithm_name" = 'Coma' ]
	then
	  for config in "$folder"/*
	  do
	    batch ./run_memory_intensive_task.sh "$config"
    done
	elif [ "$algorithm_name" = 'CorrelationClustering' ] || [ "$algorithm_name" = 'JaccardLevenMatcher' ]
	then
	  for config in "$folder"/*
	  do
	    sbatch ./run_parallel_task.sh "$config"
    done
	fi
done
