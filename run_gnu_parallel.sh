#!/bin/sh

algorithm_name=$1
num_jobs=$2

for folder in configuration_files/"$algorithm_name"/*
do
  parallel --jobs "$num_jobs" python run_job.py -c ::: "$folder"/*
done
