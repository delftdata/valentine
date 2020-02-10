#!/bin/sh

for folder in configuration_files/*
do
  	sbatch ./run_tasks.sh "$folder" &
done
wait
