#!/bin/sh

#SBATCH --partition=general
#SBATCH --qos=short
#SBATCH --time=00:10:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=4096
# Your job commands go below here
for config in "$1"/*
do
  	srun python run_job.py -c "$config"
done
wait

