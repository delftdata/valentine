#!/bin/sh

#SBATCH --partition=general
#SBATCH --qos=short
#SBATCH --time=00:45:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=10240
srun python run_job.py -c "$1"