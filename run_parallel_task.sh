#!/bin/sh

#SBATCH --partition=general
#SBATCH --qos=short
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --mem=4096
python run_job.py -c "$1"