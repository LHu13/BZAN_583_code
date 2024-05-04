#!/bin/bash
#SBATCH --job-name utk
#SBATCH --account=bckj-delta-cpu
#SBATCH --partition=cpu
#SBATCH --mem=64g
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --time 10:00:00
#SBATCH -e ./test.e
#SBATCH -o ./test.o

pwd

module load r
module list

time Rscript flight_data_reader.R

