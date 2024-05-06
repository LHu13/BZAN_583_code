#!/bin/bash
#SBATCH --job-name utk
#SBATCH --account=bckj-delta-cpu
#SBATCH --partition=cpu
#SBATCH --mem=240g
#SBATCH --nodes=2
#SBATCH --cpus-per-task=8
#SBATCH --tasks-per-node=4
#SBATCH --time 23:00:00
#SBATCH -e ./utk1.e
#SBATCH -o ./utk1.o
pwd
module load r
module list


time mpirun -np 8 Rscript flight_data.R --args 16
