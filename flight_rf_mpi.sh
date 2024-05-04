#!/bin/bash
#SBATCH --job-name utk
#SBATCH --account=bckj-delta-cpu
#SBATCH --partition=cpu
#SBATCH --mem=256g
#SBATCH --nodes=4
#SBATCH --cpus-per-task=16
#SBATCH --tasks-per-node=8
#SBATCH --time 23:00:00
#SBATCH -e ./utk.e
#SBATCH -o ./utk.o
pwd
module load r
module list


time mpirun -np 16 Rscript flight_rf_mpi.R --args 16
