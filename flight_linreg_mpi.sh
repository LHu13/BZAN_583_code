#!/bin/bash
#SBATCH --job-name utk
#SBATCH --account=bckj-delta-cpu
#SBATCH --partition=cpu
#SBATCH --mem=128g
#SBATCH --nodes=2
#SBATCH --cpus-per-task=4
#SBATCH --tasks-per-node=2
#SBATCH --time 23:00:00
#SBATCH -e ./utk2.e
#SBATCH -o ./utk2.o

pwd

module load r
module list

#time Rscript flight_rf_serial.R
#time Rscript flight_rf_parallel.R --args 1
#time Rscript flight_rf_parallel.R --args 2
#time Rscript flight_rf_parallel.R --args 4
# time Rscript flight_rf_parallel.R --args 8
# time Rscript flight_rf_parallel.R --args 16

time mpirun -np 4 Rscript flight_linreg_mpi.R --args 16
