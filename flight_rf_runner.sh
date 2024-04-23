#!/bin/bash
#SBATCH --job-name utk
#SBATCH --account=bckj-delta-cpu
#SBATCH --partition=cpu
#SBATCH --mem=64g
#SBATCH --nodes=4
#SBATCH --cpus-per-task=16
#SBATCH --time 03:00:00
#SBATCH -e ./utk.e
#SBATCH -o ./utk.o

pwd

module load r
module list

#time Rscript flight_rf_serial.R
#time Rscript flight_rf_parallel.R --args 1
#time Rscript flight_rf_parallel.R --args 2
#time Rscript flight_rf_parallel.R --args 4
time Rscript flight_rf_parallel.R --args 8
time Rscript flight_rf_parallel.R --args 16

