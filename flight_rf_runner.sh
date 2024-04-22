#!/bin/bash
#SBATCH --job-name utk
#SBATCH --account=bckj-delta-cpu
#SBATCH --partition=cpu
#SBATCH --mem=64g
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --time 02:00:00
#SBATCH -e ./utk.e
#SBATCH -o ./utk.o

pwd

module load r
module list

time Rscript flight_rf_serial.R


