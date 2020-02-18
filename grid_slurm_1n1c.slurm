#!/bin/bash
# 1 node ,4 cores, 4 tasks per node 
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0-1:00:00
#SBATCH --partition=cloud

# Load required modules
 module load Python/3.6.4-intel-2017.u2

time mpiexec -n 1 python3 GridAllocation_withComments.py bigTwitter.json melbGrid.json  
