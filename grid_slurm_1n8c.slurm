#!/bin/bash
# 1 node ,8 cores, 8 tasks per node 
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=0-1:00:00
#SBATCH --partition=cloud

# Load required modules
 module load Python/3.6.4-intel-2017.u2

split -l $((`sed -n '$=' bigTwitter.json`/8)) bigTwitter.json -d bigTwitter_

time mpiexec -n 8 python3 GridAllocation_withComments.py bigTwitter_ melbGrid.json  

rm bigTwitter_*