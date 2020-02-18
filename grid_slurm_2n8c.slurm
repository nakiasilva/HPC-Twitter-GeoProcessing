#!/bin/bash
# 2 nodes ,8 cores, 4 tasks per node 
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=0:15:00
#SBATCH --partition=physical

# Load required modules
# module load Python/3.5.2-goolf-2015a 
module load Python/3.6.4-intel-2017.u2

split -l $((`sed -n '$=' bigTwitter.json`/8)) bigTwitter.json -d bigTwitter_

time mpirun python3 GridAllocation_withComments.py bigTwitter_ melbGrid.json

rm bigTwitter_*