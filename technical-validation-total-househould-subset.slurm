#!/bin/bash
#SBATCH --job-name=dataset-processing
#SBATCH --ntasks=16
#SBATCH --cpus-per-task=1
#SBATCH --mem=120G
#SBATCH --time=12:00:00
#SBATCH --tmp=100G
#SBATCH --partition=normal
#SBATCH --qos=normal
#SBATCH --mail-type=ALL
#SBATCH --mail-user=juan.pablo.chavat [-at-] fing.edu.uy

source /etc/profile.d/modules.sh

date
echo "removing dask-worker-space..."
rm -rf dask-worker-space

mkdir -p /scratch/$USER/$SLURM_JOBID
DATA_SOURCE="THC"

date
echo "copying parquet files..."
cp -R $DATA_SOURCE/consum-parquet /scratch/$USER/$SLURM_JOBID

DATA_DEST="~/dataset/results_technical_validation_total_household_subset/"$SLURM_JOBID
mkdir -p $DATA_DEST
cp technical-validation-total-househould-subset.py technical-validation-total-househould-subset.slurm $DATA_DEST

date
echo "Start executing python script..."
python technical-validation-total-househould-subset.py /scratch/$USER/$SLURM_JOBID $DATA_DEST
echo "...finished python script."

date
echo "Starting to remove the files in scratch..."
rm -rf /scratch/$USER/$SLURM_JOBID
echo "...finished the file removal."

echo "FIN."
