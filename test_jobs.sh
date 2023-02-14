#!/bin/bash
logs='slurm_job_logs/'
mkdir -p $logs
epoch=20220901
data_path=/hercules/scratch/vishnu/SLURM_PULSARMINER/M30_20220901_cfbf00150.fil

slurmids=""

########## Starting batch 1 of 2 ##########
###################################### Running Dedispersion on Cluster M30, Epoch 20220901, Beam cfbf00150 Segment full, Chunk ck00 using 3 CPUs   ##################################################################
dedisp=$(sbatch --parsable --job-name=dedisp --output=$logs/M30_dedisp_20220901_cfbf00150_full_ck00_batch_1.out --error=$logs/M30_dedisp_20220901_cfbf00150_full_ck00_batch_1.err -p gpu.q --gres=gpu:1 --export=ALL --cpus-per-task=3 --time=12:00:00 --mem=300GB --wrap="/hercules/scratch/vishnu/SLURM_PULSARMINER/DEDISPERSE_AND_COPY_BACK.sh /u/vishnu/singularity_images/presto_gpu.sif /hercules /hercules/scratch/vishnu/SLURM_PULSARMINER /hercules/scratch/vishnu/SLURM_PULSARMINER/M30_20220901_cfbf00150.fil /hercules/scratch/vishnu/SLURM_PULSARMINER/M30/20220901/cfbf00150/01_RFIFIND/M30_20220901_cfbf00150_rfifind.mask 24.00 24.02 3 3 full ck00 /tmp/M30/20220901/cfbf00150/03_DEDISPERSION /hercules/scratch/vishnu/SLURM_PULSARMINER/M30/20220901/cfbf00150/03_DEDISPERSION/M30_20220901_cfbf00150/full/ck00")
slurmids="$slurmids:$dedisp"
echo $slurmids

