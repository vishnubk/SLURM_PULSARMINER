#!/bin/bash
logs='slurm_job_logs/'
mkdir -p $logs
# get the maximum number of jobs that the user can submit minus 5.
maxjobs_raw=$(sacctmgr list associations format=user,maxsubmitjobs -n | grep $USER | awk '{print $2}')
maxjobs=$(( maxjobs_raw - 5 ))
# If maxjobs is greater than 20k, fix it at 20k
if [ "$maxjobs" -gt 20000 ]; then
    maxjobs=20000
fi
slurm_user_requested_jobs=$maxjobs
check_job_submission_limit () {
    while true; do
        numjobs=$(squeue -u $USER -h | wc -l)
        if [ "$((numjobs))" -lt "$((slurm_user_requested_jobs))" ]; then
            break
        else
            echo "Number of jobs submitted is $numjobs. Waiting for them to finish before submitting more jobs."
            sleep 100
        fi
    done
}
data_path=/hercules/scratch/vkrishna/SLURM_PULSARMINER/NGC441_20200709_cfbf00148.fil
search=$(sbatch --mail-type=FAIL --mail-user=vivekvenkris@gmail.com --parsable --job-name=accel_search  --output=$logs/NGC441_accel_search_20200709_cfbf00148_29_ck01_DM230.35_zmax_200.out --error=$logs/NGC441_accel_search_20200709_cfbf00148_29_ck01_DM230.35_zmax_200.err -p gpu.q --gres=gpu:1 --export=ALL --cpus-per-task=1 --time=01:00:00 --mem=20GB --wrap="/hercules/scratch/vkrishna/SLURM_PULSARMINER/PERIODICITY_SEARCH_AND_COPY_BACK.sh /u/vishnu/singularity_images/presto_gpu.sif /hercules /hercules/scratch/vkrishna/SLURM_PULSARMINER /hercules/scratch/vkrishna/SLURM_PULSARMINER/NGC441/20200709/cfbf00148/03_DEDISPERSION/NGC441_20200709_cfbf00148/full/ck00/NGC441_20200709_cfbf00148_full_ck00_DM230.35.dat 200 0 16 1 /tmp/NGC441/20200709/cfbf00148/03_ACCEL_SEARCH/NGC441_20200709_cfbf00148/29/ck01/DM230.35/ZMAX_200 /hercules/scratch/vkrishna/SLURM_PULSARMINER/NGC441/20200709/cfbf00148/03_DEDISPERSION/NGC441_20200709_cfbf00148/29m/ck01 29 ck01 1")

check_job_submission_limit

search=$(sbatch --mail-type=FAIL --mail-user=vivekvenkris@gmail.com --parsable --job-name=accel_search  --output=$logs/NGC441_accel_search_20200709_cfbf00148_14_ck02_DM230.80_zmax_0.out --error=$logs/NGC441_accel_search_20200709_cfbf00148_14_ck02_DM230.80_zmax_0.err -p gpu.q --gres=gpu:1 --export=ALL --cpus-per-task=1 --time=01:00:00 --mem=20GB --wrap="/hercules/scratch/vkrishna/SLURM_PULSARMINER/PERIODICITY_SEARCH_AND_COPY_BACK.sh /u/vishnu/singularity_images/presto_gpu.sif /hercules /hercules/scratch/vkrishna/SLURM_PULSARMINER /hercules/scratch/vkrishna/SLURM_PULSARMINER/NGC441/20200709/cfbf00148/03_DEDISPERSION/NGC441_20200709_cfbf00148/full/ck00/NGC441_20200709_cfbf00148_full_ck00_DM230.80.dat 0 0 16 1 /tmp/NGC441/20200709/cfbf00148/03_ACCEL_SEARCH/NGC441_20200709_cfbf00148/14/ck02/DM230.80/ZMAX_0 /hercules/scratch/vkrishna/SLURM_PULSARMINER/NGC441/20200709/cfbf00148/03_DEDISPERSION/NGC441_20200709_cfbf00148/14m/ck02 14 ck02 1")

check_job_submission_limit

