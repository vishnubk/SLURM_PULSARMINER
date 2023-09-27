#!/bin/bash
logs='slurm_job_logs'
mkdir -p $logs
data_path=/hercules/scratch/vkrishna/SLURM_PULSARMINER/NGC441_20200709_cfbf00148.fil




###################################### Running Periodicity Search on Observation: NGC441_20200709_cfbf00148_full_ck00_DM223.00.dat, segment: full chunk: ck00    ##################################################################
search=$(sbatch --parsable --job-name=accel_search --output=$logs/NGC441_accel_search_20200709_cfbf00148_full_ck00_DM223.00_zmax_0.out --error=$logs/NGC441_accel_search_20200709_cfbf00148_full_ck00_DM223.00_zmax_0.err -p gpu.q --gres=gpu:1 --export=ALL --cpus-per-task=1 --time=01:00:00 --mem=20GB --wrap="echo 'Hello World 1'")

slurmids="$slurmids:$search"
check_job_submission_limit
search=$(sbatch --parsable --job-name=accel_search --output=$logs/NGC441_accel_search_20200709_cfbf00148_full_ck00_DM223.00_zmax_0.out --error=$logs/NGC441_accel_search_20200709_cfbf00148_full_ck00_DM223.00_zmax_0.err -p gpu.q --gres=gpu:1 --export=ALL --cpus-per-task=1 --time=01:00:00 --mem=20GB --wrap="echo 'Hello World 2'")

slurmids="$slurmids:$search"
check_job_submission_limit
###################################### Running Periodicity Search on Observation: NGC441_20200709_cfbf00148_full_ck00_DM223.05.dat, segment: full chunk: ck00    ##################################################################
search=$(sbatch --parsable --job-name=accel_search --output=$logs/NGC441_accel_search_20200709_cfbf00148_full_ck00_DM223.00_zmax_0.out --error=$logs/NGC441_accel_search_20200709_cfbf00148_full_ck00_DM223.00_zmax_0.err -p gpu.q --gres=gpu:1 --export=ALL --cpus-per-task=1 --time=01:00:00 --mem=20GB --wrap="echo 'Hello World 3'")

slurmids="$slurmids:$search"
check_job_submission_limit

IFS=':' read -r -a array <<< "$slurmids"

for index in "${!array[@]}"; do
    if [ $index -ne 0 ]; then
        jobid=${array[index]}
        
        # Getting job state using sacct
        job_state=$(sacct -j $jobid --format=State --noheader | head -1)
        
        # Checking if the job failed or timed out etc.
        if [[ "$job_state" == "FAILED" || "$job_state" == "TIMEOUT" || "$job_state" == "CANCELLED" ]]; then
            # Relaunching the job
            echo relaunch_job $jobid
        fi
    fi
done

