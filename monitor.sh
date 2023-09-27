#!/bin/bash

cluster=NGC441
epoch=20200709
logs='slurm_job_logs'
user="vkrishna"
export $logs
relaunch_job() {
    local jobid=$1

    # Getting the stderr file path using scontrol
    local stderr_path=$(scontrol show job $jobid -o | grep -oP 'StdErr=\K\S+')
    
    # Extracting the beam identifier from the stderr file name
    local beam=$(basename "$stderr_path" | grep -oP '(cfbf|ifbf)\d+')

    # Constructing the name of the shell script file containing sbatch commands
    local shell_script_file="slurm_jobs_${cluster}_${epoch}_${beam}.sh"
    
    # Getting the full sbatch command corresponding to the failed job
    local sbatch_command=$(grep $(basename "$stderr_path") "$shell_script_file")
    
    # Removing any dependency strings from the sbatch command
    sbatch_command=$(echo "$sbatch_command" | sed -e 's/--dependency=[^ ]* //')

    # Constructing the relaunch command with modified log file paths
    local relaunch_command="$(echo "$sbatch_command" | sed -e 's/_zmax_0\.out/_zmax_0_relaunch\.out/' -e 's/_zmax_0\.err/_zmax_0_relaunch\.err/')"
    #echo "$relaunch_command"
    eval "$relaunch_command"




}

# A loop to monitor the jobs and call relaunch_job for any failed jobs
while :; do
    # Get the list of failed job IDs (modify the JobState as needed)
    failed_jobs=$(squeue -u $user -o "%i %t" | awk '$2=="F" {print $1}')
    echo $failed_jobs

    for jobid in $failed_jobs; do
        echo relaunch_job $jobid
    done
    
    echo "Sleeping for 60 seconds"
    sleep 60 # Adjust the sleep time as needed

done

