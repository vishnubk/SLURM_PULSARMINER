#!/bin/bash
logs='NGC441/20200709/cfbf00193/00_SLURM_JOB_LOGS'
mkdir -p $logs
CLUSTER="NGC441"
EPOCH="20200709"
BEAM="cfbf00193"
data_path=/hercules/scratch/vishnu/SLURM_PULSARMINER/NGC441_20200709_cfbf00193.fil
slurm_user_requested_jobs=2000
slurmids=""
# Define a function that checks the condition and sleeps until it is met
check_job_submission_limit () {
    while true; do
        numjobs=$(squeue -u $USER -h | wc -l) # Get number of submitted jobs by user
        if [ "$((numjobs))" -lt "$((slurm_user_requested_jobs))" ]; then # Check if condition is met
            break # Exit loop
        else
            echo "Number of jobs submitted is $numjobs. Waiting for them to finish before submitting more jobs."
            sleep 100 # Sleep for 100 seconds
        fi
    done
}

#Relaunch function
relaunch_job() {
    local jobid=$1
    local attempt_number=$2

    # Getting the stderr file path using scontrol
    local stderr_path=$(scontrol show job $jobid -o | grep -oP 'StdErr=\K\S+')
    
    # Constructing the name of the shell script file containing sbatch commands
    local shell_script_file="slurm_jobs_${CLUSTER}_${EPOCH}_${BEAM}.sh"
    
    # Getting the full sbatch command corresponding to the failed job
    local sbatch_command=$(grep $(basename "$stderr_path") "$shell_script_file")
    
    # Removing any dependency strings from the sbatch command
    sbatch_command=$(echo "$sbatch_command" | sed -e 's/--dependency=[^ ]* //')
    
    # Adding "relaunch_" prefix to the job name and including the attempt number
    sbatch_command=$(echo "$sbatch_command" | sed -e "s/--job-name=\([^ ]*\)/--job-name=relaunch_\1_$attempt_number/")
    
    # Modifying the output and error file paths to include the relaunch attempt number
    sbatch_command=$(echo "$sbatch_command" | sed -e "s/\(_zmax_0\.out\)/_relaunch_attempt_${attempt_number}\1/" -e "s/\(_zmax_0\.err\)/_relaunch${attempt_number}\1/")
    # Find the starting index of the word "sbatch"
    start_index=$(echo $sbatch_command | awk '{ print index($0, "sbatch") }')
    # Remove everything until the word "sbatch" starts
    trimmed_sbatch_command=${sbatch_command:start_index-1}

    # Remove the closing bracket at the end
    if [[ "$trimmed_sbatch_command" == *")" ]]; then
        trimmed_sbatch_command=${trimmed_sbatch_command:0:-1}
    fi

    # Executing the modified sbatch command to relaunch the job
    local new_jobid=$(eval "$trimmed_sbatch_command")
    # Returning the new job ID
    echo $new_jobid
}

########## Starting batch 6 of 6 ##########
###################################### Running Dedispersion on Cluster NGC441, Epoch 20200709, Beam cfbf00193 Segment full, Chunk ck00 using 41 CPUs   ##################################################################
dedisp=$(sbatch --parsable --job-name=dedisp --output=$logs/NGC441_dedisp_20200709_cfbf00193_full_ck00_batch_6.out --error=$logs/NGC441_dedisp_20200709_cfbf00193_full_ck00_batch_6.err -p short.q --export=ALL --cpus-per-task=41 --time=4:00:00 --mem=350GB --wrap="/hercules/scratch/vishnu/SLURM_PULSARMINER/DEDISPERSE_AND_COPY_BACK.sh /u/vishnu/singularity_images/presto_gpu.sif /hercules /hercules/scratch/vishnu/SLURM_PULSARMINER /hercules/scratch/vishnu/SLURM_PULSARMINER/NGC441_20200709_cfbf00193.fil /hercules/scratch/vishnu/SLURM_PULSARMINER/NGC441/20200709/cfbf00193/01_RFIFIND/NGC441_20200709_cfbf00193_rfifind.mask 235.00 237.00 41 41 full ck00 /tmp/NGC441/20200709/cfbf00193/03_DEDISPERSION /hercules/scratch/vishnu/SLURM_PULSARMINER/NGC441/20200709/cfbf00193/03_DEDISPERSION/NGC441_20200709_cfbf00193/full/ck00")
echo $slurmids
echo $dedisp
slurmids="$slurmids:$dedisp"
check_job_submission_limit
###################################### Running Periodicity Search on Observation: NGC441_20200709_cfbf00193_full_ck00_DM235.00.dat, segment: full chunk: ck00    ##################################################################
search=$(sbatch --parsable --job-name=accel_search --dependency=afterok:$dedisp --output=$logs/NGC441_accel_search_20200709_cfbf00193_full_ck00_DM235.00_zmax_0.out --error=$logs/NGC441_accel_search_20200709_cfbf00193_full_ck00_DM235.00_zmax_0.err -p short.q --gres=gpu:1 --export=ALL --cpus-per-task=1 --time=04:00:00 --mem=20GB --wrap="/hercules/scratch/vishnu/SLURM_PULSARMINER/PERIODICITY_SEARCH_AND_COPY_BACK.sh /u/vishnu/singularity_images/presto_gpu.sif /hercules /hercules/scratch/vishnu/SLURM_PULSARMINER /hercules/scratch/vishnu/SLURM_PULSARMINER/NGC441/20200709/cfbf00193/03_DEDISPERSION/NGC441_20200709_cfbf00193/full/ck00/NGC441_20200709_cfbf00193_full_ck00_DM235.00.dat 0 0 16 1 /tmp/NGC441/20200709/cfbf00193/03_ACCEL_SEARCH/NGC441_20200709_cfbf00193/full/ck00/DM235.00/ZMAX_0 /hercules/scratch/vishnu/SLURM_PULSARMINER/NGC441/20200709/cfbf00193/03_DEDISPERSION/NGC441_20200709_cfbf00193/full/ck00 full ck00 1")
echo $search
slurmids="$slurmids:$search"
check_job_submission_limit
search=$(sbatch --parsable --job-name=accel_search --dependency=afterok:$dedisp --output=$logs/NGC441_accel_search_20200709_cfbf00193_full_ck00_DM235.00_zmax_200.out --error=$logs/NGC441_accel_search_20200709_cfbf00193_full_ck00_DM235.00_zmax_200.err -p short.q --gres=gpu:1 --export=ALL --cpus-per-task=1 --time=04:00:00 --mem=20GB --wrap="/hercules/scratch/vishnu/SLURM_PULSARMINER/PERIODICITY_SEARCH_AND_COPY_BACK.sh /u/vishnu/singularity_images/presto_gpu.sif /hercules /hercules/scratch/vishnu/SLURM_PULSARMINER /hercules/scratch/vishnu/SLURM_PULSARMINER/NGC441/20200709/cfbf00193/03_DEDISPERSION/NGC441_20200709_cfbf00193/full/ck00/NGC441_20200709_cfbf00193_full_ck00_DM235.00.dat 200 0 16 1 /tmp/NGC441/20200709/cfbf00193/03_ACCEL_SEARCH/NGC441_20200709_cfbf00193/full/ck00/DM235.00/ZMAX_200 /hercules/scratch/vishnu/SLURM_PULSARMINER/NGC441/20200709/cfbf00193/03_DEDISPERSION/NGC441_20200709_cfbf00193/full/ck00 full ck00 1")
echo $search
slurmids="$slurmids:$search"
check_job_submission_limit
###################################### Running Periodicity Search on Observation: NGC441_20200709_cfbf00193_full_ck00_DM235.05.dat, segment: full chunk: ck00    ##################################################################
search=$(sbatch --parsable --job-name=accel_search --dependency=afterok:$dedisp --output=$logs/NGC441_accel_search_20200709_cfbf00193_full_ck00_DM235.05_zmax_0.out --error=$logs/NGC441_accel_search_20200709_cfbf00193_full_ck00_DM235.05_zmax_0.err -p short.q --gres=gpu:1 --export=ALL --cpus-per-task=1 --time=04:00:00 --mem=20GB --wrap="/hercules/scratch/vishnu/SLURM_PULSARMINER/PERIODICITY_SEARCH_AND_COPY_BACK.sh /u/vishnu/singularity_images/presto_gpu.sif /hercules /hercules/scratch/vishnu/SLURM_PULSARMINER /hercules/scratch/vishnu/SLURM_PULSARMINER/NGC441/20200709/cfbf00193/03_DEDISPERSION/NGC441_20200709_cfbf00193/full/ck00/NGC441_20200709_cfbf00193_full_ck00_DM235.05.dat 0 0 16 1 /tmp/NGC441/20200709/cfbf00193/03_ACCEL_SEARCH/NGC441_20200709_cfbf00193/full/ck00/DM235.05/ZMAX_0 /hercules/scratch/vishnu/SLURM_PULSARMINER/NGC441/20200709/cfbf00193/03_DEDISPERSION/NGC441_20200709_cfbf00193/full/ck00 full ck00 1")
echo $search
slurmids="$slurmids:$search"
echo $slurmids
mkdir -p SEARCH_PROGRESS
search_progress_accel_search_zmax_0=false
search_progress_accel_search_zmax_200=false
check_search_progress() {
progress_file="SEARCH_PROGRESS/search_progress_NGC441_20200709_cfbf00193.txt"
###################################### Loop to check accel-search progress ##################################################################
  # Count the number of files matching *_ACCEL_0.txtcand in the directory
  found_files=$(find NGC441/20200709/cfbf00193/03_DEDISPERSION -name '*_ACCEL_0.txtcand' | wc -l)
  percentage=$(( 100 * $found_files / 8992 ))
  bar=""
  for i in $(seq 1 $percentage); do
    bar="${bar}#"
  done
  for i in $(seq 1 $((100 - $percentage))); do
    bar="${bar} "
  done
  echo "Date: $(date), Search Progress for zmax 0: [${bar}] $percentage% \r"
  echo "Date: $(date), Search Progress for zmax 0: [${bar}] $percentage%" >> $progress_file
  if [ $found_files -ge 8992 ]; then
    echo -e "\nSearch Category zmax: 0 completed." 
    echo "Search Category zmax: 0 completed. Date: $(date)" >> $progress_file
    search_progress_accel_search_zmax_0=true
    #break
  fi
  # Count the number of files matching *_ACCEL_200.txtcand in the directory
  found_files=$(find NGC441/20200709/cfbf00193/03_DEDISPERSION -name '*_ACCEL_200.txtcand' | wc -l)
  percentage=$(( 100 * $found_files / 8992 ))
  bar=""
  for i in $(seq 1 $percentage); do
    bar="${bar}#"
  done
  for i in $(seq 1 $((100 - $percentage))); do
    bar="${bar} "
  done
  echo "Date: $(date), Search Progress for zmax 200: [${bar}] $percentage% \r"
  echo "Date: $(date), Search Progress for zmax 200: [${bar}] $percentage%" >> $progress_file
  if [ $found_files -ge 8992 ]; then
    echo -e "\nSearch Category zmax: 200 completed." 
    echo "Search Category zmax: 200 completed. Date: $(date)" >> $progress_file
    search_progress_accel_search_zmax_200=true
    #break
  fi
}

# Initialize variables
IFS=':' read -r -a array <<< "$slurmids"
unset job_map
unset watching
unset retry_count
declare -A job_map
declare -A watching
declare -A retry_count
max_retry_attempts=3
# Initialize job_map and watching with original job IDs
for jobid in "${array[@]}"; do
  if [ -n "$jobid" ]; then
    job_map[$jobid]=$jobid
    watching[$jobid]=false
    retry_count[$jobid]=0
  fi
done

for key in "${!job_map[@]}"; do
  echo "Key: $key, Value: ${job_map[$key]}"
done


slurm_job_monitoring_status=false
check_job_statuses() {
  start_time=$(date +%s)
  all_jobs_completed=true
  job_running=false
  for original_jobid in "${!job_map[@]}"; do
    if [ "${watching[$original_jobid]}" = "true" ]; then
      continue
    fi
    sleep 4
    current_jobid=${job_map[$original_jobid]}
    job_state=$(sacct -j "$current_jobid" --format=State --noheader | head -1 | xargs)
    if [ -z "$job_state" ]; then
      echo "No sacct history found for JOB ID: $current_jobid (originally $original_jobid). Assuming completion."
      unset job_map[$original_jobid]
      continue
    fi
    if [[ "$job_state" == "RUNNING" ]]; then
      all_jobs_completed=false
      job_running=true
      echo "Job state of JOB ID: $current_jobid (originally $original_jobid) is $job_state."
    elif [[ "$job_state" =~ "FAILED" || "$job_state" =~ "TIMEOUT" || "$job_state" =~ "CANCELLED" ]]; then
      all_jobs_completed=false
      retry_count[$original_jobid]=$((retry_count[$original_jobid] + 1))
      if [[ ${retry_count[$original_jobid]} -le $max_retry_attempts ]]; then
        watching[$original_jobid]=true
        new_jobid=$(relaunch_job $original_jobid ${retry_count[$original_jobid]})
        if [ -n "$new_jobid" ]; then
          job_map[$original_jobid]=$new_jobid
        fi
      else
          echo "Max retry attempts reached for $original_jobid during watching. Not retrying."
          unset job_map[$original_jobid]
      fi
    elif [[ "$job_state" == "COMPLETED" ]]; then
          echo "Job state of JOB ID: $current_jobid (originally $original_jobid) is $job_state."
          unset job_map[$original_jobid]
    else
          all_jobs_completed=false
    fi
  done
  for original_jobid in "${!watching[@]}"; do
    if [ "${watching[$original_jobid]}" = "true" ]; then
      sleep 4
      current_jobid=${job_map[$original_jobid]}
      job_state=$(sacct -j "$current_jobid" --format=State --noheader | head -1 | xargs)
      if [ -z "$job_state" ]; then
        echo "No sacct history found for JOB ID: $current_jobid (originally $original_jobid). Assuming completion."
        unset job_map[$original_jobid]
        continue
      fi
      if [[ "$job_state" == "COMPLETED" ]]; then
        watching[$original_jobid]=false
        unset job_map[$original_jobid]
      elif [[ "$job_state" =~ "FAILED" || "$job_state" =~ "TIMEOUT" || "$job_state" =~ "CANCELLED" ]]; then
        all_jobs_completed=false
        retry_count[$original_jobid]=$((retry_count[$original_jobid] + 1))
        if [[ ${retry_count[$original_jobid]} -le $max_retry_attempts ]]; then
          new_jobid=$(relaunch_job $original_jobid ${retry_count[$original_jobid]})
          if [ -n "$new_jobid" ]; then
            job_map[$original_jobid]=$new_jobid
          fi
        else
          echo "Max retry attempts reached for $original_jobid during watching. Not retrying."
          watching[$original_jobid]=false
          unset job_map[$original_jobid]
        fi
      else
        all_jobs_completed=false
        sleep 20
      fi
    fi
  done
  current_time=$(date +%s)
  elapsed_time=$((current_time - start_time))
  echo $elapsed_time
  if $all_jobs_completed || [ ${#job_map[@]} -eq 0 ]; then
    slurm_job_monitoring_status=true
    #break
  elif [ $elapsed_time -ge 1200 ]; then
    return
  elif $job_running; then
    sleep 30
  fi
}
while true; do
  if [[ "$search_progress_accel_search_zmax_0" = "false" || "$search_progress_accel_search_zmax_200" = "false" ]]; then
    check_search_progress
    echo $search_progress_accel_search_zmax_0, $search_progress_accel_search_zmax_200, $slurm_job_monitoring_status
  fi
  echo "AGAIN: $search_progress_accel_search_zmax_0, $search_progress_accel_search_zmax_200, $slurm_job_monitoring_status"
  if [ "$slurm_job_monitoring_status" = "false" ]; then
    check_job_statuses
  fi
  echo $search_progress_accel_search_zmax_0, $search_progress_accel_search_zmax_200, $slurm_job_monitoring_status

  if [[ "$search_progress_accel_search_zmax_0" = "true" && "$search_progress_accel_search_zmax_200" = "true" && "$slurm_job_monitoring_status" = "true" ]]; then
    break
  fi
  echo "Going to sleep"
  sleep 10  # Sleep for 10 minutes before checking again
done