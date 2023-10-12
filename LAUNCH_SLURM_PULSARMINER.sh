#!/bin/bash

# default values
obs_file=""
pm_config_file=""
logs='slurm_job_logs'
mkdir -p $logs
# set default value for tmp directory
tmp_dir="/tmp"
# set the name of the Slurm config file
slurm_config_file="slurm_config.cfg"

#!/bin/bash

# default values
obs_file=""
pm_config_file=""
tmp_dir="/tmp"

# get the maximum number of jobs that the user can submit minus 5. This is to ensure that the user has some jobs left to submit manually
#maxjobs=$(( $(sacctmgr list associations format=user,maxsubmitjobs -n | grep $USER | awk '{print $2}') - 5 ))
maxjobs_raw=$(sacctmgr list associations format=user,maxsubmitjobs -n | grep $USER | awk '{print $2}')
maxjobs=$(( maxjobs_raw - 5 ))

# If maxjobs is greater than 20k, fix it at 20k
if [ "$maxjobs" -gt 20000 ]; then
    maxjobs=20000
fi

slurm_user_requested_jobs=$maxjobs
# parse command-line arguments
while getopts ":hm:o:p:t:" opt; do
  case $opt in
    h)
      echo "SLURM PULSARMINER Launch Script. With Great Parallelization Comes Great Responsibility. Ensure that you fill the slurm config file with the correct values for your system."
      echo ""
      echo "Usage: $0 [-h] [-m max_slurm_jobs] [-o observation_file] [-p config_file] [-t tmp_directory]"
      echo ""
      echo "Options:"
      echo "  -h            Show this help message and exit"
      echo "  -m NUM        Maximum number of SLURM jobs to submit at a time (default is five less than your max which is: $maxjobs)"
      echo "  -o FILE       Observation file to process"
      echo "  -p FILE       Configuration file"
      echo "  -t DIR        Temporary directory (default is /tmp)"
      exit 0
      ;;
    m)
      # Override the default value of $slurm_user_requested_jobs if the user provides a valid argument
      if [ "$OPTARG" -gt "$maxjobs" ]; then
        echo "Maximum number of SLURM jobs to submit ($OPTARG) is greater than the maximum allowed for the user ($maxjobs). Setting maximum number of jobs to $maxjobs."
        slurm_user_requested_jobs="$maxjobs"
      else
        slurm_user_requested_jobs="$OPTARG"
      fi
      ;;
    o)
      obs_file="$OPTARG"
      ;;
    p)
      pm_config_file="$OPTARG"
      ;;
    t)
      tmp_dir="$OPTARG"
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      echo "Use -h option to get help" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      echo "Use -h option to get help" >&2
      exit 1
      ;;
  esac
done

# print help message if no arguments are specified
if [ $# -eq 0 ]; then
  echo "SLURM PULSARMINER Launch Script. With Great Parallelization Comes Great Responsibility. Ensure that you fill the slurm config file with the correct values for your system."
  echo ""
  echo "Usage: $0 [-h] [-m max_slurm_jobs] [-o observation_file] [-p config_file] [-t tmp_directory]"
  echo ""
  echo "Options:"
  echo "  -h            Show this help message and exit"
  echo "  -m NUM        Maximum number of SLURM jobs to submit at a time (default is five less than your max which is: $maxjobs)"
  echo "  -o FILE       Observation file to process"
  echo "  -p FILE       Configuration file"
  echo "  -t DIR        Temporary directory (default is /tmp)"
  exit 0
fi

# check that observation file is specified
if [ -z "$obs_file" ]; then
  echo "Error: Observation file must be specified with -o option" >&2
  echo "Use -h option to get help" >&2
  exit 1
fi

# check that config file is specified
if [ -z "$pm_config_file" ]; then
  echo "Error: PULSARMINER Configuration file must be specified with -p option" >&2
  echo "Use -h option to get help" >&2
  exit 1
fi


# rest of the script goes here...
echo "Observation file is: $obs_file"
echo "PULSARMINER Configuration file is: $pm_config_file"
echo "Slurm Configuration file is: $slurm_config_file"
echo "Temporary directory is: $tmp_dir"
echo "Max. Slurm jobs we will submit at a time is: $slurm_user_requested_jobs"

#Parse the slurm config file
get_config_value() {
    local section="$1"
    local option="$2"
    awk -F ':\\s+' -v sec="$section" -v opt="$option" '
        $0 ~ "^\\[" sec "\\]" { f = 1 }
        f && $1 == opt {
            sub(/#.*$/, "", $2)  # remove comments
            gsub(/"/, "", $2)    # remove quotes
            if ($2 ~ /^[0-9]+(\.[0-9]+)?$/) {
                # numeric value
                if ($2 ~ /\./) {
                    # float
                    printf "%.5f", $2
                } else {
                    # integer
                    printf "%d", $2
                }
            } else {
                # string value
                if (opt == "WALL_CLOCK_TIME") {
                    split($2, t, ":")
                    if (length(t) == 2) {
                        # time in HH:MM format
                        printf "%s:00", $2
                    } else {
                        # time in HH:MM:SS format
                        print $2
                    }
                } else {
                    print $2
                }
            }
            exit
        }' "$slurm_config_file"
}

singularity_image_path=$(get_config_value "Singularity_Image" "Singularity_Image_Path")
mount_path=$(get_config_value "Singularity_Image" "MOUNT_PATH")
code_directory=$(get_config_value "Singularity_Image" "CODE_DIRECTORY_ABS_PATH")

# Get the slurm config values for the folding step
fold_cpus_per_task=$(get_config_value "Folding" "CPUS_PER_TASK")
fold_ram_per_job=$(get_config_value "Folding" "RAM_PER_JOB")
fold_wall_clock=$(get_config_value "Folding" "WALL_CLOCK_TIME")
fold_job_name=$(get_config_value "Folding" "JOB_NAME")
fold_partition=$(get_config_value "Folding" "JOB_PARTITION")

echo "Singularity Image Path: $singularity_image_path"
echo "Mount Path: $mount_path"
echo "Code Directory: $code_directory"
echo "Fold Wall Clock: $fold_wall_clock"

#Get CLUSTER_NAME, EPOCH and BEAM from the observation file
obs_name="${obs_file##*/}"  # remove path to get just the file name
obs_name="${obs_name%.fil}"  # remove extension to get just the file name
obs_parts=(${obs_name//_/ })  # split into array based on underscores
CLUSTER="${obs_parts[0]}"
EPOCH="${obs_parts[1]}"
BEAM="${obs_parts[2]}"
tmp_working_dir="${tmp_dir}/${CLUSTER}/${EPOCH}/${BEAM}"
mkdir -p $tmp_working_dir




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


singularity exec -H $HOME:/home1 -B $mount_path:$mount_path $singularity_image_path python ${code_directory}/create_slurm_jobs.py -s ${code_directory}/$slurm_config_file -p ${code_directory}/$pm_config_file -o $obs_file 

#exit 0
source ${code_directory}/slurm_jobs_${CLUSTER}_${EPOCH}_${BEAM}.sh


fold_script_filename=${CLUSTER}/${EPOCH}/${BEAM}/05_FOLDING/${CLUSTER}_${EPOCH}_${BEAM}/script_fold.txt
fold_batch_number=1
declare -a job_ids

while true; do
    if [ -f $fold_script_filename ]; then
        num_lines=$(wc -l < $fold_script_filename)

        if [ "$num_lines" -gt 0 ]; then
            for i in $(seq 0 $((fold_cpus_per_task - 1)) $((num_lines - 1))); do
                batch_size=$((num_lines - i))
                if [ "$batch_size" -gt "$fold_cpus_per_task" ]; then
                    batch_size=$fold_cpus_per_task
                fi

                start=$((i + 1))
                end=$((i + batch_size))
                sed -n "${start},${end}p" $fold_script_filename > ${CLUSTER}_${EPOCH}_${BEAM}_fold_commands_batch_${fold_batch_number}.txt
               
                job_id=$(sbatch --parsable --job-name=$fold_job_name --output=$logs/${CLUSTER}_fold_${EPOCH}_${BEAM}_batch_${fold_batch_number}.out --error=$logs/${CLUSTER}_fold_${EPOCH}_${BEAM}_batch_${fold_batch_number}.err -p ${fold_partition} --export=ALL --cpus-per-task=$batch_size --time=$fold_wall_clock --mem=$fold_ram_per_job ${code_directory}/FOLD_AND_COPY_BACK.sh ${singularity_image_path} ${mount_path} ${code_directory} ${tmp_working_dir} ${code_directory}/${CLUSTER}/${EPOCH}/${BEAM} ${obs_file} ${CLUSTER}_${EPOCH}_${BEAM}_fold_commands_batch_${fold_batch_number}.txt $batch_size $pm_config_file)
                job_ids+=("$job_id")

                fold_batch_number=$((fold_batch_number + 1))
            done

            all_job_ids=$(IFS=, ; echo "${job_ids[*]}")
            exit 0
            
            # Wait for all jobs to complete
            while true; do
                statuses=$(sacct -j $all_job_ids --format=JobID,State --noheader -P)
                unique_completed=$(echo "$statuses" | awk -F'|' '$2=="COMPLETED" && !/\./{print $1}' | wc -l)

                if [ "$unique_completed" -eq ${#job_ids[@]} ]; then
                    echo "All folding jobs completed."
                    echo "${BEAM}" >> search_completed_beams_${CLUSTER}_${EPOCH}.txt
                    break
                else
                    echo "Completed Jobs: $unique_completed of ${#job_ids[@]}. Folding Jobs are still running. Sleeping for 10 minutes."
                    sleep 600
                fi
            done

            


        elif [ "$num_lines" -eq 0 ]; then
            echo "No candidates found to fold. Exiting."
        fi

        break
    fi

    echo "Waiting for folding script to be created. Going to sleep for 10 minutes."
    sleep 600
done

#delete dat files after folding
rm -rf ${code_directory}/${CLUSTER}/${EPOCH}/${BEAM}/03_DEDISPERSION/${CLUSTER}_${EPOCH}_${BEAM}/full/ck00/*.dat


