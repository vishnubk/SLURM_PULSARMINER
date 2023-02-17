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

# parse command-line arguments
while getopts ":ho:p:t:" opt; do
  case $opt in
    h)
      echo "SLURM PULSARMINER Launch Script. With Great Parallelization Comes Great Responsibility. Ensure that you fill the slurm config file with the correct values for your system."
      echo ""
      echo "Usage: $0 [-h] [-o observation_file] [-p config_file] [-t tmp_directory]"
      echo ""
      echo "Options:"
      echo "  -h            Show this help message and exit"
      echo "  -o FILE       Observation file to process"
      echo "  -p FILE       Configuration file"
      echo "  -t DIR        Temporary directory (default is /tmp)"
      exit 0
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
  echo "Usage: $0 [-h] [-o observation_file] [-p config_file] [-t tmp_directory]"
  echo ""
  echo "Options:"
  echo "  -h            Show this help message and exit"
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

singularity exec -H $HOME:/home1 -B $mount_path:$mount_path $singularity_image_path python ${code_directory}/create_slurm_jobs.py -s ${code_directory}/$slurm_config_file -p ${code_directory}/$pm_config_file -o $obs_file 

source ${code_directory}/slurm_jobs_${CLUSTER}_${EPOCH}_${BEAM}.sh


fold_script_filename=${CLUSTER}/${EPOCH}/${BEAM}/05_FOLDING/${CLUSTER}_${EPOCH}_${BEAM}/script_fold.txt
fold_batch_number=1
while true; do
  if [ -f $fold_script_filename ]; then
    # Get the number of lines in the file
    num_lines=$(wc -l < $fold_script_filename)

    if [ "$num_lines" -gt 0 ]; then
      # Divide the lines into batches of $fold_cpus_per_task or less
      for i in $(seq 0 $((fold_cpus_per_task - 1)) $((num_lines - 1))); do
        # Calculate the number of lines in this batch
        batch_size=$((num_lines - i))
        if [ "$batch_size" -gt "$fold_cpus_per_task" ]; then
          batch_size=$fold_cpus_per_task
        fi

        # Get the lines for this batch and write them to a temporary file
        start=$((i + 1))
        end=$((i + batch_size))
        sed -n "${start},${end}p" $fold_script_filename > ${CLUSTER}_${EPOCH}_${BEAM}_fold_commands_batch_${fold_batch_number}.txt

        sbatch --job-name=$fold_job_name --output=$logs/${CLUSTER}_fold_${EPOCH}_${BEAM}_batch_${fold_batch_number}.out --error=$logs/${CLUSTER}_fold_${EPOCH}_${BEAM}_batch_${fold_batch_number}.err -p ${fold_partition} --export=ALL --cpus-per-task=$batch_size --time=$fold_wall_clock --mem=$fold_ram_per_job ${code_directory}/FOLD_AND_COPY_BACK.sh ${singularity_image_path} ${mount_path} ${code_directory} ${tmp_working_dir} ${code_directory}/${CLUSTER}/${EPOCH}/${BEAM} ${obs_file} ${CLUSTER}_${EPOCH}_${BEAM}_fold_commands_batch_${fold_batch_number}.txt $batch_size 
        fold_batch_number=$((fold_batch_number + 1))
      done
    elif [ "$num_lines" -eq 0 ]; then
      echo "No candidates found to fold. Exiting."
    fi

    # Exit the loop once we've processed the file
    break
  fi

  # Wait 10 minutes before checking again
  echo "Waiting for folding script to be created. Going to sleep for 10 minutes."
  sleep 600
done



