#!/usr/bin/python3

import os, sys
import re

input_file = sys.argv[1]  # Replace with your input file name
match = re.search(r'slurm_jobs_(\w+)_(\d+)_(\w+)\.sh', input_file)
cluster, epoch, beam = match.groups()

output_file = 'relaunch_' + input_file

sbatch_regex = re.compile(r'sbatch .+ --error=(\S+)')
logs_prefix = "/hercules/scratch/vkrishna/SLURM_PULSARMINER/slurm_job_logs/"
zmax_list = [0, 200]
sbatch_commands = []
dedisp_flag=0
# Read your original script
with open(input_file, 'r') as f:
    data_path_line = ""
    for line in f:
        # Store data_path initialization
        if "data_path" in line:
            data_path_line = line
        # Match sbatch and --error logs
        sbatch_match = sbatch_regex.search(line)
        if sbatch_match:
            error_log = os.path.basename(sbatch_match.group(1))
            full_error_path = os.path.join(logs_prefix, error_log)
            
          
            # Check if log file exists
            if not os.path.exists(full_error_path):
                if dedisp_flag==0:
                    #print(f"Dedispersion for this batch has already completed. Removing dependency flag")
                   
                    if line.startswith("search="):
                        line = line.replace("--dependency=afterok:$dedisp", "")
                    #line = line.replace(" --dependency=afterok:", "")
                    elif line.startswith("dedisp="):
                        dedisp_flag=1
                    
                        
                elif line.startswith("sift_and_create_fold_command_file="):
                    line = line.replace("--dependency=afterok$slurmids", "")
                sbatch_commands.append(line)

# Write to the new output file
with open(output_file, 'w') as f:
    f.write("#!/bin/bash\n")
    f.write("logs='slurm_job_logs/'\n")
    f.write("mkdir -p $logs\n")
    
    # Hardcoded lines for maximum job submission
    f.write("# get the maximum number of jobs that the user can submit minus 5.\n")
    f.write("maxjobs_raw=$(sacctmgr list associations format=user,maxsubmitjobs -n | grep $USER | awk '{print $2}')\n")
    f.write("maxjobs=$(( maxjobs_raw - 5 ))\n")
    f.write("# If maxjobs is greater than 20k, fix it at 20k\n")
    f.write("if [ \"$maxjobs\" -gt 20000 ]; then\n")
    f.write("    maxjobs=20000\n")
    f.write("fi\n")
    f.write("slurm_user_requested_jobs=$maxjobs\n")
    
    # Hardcoded check_job_submission_limit function
    f.write("check_job_submission_limit () {\n")
    f.write("    while true; do\n")
    f.write("        numjobs=$(squeue -u $USER -h | wc -l)\n")
    f.write("        if [ \"$((numjobs))\" -lt \"$((slurm_user_requested_jobs))\" ]; then\n")
    f.write("            break\n")
    f.write("        else\n")
    f.write("            echo \"Number of jobs submitted is $numjobs. Waiting for them to finish before submitting more jobs.\"\n")
    f.write("            sleep 100\n")
    f.write("        fi\n")
    f.write("    done\n")
    f.write("}\n")
    
    f.write(data_path_line)
    for cmd in sbatch_commands:
        if cmd.startswith("sift_and_create_fold_command_file"):
            for zmax in zmax_list:
                zmax = int(zmax)
                f.write('# Count the number of files matching *_ACCEL_%d.txtcand in the directory\n' % zmax)
                additional_code = f'''mkdir -p SEARCH_PROGRESS
progress_file="SEARCH_PROGRESS/search_progress_{cluster}_{epoch}_{beam}.txt"
while true; do
found_files=$(find {cluster}/{epoch}/{beam}/03_DEDISPERSION -name '*_ACCEL_{zmax}.txtcand' | wc -l)
percentage=$(( 100 * $found_files / 8992 ))
bar=""
for i in $(seq 1 $percentage); do
    bar="${{bar}}#"
done
for i in $(seq 1 $((100 - $percentage))); do
    bar="${{bar}} "
done
echo -ne "Date: $(date), Search Progress for zmax {zmax}: [${{bar}}] $percentage% \\r"
echo "Date: $(date), Search Progress for zmax {zmax}: [${{bar}}] $percentage%" >> $progress_file
if [ $found_files -lt 8992 ]; then
    sleep 30m
else
    echo -e "\\nSearch Category zmax: {zmax} completed."
    echo "Search Category zmax: {zmax} completed. Date: $(date)" >> $progress_file
    break
fi
done
'''
                f.write(additional_code)
                f.write("\n")

        f.write(cmd)
        f.write("\n")
        f.write("check_job_submission_limit\n")
        f.write("\n")


print(f"Retained sbatch commands written to {output_file}")




