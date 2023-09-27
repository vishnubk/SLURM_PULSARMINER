#!/bin/bash
filename="/hercules/scratch/vkrishna/SLURM_PULSARMINER/NGC441/20200709/cfbf00009/05_FOLDING/NGC441_20200709_cfbf00009/script_fold.txt"
output_dir_folding="/hercules/scratch/vkrishna/SLURM_PULSARMINER/NGC441/20200709/cfbf00009/05_FOLDING/NGC441_20200709_cfbf00009"
while read -r line; do
    # Use awk to extract the filename after the '-o' flag
    filename=$(echo "$line" | awk '{for(i=1;i<=NF;i++) if ($i == "-o") print $(i+1)}')

    # Extract the accelcand number
    accel_cand_number=$(echo "$line" | awk '{for(i=1;i<=NF;i++) if ($i == "-accelcand") print $(i+1)}')
    
    # Extract the accelfile
    accel_file=$(echo "$line" | awk '{for(i=1;i<=NF;i++) if ($i == "-accelfile") print $(i+1)}')

    # Check for "JERK" in accelfile
    if [[ "$accel_file" == *"JERK"* ]]; then
        pfd_file="${output_dir_folding}/${filename}_JERK_Cand_${accel_cand_number}.pfd"
        bestprof_file="${output_dir_folding}/${filename}_JERK_Cand_${accel_cand_number}.pfd.bestprof"
    else
        pfd_file="${output_dir_folding}/${filename}_ACCEL_Cand_${accel_cand_number}.pfd"
        bestprof_file="${output_dir_folding}/${filename}_ACCEL_Cand_${accel_cand_number}.pfd.bestprof"
    fi


    

    # Check if the .pfd file exists
    if [[ ! -f "$pfd_file" ]]; then
        echo "Missing pfd file: ${filename}.pfd"
        # Run the prepfold command
        echo "$line"
    fi
done < "$filename"
