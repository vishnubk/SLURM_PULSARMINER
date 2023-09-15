#!/bin/bash

cluster="NGC6397"
epoch="20201105"
meta_file="2020-11-05T10:13:34.meta"
utc_time="2020-11-05T10:13:34"
#output_dir="CANDIDATE_VIEWER/${cluster}/${epoch}/"
output_dir="CANDIDATE_VIEWER/${cluster}/${epoch}/EVERYTHING"
mkdir -p "$output_dir"
output_file="${output_dir}/candidates.csv"
sing_command="singularity exec -H /u/vishnu:/home1 -B /hercules/:/hercules/ /u/vishnu/singularity_images/presto_gpu.sif"

# Loop through directories starting with "cf" or "if" in the path "$cluster/$epoch"
for beam in "$cluster/$epoch"/{cf,if}*; do
  # Check if it's a directory
  if [ -d "$beam" ]; then
    beam_name=$(basename "$beam") # Get the base name of the directory as the beam name
    # Run the python command with the appropriate variables
    #$sing_command python get_csv_from_pfds.py -pfds "$beam"/05_FOLDING/"$cluster"_"$epoch"_"$beam_name"/*.pfd -meta "$meta_file" -beam_name "$beam_name" -utc "$utc_time" -out "$output_file" -copy_ml_cands_only
    $sing_command python get_csv_from_pfds.py -pfds "$beam"/05_FOLDING/"$cluster"_"$epoch"_"$beam_name"/*.pfd -meta "$meta_file" -beam_name "$beam_name" -utc "$utc_time" -out "$output_file" 
  fi
done
