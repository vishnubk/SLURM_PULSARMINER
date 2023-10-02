#!/bin/bash
logs='slurm_job_logs'
mkdir -p $logs


cluster="NGC6397"
epoch="20201105"
code_dir="/hercules/scratch/vishnu/SLURM_PULSARMINER"
ml_model_dir="$code_dir/ML_MODELS/"
meta_file="2020-11-05T10:13:34.meta"

directories="$code_dir/$cluster/$epoch/"
slurmids=""
for dir in $directories*; do
    beam=$(basename $dir)
    if [ -d "$dir" ] && [[ $beam == cf* || $beam == if* ]]; then
        fold_dir=$dir/05_FOLDING/${cluster}_${epoch}_${beam}/
        if [ -d "$fold_dir" ]; then
            # Check if at least one .pfd file exists in that directory
            num_pfd_files=$(ls "$fold_dir"/*.pfd 2>/dev/null | wc -l)
            if [ "$num_pfd_files" -ge 1 ]; then
                echo "Submitting job for $dir"
                score=$(sbatch --parsable --job-name=score_${cluster} --output="$logs/ml_score_${cluster}_${epoch}_${beam}.out" --error="$logs/ml_score_${cluster}_${epoch}_${beam}.err" -p long.q --time 1-00:00:00 --export=ALL --cpus-per-task=48 --mem=350GB --wrap="${code_dir}/SCORE_BEAM_WITH_ML_MODEL_PREPARE_CANDYJAR_FILES.sh $cluster $epoch $beam $code_dir $meta_file")
                slurmids="$slurmids:$score"
            fi
        fi
            
    fi
done


while true; do
  # Count the number of jobs in the queue with names starting with "score_$cluster"
  job_count=$(squeue -n "score_${cluster}" --noheader | wc -l)
  
  if [[ $job_count -eq 0 ]]; then
    echo "All jobs are complete. Running final command."
    echo "Running merging candjar files"
    cd $code_dir/$cluster/$epoch/ 
    find . -name "candidates.csv" -exec awk 'NR==1{if (!header_printed) {print; header_printed=1}} FNR > 1' {} + > combined_candidates.csv && awk -F, -v OFS=',' 'NR==1{print $0} NR>1{$32 = $3 "/" $32; print $0}' combined_candidates.csv > modified_combined_candidates.csv && mv modified_combined_candidates.csv candidates.csv
    # Your final command here
    break
  else
    echo "Waiting for jobs to complete. Jobs remaining: $job_count."
    sleep 60  # Wait for 60 seconds before checking again
  fi
done

#Merge and make global candjar file

#find . -name "candidates.csv" -exec awk 'NR==1{print; next} FNR > 1' {} + > candidates.csv

#sing_command="singularity exec -H $HOME:/home1 -B /hercules:/hercules /u/vishnu/singularity_images/presto_gpu.sif"
#job_command="python ${code_dir}/MERGE_CANDYJAR_FILES.py $cluster $epoch $code_dir"
#sbatch --dependency=afterok:$slurmids --job-name=merge --output="$logs/ml_merge_${cluster}_${epoch}.out" --error="$logs/ml_merge_${cluster}_${epoch}.err" -p short.q --time 04:00:00 --export=ALL --cpus-per-task=1 --mem=20GB --wrap="$sing_command $job_command"


      

