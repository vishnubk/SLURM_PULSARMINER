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
    #if [ -d "$dir" ] && [[ $beam == cf* || $beam == if* ]] && [ $beam == "cfbf00002" ]; then
    if [ -d "$dir" ] && [[ $beam == cf* || $beam == if* ]]; then
        fold_dir=$dir/05_FOLDING/${cluster}_${epoch}_${beam}/
        if [ -d "$fold_dir" ]; then
            # Check if at least one .pfd file exists in that directory
            num_pfd_files=$(ls "$fold_dir"/*.pfd 2>/dev/null | wc -l)
            if [ "$num_pfd_files" -ge 1 ]; then
                echo "Submitting job for $dir"
                score=$(sbatch --parsable --job-name=score_${cluster} --output="$logs/ml_score_${cluster}_${epoch}_${beam}.out" --error="$logs/ml_score_${cluster}_${epoch}_${beam}.err" -p short.q --time 4:00:00 --export=ALL --cpus-per-task=48 --mem=350GB --wrap="${code_dir}/SCORE_BEAM_WITH_ML_MODEL_PREPARE_CANDYJAR_FILES.sh $cluster $epoch $beam $code_dir $meta_file")
                slurmids="$slurmids:$score"
            fi
        fi
            
    fi
done

exit 0
while true; do
  # Count the number of jobs in the queue with names starting with "score_$cluster"
  job_count=$(squeue -n "score_${cluster}" --noheader | wc -l)
  
  if [[ $job_count -eq 0 ]]; then
    echo "All jobs are complete. Running final command."
    echo "Running merging candjar files"
    cd $code_dir/$cluster/$epoch/
    find . -name "candidates.csv" -exec awk 'NR==1{if (!header_printed) {print; header_printed=1}} FNR > 1' {} + > combined_candidates.csv && awk -F, -v OFS=',' 'NR==1{print $0} NR>1{split($34, a, "/"); gsub(".fil", "", a[length(a)]); $32 = $3 "/05_FOLDING/" a[length(a)] "/" $32; print $0}' combined_candidates.csv > modified_combined_candidates.csv && mv modified_combined_candidates.csv candidates.csv

    find . -name "candidates_ml_selected.csv" -exec awk 'NR==1{if (!header_printed) {print; header_printed=1}} FNR > 1' {} + > combined_candidates_ml_selected.csv && awk -F, -v OFS=',' 'NR==1{print $0} NR>1{split($34, a, "/"); gsub(".fil", "", a[length(a)]); $32 = $3 "/05_FOLDING/" a[length(a)] "/" $32; print $0}' combined_candidates_ml_selected.csv > modified_combined_candidates_ml_selected.csv && mv modified_combined_candidates_ml_selected.csv candidates_ml_selected.csv
 
    rm -rf combined_candidates.csv
    rm -rf combined_candidates_ml_selected.csv
    break
  else
    echo "Waiting for jobs to complete. Jobs remaining: $job_count."
    sleep 60  # Wait for 60 seconds before checking again
  fi
done



      

