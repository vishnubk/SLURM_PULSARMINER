#!/bin/bash
cluster="NGC6397"
epoch="20201105"
code_dir="/hercules/scratch/vishnu/SLURM_PULSARMINER"
pm_config="NGC6397_accel_search_gpu_only_accel200.config"

for beam in cfbf{00000..00287}; do
#beam="cfbf00002"
rawdata_basename="${cluster}_${epoch}_${beam}"

if [ -f "${cluster}_${epoch}_${beam}.fil" ]; then
    echo file "${cluster}_${epoch}_${beam}.fil" exists
    slurm_job_file="slurm_jobs_${cluster}_${epoch}_${beam}.sh"
    if [ -f $slurm_job_file ]; then
        slurm_job_file_no_ext="${slurm_job_file%.*}"
        head -n 5 $slurm_job_file > ${slurm_job_file_no_ext}_candidate_shortlist.sh
        echo "slurm_ids=\"\"" >> "${slurm_job_file_no_ext}_candidate_shortlist.sh"
        echo "" >> "${slurm_job_file_no_ext}_candidate_shortlist.sh"
        grep "dedisp=" "$slurm_job_file" > temp_dedisp_lines.txt
        while IFS= read -r line; do
            echo "$line" >> "${slurm_job_file_no_ext}_candidate_shortlist.sh"
            echo "" >> "${slurm_job_file_no_ext}_candidate_shortlist.sh"
            echo 'slurmids="$slurmids:$dedisp"' >> "${slurm_job_file_no_ext}_candidate_shortlist.sh"
            echo "" >> "${slurm_job_file_no_ext}_candidate_shortlist.sh"
        done < temp_dedisp_lines.txt

        # Remove the temporary file
        rm temp_dedisp_lines.txt
        awk '/sift_and_create_fold_command_file=/{print}' "$slurm_job_file" | while read -r line; do 
            echo "$line" | grep -q -- "--dependency=afterok$slurmids" && echo "$line" || echo "$line" | sed "s/--parsable/--parsable --dependency=afterok\$slurmids/"
        done >> "${slurm_job_file_no_ext}_candidate_shortlist.sh"

        echo "" >> "${slurm_job_file_no_ext}_candidate_shortlist.sh"

        echo "timeseries_folds=\$(sbatch --parsable --dependency=afterok:\$sift_and_create_fold_command_file --job-name=timeseries_folds --output=\$logs/${cluster}_${epoch}_${beam}_fold_timeseries_get_alpha.out --error=\$logs/${cluster}_${epoch}_${beam}_fold_timeseries_get_alpha.err -t 4:00:00 -p short.q --mem=220GB --cpus-per-task=48 --wrap=\"${code_dir}/FOLD_TIMESERIES_PER_BEAM.sh /u/vishnu/singularity_images/presto_gpu.sif /hercules ${code_dir} /tmp/${cluster}/${epoch}/${beam} ${code_dir}/$cluster/$epoch/$beam $rawdata_basename $cluster/$epoch/$beam/05_FOLDING/$rawdata_basename/script_fold_timeseries.txt $cluster/$epoch/$beam/05_FOLDING/$rawdata_basename/script_fold.txt 48 ${pm_config} 1\")" >> "${slurm_job_file_no_ext}_candidate_shortlist.sh"
        echo "" >> "${slurm_job_file_no_ext}_candidate_shortlist.sh"
        echo "sbatch --dependency=afterok:\$timeseries_folds --job-name=delete_timeseries --output=\$logs/${cluster}_${epoch}_${beam}_delete_timeseries.out --error=\$logs/${cluster}_${epoch}_${beam}_delete_timeseries.err -t 4:00:00 -p short.q --mem=3GB --cpus-per-task=1 --wrap=\"rm -rf ${code_dir}/${cluster}/${epoch}/${beam}/03_DEDISPERSION/${cluster}_${epoch}_${beam}/full/ck00/*.dat\"" >> "${slurm_job_file_no_ext}_candidate_shortlist.sh"
        exit 0
        

    else
        echo file $slurm_job_file does not exist. Run LAUNCH_SLURM_PULSARMINER.sh on the observation file to generate the slurm job file.
    fi

else
    echo file "${cluster}_${epoch}_${beam}.fil" does not exist. You need to copy over the filterbank file and run filtool.
fi
done
