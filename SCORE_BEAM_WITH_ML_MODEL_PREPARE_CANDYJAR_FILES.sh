#!/bin/bash

cluster=$1
epoch=$2
beam=$3
code_dir=$4
ml_model_dir="$code_dir/ML_MODELS"
meta_file=$5
utc="${$meta_file%.*}"

dir="$code_dir/$cluster/$epoch/$beam"



fold_dir=$dir/05_FOLDING
search_dir="$dir/03_DEDISPERSION"
output_csv="candidates_$basename_dir.csv"
filterbank_file="$code_dir/${cluster}_${epoch}_${beam}.fil"

tmpdir=$(mktemp -d)
fold_script_file="${fold_dir}/${cluster}_${epoch}_${beam}/script_fold.txt"


unique_accel_files=$(grep -oP '(?<=-accelfile )\S+' $fold_script_file | sort | uniq)
unique_accel_files=$(echo "$unique_accel_files" | grep '03_DEDISPERSION' | sort -u | sed 's/.*03_DEDISPERSION\///')
disk_path_accel_files=$(echo "$unique_accel_files" | sed "s#^#$search_dir/#")


echo "Creating directory structure in working directory"
echo "$disk_path_accel_files" | tr ' ' '\n' | xargs -I {} dirname {} | sort -u | while IFS= read -r dir; do
   filename=$(echo "$dir" | grep '03_DEDISPERSION' | sort -u | sed 's/.*03_DEDISPERSION/03_DEDISPERSION/')
   mkdir -p "$tmpdir/$filename"
done


echo "Copying files to working directory"
echo "$disk_path_accel_files" | tr ' ' '\n' | while IFS= read -r file; do
    file=${file%.*}
    filename=$(echo "$file" | grep '03_DEDISPERSION' | sort -u | sed 's/.*03_DEDISPERSION/03_DEDISPERSION/')

    chunk_dir=$(dirname "$filename")
    tmp_chunk_dir=$tmpdir/$chunk_dir

    rsync -Pav ${code_dir}/${cluster}/${epoch}/${beam}/$filename $tmp_chunk_dir
done


#mkdir -p $tmpdir/05_FOLDING


rsync -Pav --exclude='LOG*' --exclude='*.ps' --exclude='*.png' --exclude='*fold_commands_batch*' --exclude='pm_run_multithread' --exclude='ps_to_png_parallel.sh' $fold_dir $tmpdir
rsync -Pav $ml_model_dir $tmpdir

tmp_fold_dir=$tmpdir/$(basename $fold_dir)
tmp_search_dir=$tmpdir/$(basename $search_dir)
tmp_ml_model_dir=$tmpdir/$(basename $ml_model_dir)

pics_results="pics_scores.csv"
# If result file does not exist, run the code
if [ ! -f $fold_dir/${cluster}_${epoch}_${beam}/$pics_results ]; then
    singularity exec -H $HOME:/home1 -B /hercules/:/hercules/ /hercules/scratch/vishnu/singularity_images/trapum_pulsarx_fold_docker_20220411.sif python $code_dir/pics_classifier_multiple_models.py -i $tmp_fold_dir/${cluster}_${epoch}_${beam} -m $tmp_ml_model_dir
        
fi

singularity exec -H $HOME:/home1 -B /hercules/:/hercules/ /u/vishnu/singularity_images/presto_gpu.sif python $code_dir/prepare_cands_for_candyjar.py -pfds $tmp_fold_dir/${cluster}_${epoch}_${beam} -beam_name $beam -pointing $cluster -epoch $epoch -search $tmp_search_dir -meta $meta_file -bary -filterbank_path $filterbank_file -code_d $code_dir -utc $utc
#Copy results back
rsync -Pav $tmp_fold_dir/${cluster}_${epoch}_${beam}/pics_scores.csv $fold_dir/${cluster}_${epoch}_${beam}/
rsync -Pav $tmp_fold_dir/${cluster}_${epoch}_${beam}/candidates.csv $fold_dir/${cluster}_${epoch}_${beam}/
rsync -Pav $tmp_fold_dir/${cluster}_${epoch}_${beam}/candidates_ml_selected.csv $fold_dir/${cluster}_${epoch}_${beam}/

rm -rf $tmpdir
