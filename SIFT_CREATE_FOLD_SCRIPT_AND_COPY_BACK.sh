#!/bin/bash

sing_image=$1
data_dir=$2
code_dir=$3
working_dir=$4
previous_results=$5
raw_data=$6
pm_config=$7
fold_timeseries=0
rfifind_mask=${previous_results}/01_RFIFIND
search_results=${previous_results}/03_DEDISPERSION

#Cleaning up any prior runs
rm -rf $working_dir


mkdir -p $working_dir


## Copy the raw data, rfifind mask, search and sifting results to the working directory
rsync -Pav $pm_config $working_dir
rsync -Pav $raw_data $working_dir
rsync -Pav $rfifind_mask $working_dir
rsync -Pav $code_dir/pm_run_multithread $working_dir

basename_rawdata=$(basename "$raw_data")
raw_data=${working_dir}/${basename_rawdata}

basename_config=$(basename "$pm_config")
pm_config=${working_dir}/${basename_config}

if [ $fold_timeseries == "1" ]; then
  rsync -Pav $search_results $working_dir
else
  rsync -Pav --exclude='*.dat' $search_results $working_dir

fi


singularity exec -H $HOME:/home1 -B $data_dir:$data_dir $sing_image python ${code_dir}/run_sifting_and_create_fold_script.py -p $pm_config -o $raw_data -r $working_dir



#Copy Results back

rsync -Pav ${working_dir}/04_SIFTING $previous_results
rsync -Pav ${working_dir}/05_FOLDING $previous_results

# #Clean Up
#rm -rf $working_dir
