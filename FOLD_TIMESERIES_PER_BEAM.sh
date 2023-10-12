#!/bin/bash

sing_image=$1
data_dir=$2
code_dir=$3
working_dir=$4
previous_results=$5
rawdata_basename=$6
fold_script_file=$7
raw_fold_script_file=$8
ncpus=$9
pm_config=${10}
fold_timeseries=${11}
rfifind_mask=${previous_results}/01_RFIFIND
search_results=${previous_results}/03_DEDISPERSION
fold_results=${previous_results}/05_FOLDING
birdie_directory=${previous_results}/02_BIRDIES

#Cleaning up any prior runs
rm -rf $working_dir


mkdir -p $working_dir
#Copy birdie directory
rsync -Pav $birdie_directory $working_dir

unique_accel_files=$(grep -oP '(?<=-accelfile )\S+' $fold_script_file | sort | uniq)
unique_accel_files=$(echo "$unique_accel_files" | grep '03_DEDISPERSION' | sort -u | sed 's/.*03_DEDISPERSION\///')
disk_path_accel_files=$(echo "$unique_accel_files" | sed "s#^#$search_results/#")

unique_timeseries_files=$(grep -o '/[^ ]*\.dat' $fold_script_file | sort | uniq)
unique_timeseries_files=$(echo "$unique_timeseries_files" | grep '03_DEDISPERSION' | sort -u | sed 's/.*03_DEDISPERSION\///')
disk_path_timeseries_files=$(echo "$unique_timeseries_files" | sed "s#^#$search_results/#")


echo "Creating directory structure in working directory"
echo "$disk_path_accel_files" | tr ' ' '\n' | xargs -I {} dirname {} | sort -u | while IFS= read -r dir; do
  filename=$(echo "$dir" | grep '03_DEDISPERSION' | sort -u | sed 's/.*03_DEDISPERSION/03_DEDISPERSION/')
  mkdir -p "$working_dir/$filename"
done


echo "Copying accel files to working directory"
echo "$disk_path_accel_files" | tr ' ' '\n' | while IFS= read -r file; do
   filename=$(echo "$file" | grep '03_DEDISPERSION' | sort -u | sed 's/.*03_DEDISPERSION/03_DEDISPERSION/')
   dir=$(dirname "$filename")
   truncated_filename="${file%_ACCEL_*}"
   rsync -Pav --exclude='*.dat' ${truncated_filename}* "$working_dir/$dir/"
done

echo "Copying timeseries files to working directory"
if [ $fold_timeseries == 1 ]; then
   echo "$disk_path_timeseries_files" | tr ' ' '\n' | while IFS= read -r file; do
   filename=$(echo "$file" | grep '03_DEDISPERSION' | sort -u | sed 's/.*03_DEDISPERSION/03_DEDISPERSION/')
   dir=$(dirname "$filename")
   rsync -Pav $file "$working_dir/$dir/"
   rsync -Pav "${file%.dat}.inf" "$working_dir/$dir/"
   done

fi


## Copy the raw data, rfifind mask, search and sifting results to the working directory
echo "Copying config file"
rsync -Pav ${code_dir}/${pm_config} $working_dir
echo "Copying rfifind mask"
rsync -Pav $rfifind_mask $working_dir

#
mkdir -p ${working_dir}/05_FOLDING_TIMESERIES/$rawdata_basename
mkdir -p ${working_dir}/05_FOLDING/$rawdata_basename

echo "Copying folding code"
rsync -Pav $code_dir/pm_run_multithread ${working_dir}/05_FOLDING_TIMESERIES/$rawdata_basename/
rsync -Pav $code_dir/$fold_script_file ${working_dir}/05_FOLDING_TIMESERIES/$rawdata_basename/
rsync -Pav $code_dir/$raw_fold_script_file ${working_dir}/05_FOLDING_TIMESERIES/$rawdata_basename/
rsync -Pav $fold_results/$rawdata_basename/candidates.csv ${working_dir}/05_FOLDING/$rawdata_basename/

basename_fold_script_file=$(basename "$fold_script_file")
fold_script_file=${working_dir}/05_FOLDING_TIMESERIES/$rawdata_basename/${basename_fold_script_file}

basename_raw_fold_script_file=$(basename "$raw_fold_script_file")
raw_fold_script_file=${working_dir}/05_FOLDING_TIMESERIES/$rawdata_basename/${basename_raw_fold_script_file}

cd ${working_dir}/05_FOLDING_TIMESERIES/$rawdata_basename/
singularity exec -H $HOME:/home1 -B $data_dir:$data_dir $sing_image python pm_run_multithread -cmdfile $basename_fold_script_file -ncpus $ncpus

#-l flag is set when raw data is already folded. The code then acts as a shortlister else it will create a separate raw candidates file that needs to be folded
singularity exec -H $HOME:/home1 -B $data_dir:$data_dir $sing_image python ${code_dir}/calculate_alpha_and_shortlist_cands.py -pfds ${working_dir}/05_FOLDING_TIMESERIES/$rawdata_basename/ -s $raw_fold_script_file -l



#rsync -Pav --exclude='*.pfd*' --exclude='LOG*' --exclude='pm_run_multithread' ${working_dir}/05_FOLDING_TIMESERIES $previous_results
rsync -Pav --exclude='pm_run_multithread' ${working_dir}/05_FOLDING_TIMESERIES $previous_results
rsync -Pav ${working_dir}/05_FOLDING $previous_results

#Clean Up
rm -rf $working_dir
