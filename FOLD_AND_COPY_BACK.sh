#!/bin/bash

sing_image=$1
data_dir=$2
code_dir=$3
working_dir=$4
previous_results=$5
raw_data=$6
fold_script_file=$7
ncpus=$8
pm_config=$9
fold_timeseries=0
rfifind_mask=${previous_results}/01_RFIFIND
search_results=${previous_results}/03_DEDISPERSION
fold_results=${previous_results}/05_FOLDING

#Cleaning up any prior runs
rm -rf $working_dir


mkdir -p $working_dir
unique_accel_files=$(grep -oP '(?<=-accelfile )\S+' $fold_script_file | sort | uniq)
unique_accel_files=$(echo "$unique_accel_files" | grep '03_DEDISPERSION' | sort -u | sed 's/.*03_DEDISPERSION\///')
disk_path_accel_files=$(echo "$unique_accel_files" | sed "s#^#$search_results/#")

echo "Creating directory structure in working directory"
echo "$disk_path_accel_files" | tr ' ' '\n' | xargs -I {} dirname {} | sort -u | while IFS= read -r dir; do
   filename=$(echo "$dir" | grep '03_DEDISPERSION' | sort -u | sed 's/.*03_DEDISPERSION/03_DEDISPERSION/')
   mkdir -p "$working_dir/$filename"
done


echo "Copying files to working directory"
echo "$disk_path_accel_files" | tr ' ' '\n' | while IFS= read -r file; do
 filename=$(echo "$file" | grep '03_DEDISPERSION' | sort -u | sed 's/.*03_DEDISPERSION/03_DEDISPERSION/')
 dir=$(dirname "$filename")
 truncated_filename="${file%_ACCEL_*}"

if [ $fold_timeseries == 1 ]; then
  rsync -Pav ${truncated_filename}* "$working_dir/$dir/"
else
  rsync -Pav --exclude='*.dat' ${truncated_filename}* "$working_dir/$dir/"

fi
done

## Copy the raw data, rfifind mask, search and sifting results to the working directory
echo "Copying config file"
rsync -Pav ${code_dir}/${pm_config} $working_dir
echo "Copying raw data"
rsync -PavL $raw_data $working_dir
echo "Copying rfifind mask"
rsync -Pav $rfifind_mask $working_dir
#rsync -Pav $fold_results $working_dir

basename_rawdata=$(basename "$raw_data")
raw_data=${working_dir}/${basename_rawdata}
rawdata_basename="${basename_rawdata%.*}"

mkdir -p ${working_dir}/05_FOLDING/$rawdata_basename

echo "Copying folding code"
rsync -Pav $code_dir/pm_run_multithread ${working_dir}/05_FOLDING/$rawdata_basename/
rsync -Pav $code_dir/$fold_script_file ${working_dir}/05_FOLDING/$rawdata_basename/

basename_fold_script_file=$(basename "$fold_script_file")
fold_script_file=${working_dir}/05_FOLDING/$rawdata_basename/${basename_fold_script_file}


cd ${working_dir}/05_FOLDING/$rawdata_basename/
singularity exec -H $HOME:/home1 -B $data_dir:$data_dir $sing_image python pm_run_multithread -cmdfile $basename_fold_script_file -ncpus $ncpus

#PS TO PNG HACK! ONLY WORKS IN MY LOCAL DIR.
rsync -Pav $code_dir/ps_to_png_parallel.sh ${working_dir}/05_FOLDING/$rawdata_basename/
singularity exec -H $HOME:/home1 -B $data_dir:$data_dir /u/vishnu/singularity_images/compare_pulsar_search_algorithms.simg ${working_dir}/05_FOLDING/$rawdata_basename/ps_to_png_parallel.sh 


#Copy Results back

rsync -Pav ${working_dir}/05_FOLDING $previous_results


#Clean Up
rm -rf $working_dir
rm -rf $code_dir/$basename_fold_script_file
