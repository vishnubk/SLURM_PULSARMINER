#!/bin/bash

sing_image=$1
data_dir=$2
code_dir=$3
dat_file=$4
zmax=$5
wmax=$6
numharm=$7
ncpus=$8
working_dir=$9
output_dir=${10}
segment=${11}
chunk=${12}
gpu_flag=${13}

inf_file=${dat_file::-4}.inf

# Check if the output file exists
base_name=$(basename "$dat_file")
file_string=$(echo "$base_name" | awk -F'_' '{print $1"_"$2"_"$3}')_${segment}_${chunk}
dm_value="DM${base_name##*DM}"; dm_value=${dm_value%.dat}
output_search_file=${output_dir}/${file_string}_${dm_value}_ACCEL_${zmax}.txtcand

if [ -s "$output_search_file" ]; then
    echo "Output File: $(basename $output_search_file) exists."
    if (( $(wc -l < "$output_search_file") > 0 )); then
        echo "Output File: $(basename $output_search_file) has more than zero lines, exiting with status 0."
        exit 0
    else
        echo "Output File: $(basename $output_search_file) is not empty, not exiting."
    fi
fi


#Cleaning up any prior runs
rm -rf $working_dir

mkdir -p $working_dir
mkdir -p $output_dir

## Copy the *.dat and *.inf file to the working directory 
rsync -Pav $dat_file $working_dir 
rsync -Pav $inf_file $working_dir

basename_inf=$(basename "$inf_file")


basename_dat=$(basename "$dat_file")

if [ $segment != "full" ]; then
    
    singularity exec -H $HOME:/home1 -B $data_dir:$data_dir $sing_image python ${code_dir}/split_datfiles.py -i $basename_dat -c $chunk -s $segment -w $working_dir
    status=$?
    if [ $status -ne 0 ]; then
        echo "Error in split_datfiles.py"
        echo "Cleaning up"
        rm -rf $working_dir
        exit 1
    fi
    basename_dat=${basename_dat/full/$segment}
    basename_dat=${basename_dat/ck00/$chunk}
fi

if [[ $gpu_flag -eq 1 ]]
then

    singularity exec --nv -H $HOME:/home1 -B $data_dir:$data_dir $sing_image python ${code_dir}/periodicity_search.py -i $basename_dat -z $zmax -w $wmax -n $ncpus -t $working_dir -s $numharm
    status=$?
    if [ $status -ne 0 ]; then
        echo "Error in periodicity_search.py"
        echo "Cleaning up"
        rm -rf $working_dir
        exit 1
    fi
else
    singularity exec --nv -H $HOME:/home1 -B $data_dir:$data_dir $sing_image python ${code_dir}/periodicity_search.py -i $basename_dat -z $zmax -w $wmax -n $ncpus -t $working_dir -s $numharm -g
    status=$?
    if [ $status -ne 0 ]; then
        echo "Error in periodicity_search.py"
        echo "Cleaning up"
        rm -rf $working_dir
        exit 1
    fi

fi

#rsync -Pav $working_dir/*ACCEL*  $output_dir
#rsync -Pav $working_dir/*.inf  $output_dir

if [[ $wmax -eq 0 ]]; then
    base_file1="${file_string}_${dm_value}_ACCEL_${zmax}.txtcand"
    base_file2="${file_string}_${dm_value}.inf"
    base_file3="${file_string}_${dm_value}_ACCEL_${zmax}"
    base_file4="${file_string}_${dm_value}_ACCEL_${zmax}.cand"
else
    base_file1="${file_string}_${dm_value}_ACCEL_${zmax}_JERK_${wmax}.txtcand"
    base_file2="${file_string}_${dm_value}.inf"
    base_file3="${file_string}_${dm_value}_ACCEL_${zmax}_JERK_${wmax}"
    base_file4="${file_string}_${dm_value}_ACCEL_${zmax}_JERK_${wmax}.cand"
fi

#Group 1 is for the case when accel search finds no candidates and only outputs an empty .txtcand file

expected_files_group1=("$working_dir/$base_file1" "$working_dir/$base_file2")
expected_files_group2=("$working_dir/$base_file3" "$working_dir/$base_file4")

# Rsync files from group1
for file in "${expected_files_group1[@]}"; do
    echo rsync -Pav "$file" "$output_dir/"
    rsync -Pav "$file" "$output_dir/"
    status=$?
    if [ $status -ne 0 ]; then
        echo "Error: rsync of $(basename $file) failed" >&2
        exit 1
    fi
done

# Check the .txtcand file and rsync files from group2 if necessary
txtcand_file="$working_dir/$base_file1"
if (( $(wc -l < "$txtcand_file") > 0 )); then
    for file in "${expected_files_group2[@]}"; do
        echo rsync -Pav "$file" "$output_dir/"
        rsync -Pav "$file" "$output_dir/"
        status=$?
        if [ $status -ne 0 ]; then
            echo "Error: rsync of $(basename $file) failed" >&2
            exit 1
        fi
    done
fi

# Clean Up
rm -rf $working_dir

expected_files_group1=("$output_dir/$base_file1" "$output_dir/$base_file2")
expected_files_group2=("$output_dir/$base_file3" "$output_dir/$base_file4")


# Check if the expected output files exist 
all_files_exist_in_group1=true
for file in "${expected_files_group1[@]}"; do
    if [[ ! -e $file ]]; then
        echo "Error: Output file $file does not exist" >&2
        all_files_exist_in_group1=false
        break
    else
        echo "Output File: $(basename $file) exists."
    fi
done

if [[ $all_files_exist_in_group1 == true ]]; then
    txtcand_file="$output_dir/${file_string}_${dm_value}_ACCEL_${zmax}.txtcand"
    if (( $(wc -l < "$txtcand_file") > 0 )); then
        echo "Output File: $(basename $txtcand_file) exists and is not empty, so checking for the search candidates file."
        for file in "${expected_files_group2[@]}"; do
            if [[ ! -e $file ]]; then
                echo "Error: Expected output file $file from group 2 does not exist" >&2
                exit 1
            else
                echo "Output File: $(basename $file) exists."
            fi
        done
    fi
else
    exit 1
fi





