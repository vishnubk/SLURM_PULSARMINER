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
inf_file=${dat_file::-4}.inf

#Cleaning up any prior runs
rm -rf $working_dir

mkdir -p $working_dir
mkdir -p $output_dir

## Copy the *.dat and *.inf file to the working directory 
rsync -Pav $dat_file $working_dir 
rsync -Pav $inf_file $working_dir
#
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
singularity exec -H $HOME:/home1 -B $data_dir:$data_dir $sing_image python ${code_dir}/periodicity_search.py -i $basename_dat -z $zmax -w $wmax -n $ncpus -t $working_dir -s $numharm
status=$?
if [ $status -ne 0 ]; then
    echo "Error in periodicity_search.py"
    echo "Cleaning up"
    rm -rf $working_dir
    exit 1
fi

## Copy Results back

rsync -Pav $working_dir/*ACCEL*  $output_dir
rsync -Pav $working_dir/*.inf  $output_dir

#Clean Up
rm -rf $working_dir
