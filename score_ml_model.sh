#!/bin/bash

model=/hercules/scratch/vishnu/SLURM_PULSARMINER/MeerKAT_L_SBAND_COMBINED_Best_Recall.pkl
epoch=20201105
cluster=NGC6397
results_dir_high_score=/hercules/scratch/vishnu/SLURM_PULSARMINER/ML_SELECTED/$cluster/$epoch/
mkdir -p $results_dir_high_score
for dir in /hercules/scratch/vishnu/SLURM_PULSARMINER/$cluster/$epoch/*;do
    basename_dir=$(basename $dir)
    fold_dir=$dir/05_FOLDING/${cluster}_${epoch}_${basename_dir}

    basename_model=$(basename $model)
    pics_results=pics_${basename_model::-4}.csv

    #If result file does not exist, run the code
    if [ ! -f $fold_dir/$pics_results ];then
   singularity exec -H $HOME:/home1 -B /hercules/:/hercules/ /hercules/scratch/vishnu/singularity_images/trapum_pulsarx_fold_docker_20220411.sif python pics_classifier.py -i $fold_dir/ -m /hercules/scratch/vishnu/SLURM_PULSARMINER/MeerKAT_L_SBAND_COMBINED_Best_Recall.pkl -o $results_dir_high_score
    fi
done
