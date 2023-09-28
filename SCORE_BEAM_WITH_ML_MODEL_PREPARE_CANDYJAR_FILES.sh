#!/bin/bash

function usage {
    echo "Usage: $0 [OPTIONS]"
    echo "  -e EPOCH          Set epoch (default: 20201105)"
    echo "  -c CLUSTER        Set cluster (default: NGC6397)"
    echo "  -r RESULTS_DIR    Set results directory (default: off)"
    echo "  -b BEAM           Specify a single directory to process (default: all directories)"
    echo "  -h                Display this help message"
    exit 1
}


epoch="20201105"
code_dir="/hercules/scratch/vishnu/SLURM_PULSARMINER"
ml_model_dir="$code_dir/ML_MODELS/"
cluster="NGC6397"
results_dir_high_score=""
beam=""

while getopts ":e:c:r:b:h" opt; do
  case $opt in
    e) epoch="$OPTARG";;
    c) cluster="$OPTARG";;
    r) results_dir_high_score="$OPTARG";;
    b) beam="$OPTARG";;
    h) usage;;
    \?) echo "Invalid option -$OPTARG" >&2; usage;;
    :) echo "Option -$OPTARG requires an argument." >&2; usage;;
  esac
done

if [ -z "$results_dir_high_score" ]; then
    results_dir_high_score="/hercules/scratch/vishnu/SLURM_PULSARMINER/ML_SELECTED/$cluster/$epoch/"
fi

mkdir -p $results_dir_high_score

if [ -z "$beam" ]; then
    directories="$code_dir/$cluster/$epoch/"
else
    directories="$code_dir/$cluster/$epoch/$beam"
fi

for dir in $directories*; do
    if [ -d "$dir" ] && [[ $(basename $dir) == cf* || $(basename $dir) == if* ]]; then
        basename_dir=$(basename $dir)
        fold_dir=$dir/05_FOLDING/${cluster}_${epoch}_${basename_dir}

        for model in $ml_model_dir*.pkl; do
            #absolute_model_path=$(realpath $model)  # Getting the absolute path

            #basename_model=$(basename $model)
            pics_results="pics_scores.csv"

            # If result file does not exist, run the code
            if [ ! -f $fold_dir/$pics_results ]; then
                echo singularity exec -H $HOME:/home1 -B /hercules/:/hercules/ /hercules/scratch/vishnu/singularity_images/trapum_pulsarx_fold_docker_20220411.sif python $code_dir/pics_classifier_multiple_models.py -i $fold_dir/ -m $ml_model_dir
            fi
        done
    fi
done
