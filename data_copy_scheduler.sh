#!/bin/bash

USER="vishnu"
HOST="archive"
#DATA_DIR="/p/MFR/MEERKAT/TRAPUM/TRAPUM_517/TRAPUM/SCI-20180923-MK-04/J1740-5340A/2020-10-20-15:40:16/1284/"
DATA_DIR="/p/MFR/MEERKAT/TRAPUM/TRAPUM_517/TRAPUM/SCI-20180923-MK-04/J1740-5340A/2020-11-05-10:13:37/1284/"
#DATA_DIR="/p/MFR/MEERKAT/TRAPUM/TRAPUM_518/TRAPUM/SCI-20180923-MK-04/J1740-5340A/2020-11-05-10:13:37/1284/"
#DATA_DIR="/p/MFR/MEERKAT/TRAPUM/TRAPUM_511/TRAPUM/SCI-20180923-MK-04/NGC_6441/2020-07-09-17:33:18/1284/"
OUTPUT_DIR="/hercules/scratch/vishnu/TRAPUM/observations/NGC6397/20201105/"
#OUTPUT_DIR="/hercules/scratch/vishnu/TRAPUM/observations/NGC6441/20200709/"
filtool_img="/u/vishnu/singularity_images/pulsarx_latest.sif"
filtool_cmd="filtool -t 12 --telescope meerkat -z zdot --cont"
cluster="NGC6397"
epoch="20201105"
code_dir="/hercules/scratch/vishnu/SLURM_PULSARMINER/"
#pm_config_file="NGC6397_20201020_accel_search.config"
#pm_config_file="NGC6397_20201105_accel_search_only.config"
pm_config_file="NGC6397_test.config"
PROCESSED_FILE="${code_dir}processed_dirs_NGC6397_20201105.txt"

mkdir -p $OUTPUT_DIR

# Create the processed file if it doesn't exist
if [ ! -f ${PROCESSED_FILE} ]; then
    touch ${PROCESSED_FILE}
fi

# Get all subdirectories
SUBDIRS=$(ssh ${USER}@${HOST} "find ${DATA_DIR} -mindepth 1 -maxdepth 1 -type d -print")

# Iterate over each subdirectory
for SUBDIR in ${SUBDIRS}
do  
    # Check if this subdirectory has been processed
    if grep -Fxq "${SUBDIR}" ${PROCESSED_FILE}
    then
    echo "${SUBDIR} has already been processed. Skipping."
    continue
    fi


    echo "Starting rsync for ${SUBDIR}"

    # Run rsync
    rsync -Pav ${USER}@${HOST}:${SUBDIR} ${OUTPUT_DIR}

    # Check if rsync was successful
    if [ $? -ne 0 ]
    then
        echo "rsync for ${SUBDIR} failed. Moving to the next subdirectory."
        continue
    fi
    
    beam=$(basename ${SUBDIR})
    cd ${OUTPUT_DIR}${beam}

    # Count number of .fil files. If there are more than one, sort them and get the last one. Fixing a TRAPUM sub-banding pipeline bug.
    num_files=$(ls *.fil 2>/dev/null | wc -l)

    # Check condition and execute accordingly
    if [ "$num_files" -gt 1 ]; then
        # Sort files and get the last one
        last_file=$(ls -v *.fil | tail -n 1)
        singularity exec -H $HOME:/home1 -B /hercules/:/hercules/ $filtool_img $filtool_cmd -o ${cluster}_${epoch}_${beam}_zdot -f ${OUTPUT_DIR}${beam}/$last_file
        if [ $? -ne 0 ]; then
            echo "Filtool failed for ${cluster}_${epoch}_${beam}. Moving to the next subdirectory."
            continue
        fi
    elif [ "$num_files" -eq 1 ]; then
        single_file=$(ls *.fil)
        singularity exec -H $HOME:/home1 -B /hercules/:/hercules/ $filtool_img $filtool_cmd -o ${cluster}_${epoch}_${beam}_zdot -f ${OUTPUT_DIR}${beam}/$single_file
        if [ $? -ne 0 ]; then
            echo "Filtool failed for ${cluster}_${epoch}_${beam}. Moving to the next subdirectory."
            continue
        fi
    fi

    # Create a symlink to the fil file
    ln -s ${OUTPUT_DIR}${beam}/${cluster}_${epoch}_${beam}_zdot_01.fil $code_dir${cluster}_${epoch}_${beam}.fil

    #Check if the symlink was successful
    if [ $? -ne 0 ]
    then
        echo "Symlink creation failed for ${cluster}_${epoch}_${beam}. Moving to the next subdirectory."
        continue
    fi

    # Start SLURM Pulsarminer inside a screen session on the filtoled file
    screen -S SLURM_PULSARMINER_${cluster}_${epoch}_${beam} -dm bash  -c "cd ${code_dir} && ./LAUNCH_SLURM_PULSARMINER.sh -o ${cluster}_${epoch}_${beam}.fil -p $pm_config_file"

    ## Check if pulsarminer start was successful
    if [ $? -ne 0 ]
    then
       echo "Pulsarminer start failed for ${cluster}_${epoch}_${beam}. Moving to the next subdirectory."
       continue
    fi

    echo "Started Pulsarminer for ${cluster}_${epoch}_${beam}_zdot_01.fil. Sleeping for 2 hours..."
    #echo "Started Pulsarminer for ${cluster}_${epoch}_${beam}_zdot_01.fil. Sleeping for 40 minutes..."
    # Mark this directory as processed
    echo ${SUBDIR} >> ${PROCESSED_FILE}

    # Delete pre-filtoled files
    cd ${OUTPUT_DIR}${beam}
    for file in *.fil; do
       if [ "$file" != ${cluster}_${epoch}_${beam}_zdot_01.fil ]; then
           rm -f $file
       fi
    done
    # Sleep for 4 hours 
    #sleep 4h

done

