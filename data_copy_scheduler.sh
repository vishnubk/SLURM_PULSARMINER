#!/bin/bash

USER="vishnu"
HOST="archive"
#DATA_DIR="/p/MFR/MEERKAT/TRAPUM/TRAPUM_517/TRAPUM/SCI-20180923-MK-04/J1740-5340A/2020-10-20-15:40:16/1284/"
#DATA_DIR="/p/MFR/MEERKAT/TRAPUM/TRAPUM_517/TRAPUM/SCI-20180923-MK-04/J1740-5340A/2020-11-05-10:13:37/1284/"
DATA_DIR="/p/MFR/MEERKAT/TRAPUM/TRAPUM_518/TRAPUM/SCI-20180923-MK-04/J1740-5340A/2020-11-05-10:13:37/1284/"
OUTPUT_DIR="/hercules/scratch/vishnu/TRAPUM/observations/NGC6397/20201105/"
filtool_img="/u/vishnu/singularity_images/pulsarx_latest.sif"
filtool_cmd="filtool -t 12 --telescope meerkat -z zdot --cont"
cluster="NGC6397"
epoch="20201105"
code_dir="/hercules/scratch/vishnu/SLURM_PULSARMINER/"
#pm_config_file="NGC6397_20201020_accel_search.config"
pm_config_file="NGC6397_20201105_accel_search_only.config"
PROCESSED_FILE="${code_dir}processed_dirs_NGC6397_20201105.txt"


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
 
    # Check if the file size on the remote server is less than 100MB
    #REMOTE_SIZE=$(ssh ${USER}@${HOST} du -sB1 "${SUBDIR}" | cut -f1)

    #if [ ${REMOTE_SIZE} -lt 104857600 ]
    #then
    #    echo "${SUBDIR} is less than 100MB. Skipping."
    #    continue
    #fi

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
    # Run filtool
    singularity exec -H $HOME:/home1 -B /hercules/:/hercules/ $filtool_img $filtool_cmd -o ${cluster}_${epoch}_${beam}_zdot -f ${OUTPUT_DIR}${beam}/*.fil

    # Check if filtool was successful
    if [ $? -ne 0 ]
    then
        echo "Filtool failed for ${cluster}_${epoch}_${beam}. Moving to the next subdirectory."
        continue
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

    # Check if pulsarminer start was successful
    if [ $? -ne 0 ]
    then
        echo "Pulsarminer start failed for ${cluster}_${epoch}_${beam}. Moving to the next subdirectory."
        continue
    fi

    #echo "Started Pulsarminer for ${cluster}_${epoch}_${beam}_zdot_01.fil. Sleeping for 1 hours..."
    echo "Started Pulsarminer for ${cluster}_${epoch}_${beam}_zdot_01.fil. Sleeping for 40 minutes..."
    # Mark this directory as processed
    echo ${SUBDIR} >> ${PROCESSED_FILE}

    # Delete pre-filtoled files
    cd ${OUTPUT_DIR}${beam}
    for file in *.fil; do
        if [ "$file" != ${cluster}_${epoch}_${beam}_zdot_01.fil ]; then
            rm -f $file
        fi
    done

    # Sleep for 2 hours 
    sleep 40m

done

