#!/bin/bash

beam_list=("cfbf00224" "cfbf00182" "cfbf00274" "cfbf00211" "cfbf00196" "cfbf00278" "cfbf00159" "cfbf00234" "cfbf00245" "cfbf00039" "cfbf00190" "cfbf00194" "cfbf00262" "cfbf00186" "cfbf00158")

for beam in "${beam_list[@]}"
do
  python relaunch_checker.py slurm_jobs_NGC441_20200709_${beam}.sh 
  source relaunch_slurm_jobs_NGC441_20200709_${beam}.sh
done
