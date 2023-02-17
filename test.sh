#!/bin/bash

fold_timeseries=0


if [ $fold_timeseries == "1" ]; then
  echo rsync -Pav $search_results $working_dir
else
  echo rsync -Pav --exclude='*.dat' $search_results $working_dir

fi


