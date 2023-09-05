
slurmids="$slurmids:$search"
check_job_submission_limit
mkdir -p SEARCH_PROGRESS
progress_file="SEARCH_PROGRESS/search_progress_NGC441_20200709_cfbf00194.txt"
###################################### Loop to check accel-search progress ##################################################################
  # Count the number of files matching *_ACCEL_0.txtcand in the directory
while true; do
  found_files=$(find NGC441/20200709/cfbf00194/03_DEDISPERSION -name '*_ACCEL_0.txtcand' | wc -l)
  percentage=$(( 100 * $found_files / 8992 ))
  bar=""
  for i in $(seq 1 $percentage); do
    bar="${bar}#"
  done
  for i in $(seq 1 $((100 - $percentage))); do
    bar="${bar} "
  done
  echo -ne "Date: $(date), Search Progress for zmax 0: [${bar}] $percentage% \r"
  echo "Date: $(date), Search Progress for zmax 0: [${bar}] $percentage%" >> $progress_file
  if [ $found_files -lt 8992 ]; then
    sleep 30m
  else
    echo -e "\nSearch Category zmax: 0 completed." 
    echo "Search Category zmax: 0 completed. Date: $(date)" >> $progress_file
    break
  fi
done
  # Count the number of files matching *_ACCEL_200.txtcand in the directory
while true; do
  found_files=$(find NGC441/20200709/cfbf00194/03_DEDISPERSION -name '*_ACCEL_200.txtcand' | wc -l)
  percentage=$(( 100 * $found_files / 8992 ))
  bar=""
  for i in $(seq 1 $percentage); do
    bar="${bar}#"
  done
  for i in $(seq 1 $((100 - $percentage))); do
    bar="${bar} "
  done
  echo -ne "Date: $(date), Search Progress for zmax 200: [${bar}] $percentage% \r"
  echo "Date: $(date), Search Progress for zmax 200: [${bar}] $percentage%" >> $progress_file
  if [ $found_files -lt 8992 ]; then
    sleep 30m
  else
    echo -e "\nSearch Category zmax: 200 completed." 
    echo "Search Category zmax: 200 completed. Date: $(date)" >> $progress_file
    break
  fi
done
###################################### Loop to check jerk-search progress ##################################################################
  # Count the number of files matching *ACCEL_200_JERK_600.txtcand in the directory
while true; do
  found_files=$(find NGC441/20200709/cfbf00194/03_DEDISPERSION -name '*ACCEL_200_JERK_600.txtcand' | wc -l)
  percentage=$(echo "scale=2; ($found_files / 8992) * 100" | bc)
  num_hashes=$(echo "($found_files * 50)/8992" | bc)
  bar=$(printf "%-${num_hashes}s" "#")
  bar=${bar// /#}
  # If the number of found files is not what is expected, sleep and check again
  if [ $found_files -lt 8992 ]; then
    echo -ne "Date: $(date), Jerk Search Progress for wmax 600: [${bar}] $percentage% \r" >> $progress_file
    echo "Date: $(date), Jerk Search Progress for wmax 600: [${bar}] $percentage%" >> $progress_file
    echo -ne "Date: $(date), Jerk Search Progress for wmax 600: [${bar}] $percentage% \r"
    sleep 30m
  else
    echo "Jerk Search for wmax 600 completed."
    echo "Jerk Search for wmax 600 completed." >> $progress_file
    break
  fi
done

