#!/bin/bash
# Change to script's directory
cd "$(dirname "$0")"

# Maximum number of parallel jobs
max_jobs=48

# Check for each pfd file
for i in *.pfd; do
    # Check if the corresponding png file already exists
    if [ ! -f "${i%.pfd}.png" ]; then
        # Run the command in background
        show_pfd -noxwin -fixchi $i &
        echo "Running job for $i in background..."
    fi

    # Control the number of background jobs to not exceed $max_jobs
    while (( $(jobs | wc -l) >= max_jobs )); do
        sleep 1 # Wait until a job has finished before starting a new one
    done
done

# Wait for all jobs to complete
wait

echo "All jobs completed."

