#!/bin/bash

# Directory containing the files
directory="/hercules/scratch/vishnu/SLURM_PULSARMINER/VIVEK_LOGS/"

# Loop through each file in the directory
for file in "$directory"/*; do
    # Use grep to filter out the Perl warnings and redirect the output to a temporary file
    grep -v -e "perl: warning: Falling back to the standard locale" \
         -e "perl: warning: Setting locale failed." \
         -e "perl: warning: Please check that your locale settings:" \
         -e "LANGUAGE = (unset)," \
         -e "LC_ALL = (unset)," \
         -e "LC_CTYPE = \"UTF-8\"," \
         -e "LANG = \"en_US.UTF-8\"" "$file" > temp_filtered.txt

    # Check if the temporary file is empty
    if [[ -s temp_filtered.txt ]]; then
        echo "File $file contains text other than Perl warnings."
    else
        echo "File $file contains only Perl warnings."
    fi
done

# Remove the temporary file
rm temp_filtered.txt

