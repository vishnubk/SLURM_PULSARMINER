import os
import sys
import pandas as pd

cluster = sys.argv[1]
epoch = sys.argv[2]
code_dir = sys.argv[3]

# Initialize an empty DataFrame to hold all candidates
master_df = pd.DataFrame()

# Use os.walk for efficient directory traversal
for root, dirs, files in os.walk(os.path.join(code_dir, cluster, epoch)):

    valid_dirs = [d for d in dirs if d.startswith(('cfbf', 'ifbf'))]
    

    for beam in valid_dirs:
        candidate_file_path = os.path.join(root, beam, '05_FOLDING', '%s_%s_%s' %(cluster, epoch, beam), 'candidates.csv')
       
        # Check if 'candidates.csv' exists in this directory
        if os.path.exists(candidate_file_path):
            # Read the CSV file into a DataFrame
            df = pd.read_csv(candidate_file_path)
            beam_str = str(df['beam_name'].iloc[0])
            df['png_path'] = beam_str + '/' + df['png_path'].astype(str)
            # Append it to the master DataFrame
            master_df = pd.concat([master_df, df], ignore_index=True)

# Save the master DataFrame to a CSV file
if not master_df.empty:
    master_df.to_csv(os.path.join(code_dir, epoch, 'candidates.csv'), index=False)

#Select high ML candidates
highML_df = master_df.loc[(master_df['pics_meerkat_l_sband_combined_best_recall'] > 0.1) | (master_df['pics_palfa_meerkat_l_sband_best_fscore'] > 0.1)]

highML_df.to_csv(os.path.join(code_dir, cluster, epoch, 'candidates_ml_selected.csv'), index=False)
