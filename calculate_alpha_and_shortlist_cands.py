from prepfold import pfd
import argparse
import glob
import os, sys
import bestprof_utils
import pandas as pd
import subprocess
import re

def get_args():
    arg_parser = argparse.ArgumentParser(
        description="A utility tool to calculate alpha values and shortlist candidates for Candyjar")
    arg_parser.add_argument("-pfds", dest="pfds_dir",
                            help="PFD file directory", required=True)
    arg_parser.add_argument("-t", dest="aggressive_threshold",default=0.5,help="Threshold for alpha parameter. Default: 0.5", type=float)
    arg_parser.add_argument("-T", dest="conservative_threshold",default=1.0,help="Threshold for alpha parameter. Default: 1.0", type=float)
    arg_parser.add_argument("-s", dest="script_file",default=None,help="Script file for raw data folding that needs to be shortlisted")
    arg_parser.add_argument("-l", dest="shortlist", action='store_true', help="Activate shortlisting of candidates for Candyjar. This assumes raw candidates are already folded")





                                                     
    return arg_parser.parse_args()



if __name__ == '__main__':
    args = get_args()
    candidate_dir = args.pfds_dir
    pfd_files = glob.glob(candidate_dir + '/' + '*.pfd')
    zerodm_files = [filename for filename in pfd_files if 'zerodm' in filename]
    aggressive_threshold = args.aggressive_threshold
    conservative_threshold = args.conservative_threshold
    current_directory = os.getcwd()
    raw_script_file = args.script_file
    alpha_results = []
    results_dir = os.path.dirname(candidate_dir)
    
    for pfd_file in zerodm_files:
        if os.path.isfile(pfd_file + ".bestprof"):
            bestprof = bestprof_utils.parse_bestprof(pfd_file + ".bestprof")
        
        zero_dm_sigma = float(bestprof['Sigma'])
        candidate_dm_file = pfd_file.replace('_zerodm', '')
        if os.path.isfile(candidate_dm_file + ".bestprof"):
            bestprof = bestprof_utils.parse_bestprof(candidate_dm_file + ".bestprof")
            candidate_dm_sigma = float(bestprof['Sigma'])
        else:
            candidate_dm_sigma = 1e-10 # set to a very small value to avoid division by zero
        
        alpha = zero_dm_sigma/candidate_dm_sigma
        alpha_results.append([os.path.basename(candidate_dm_file), zero_dm_sigma, candidate_dm_sigma, alpha])

    alpha_df = pd.DataFrame(alpha_results, columns=['candidate_dm_file', 'ts_zero_dm_fold_sigma', 'ts_candidate_dm_fold_sigma', 'alpha'])
    #print(alpha_df['alpha'])
    #print(alpha_df['alpha'].min(), alpha_df['alpha'].max())
    alpha_df.to_csv(results_dir + '/' + 'alpha_results.csv', index=False)
    #alpha_df = pd.read_csv(results_dir + '/' + 'alpha_results.csv')
   
   
    alpha_df_agg = alpha_df.loc[alpha_df['alpha'] < aggressive_threshold]
    alpha_df_con = alpha_df.loc[alpha_df['alpha'] < conservative_threshold]
    
    print('%d candidates shortlisted for aggressive threshold: %.1f' % (len(alpha_df_agg), aggressive_threshold))
    print('%d candidates shortlisted for conservative threshold: %.1f' % (len(alpha_df_con), conservative_threshold))

    # Preprocess lines into dictionaries
    accelcand_dict = {}
    filename_dict = {}

    with open(raw_script_file, "r") as infile:
        lines = infile.readlines()

    for line in lines:
        accel_match = re.search(r'-accelcand (\d+)', line)
        filename_match = re.search(r'-o (\S+)', line)
        
        if accel_match:
            accelcand_number = int(accel_match.group(1))
            accelcand_dict.setdefault(accelcand_number, []).append(line)
        
        if filename_match:
            filename = filename_match.group(1)
            filename_dict.setdefault(filename, []).append(line)
    
    '''
    Shortlisting flag is used if raw candidates are already folded and 
    you just need to find candidates inside candidates.csv for candyjar that have a good alpha value '''
    
    if args.shortlist:
        print('Assuming Candyjar files already exist in your pfd candidate directory')
        raw_folding_dir = candidate_dir.replace('05_FOLDING_TIMESERIES', '05_FOLDING')
        candyjar_csv = raw_folding_dir  + 'candidates.csv'
        print('Reading %s' % candyjar_csv)
        candyjar_df = pd.read_csv(candyjar_csv)
        alpha_df['png_file'] = alpha_df['candidate_dm_file'].str.replace('ts_fold', 'raw_fold') + '.png'
        #Left join to get alpha values to candyjar candidates
        candyjar_df = candyjar_df.merge(alpha_df, left_on='png_path', right_on='png_file', how='left')
        del candyjar_df['png_file']
        #Write shortlisted candidates to file
        agg_alpha_shortlist = candyjar_df.loc[candyjar_df['alpha'] < aggressive_threshold]
        con_alpha_shortlist = candyjar_df.loc[(candyjar_df['alpha'] < conservative_threshold) & (candyjar_df['alpha'] > aggressive_threshold)]
        agg_alpha_shortlist.to_csv(raw_folding_dir + '/' + 'candidates_alpha_below_%s.csv' %(str(aggressive_threshold).replace('.', '_')) , index=False)
        con_alpha_shortlist.to_csv(raw_folding_dir + '/' + 'candidates_alpha_between_%s_%s.csv' %(str(aggressive_threshold).replace('.', '_'), str(conservative_threshold).replace('.', '_')) , index=False)
        candyjar_df.to_csv(raw_folding_dir + '/' + 'candidates_alpha_all.csv', index=False)
        

    else:

        # Write to file
        with open(results_dir + '/' + "script_raw_fold_short_list.txt", "w") as outfile:

            for index, row in alpha_df_con.iterrows():
                candidate_dm_file = row['candidate_dm_file']
                candidate_dm_file_basename = os.path.basename(candidate_dm_file)
                # Extract accelcand_number and modify filename
                accelcand_number_match = re.search(r'ACCEL_Cand_(\d+)', candidate_dm_file_basename)
                accelcand_number = int(accelcand_number_match.group(1)) if accelcand_number_match else None
                raw_fold_string = re.sub(r'_ACCEL_Cand.*$', '', candidate_dm_file_basename)
                raw_fold_string = raw_fold_string.replace("ts_fold", "raw_fold")
                
                
                # Check if they exist in the dictionaries and write to file
                if accelcand_number in accelcand_dict and raw_fold_string in filename_dict:
                    for line in accelcand_dict[accelcand_number]:
                        if line in filename_dict[raw_fold_string]:
                            outfile.write(line)









            
         
