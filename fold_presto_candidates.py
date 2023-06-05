import numpy as np
import os, glob, sys, time
from pulsar_miner import SurveyConfiguration, sift_candidates, check_if_cand_is_known, Observation, fold_candidate
import argparse
from run_sift_candidates import add_search_scheme_to_pm_config, call_pulsarminer_sifting
from create_slurm_jobs import mkdir_p

parser = argparse.ArgumentParser(description='Create Presto fold file for all candidates in a beam')
parser.add_argument('-o', '--obs', help='Observation file ($Cluster_$epoch_$beam.fil)', type=str,  required=True)
parser.add_argument('-m', '--rfifind_mask', help='Presto RFIFind Mask file', type=str,  required=True)
parser.add_argument('-p', '--pulsarminer_config', help='PulsarMiner Configuration file', type=str,  default="sample_M30.config")
parser.add_argument('-w', '--working_tmp_directory', help='Scratch space root directory (Eg: /tmp) in a Cluster node where all previous results are stored.', type=str,  default='/tmp')
parser.add_argument('-v', '--verbosity_level', help='Verbosity level', type=int,  default=1)

def pulsarminer_fold(pm_config, LOG_dir, dir_dedispersion, working_dir, N_cands_all, count_candidates_to_fold_redet, count_candidates_to_fold_new, verbosity_level):
    if pm_config.flag_step_folding == 1:
            print
            print
            print "##################################################################################################"
            print "#                                       STEP 5 - FOLDING "
            print "##################################################################################################"
            print

            dir_folding = os.path.join(working_dir, "05_FOLDING")
            if verbosity_level >= 1: print "5) FOLDING: Creating working directories...",; sys.stdout.flush()
            if not os.path.exists(dir_folding):
                    os.mkdir(dir_folding)
            if verbosity_level >= 1: print "done!"

            obs = pm_config.list_Observations.file_basename
            print "Folding observation '%s'" % (obs)
            print

            work_dir_candidate_folding = os.path.join(dir_folding, pm_config.list_Observations.file_basename)
            if verbosity_level >= 1:   print "5) CANDIDATE FOLDING: creating working directory '%s'..." % (work_dir_candidate_folding),; sys.stdout.flush()
            if not os.path.exists(work_dir_candidate_folding):
                    os.mkdir(work_dir_candidate_folding)
            if verbosity_level >= 1:   print "done!"
            

            file_script_fold_name = "script_fold.txt"
            file_script_fold_abspath = "%s/%s" % (work_dir_candidate_folding, file_script_fold_name)
            file_script_fold = open(file_script_fold_abspath, "w")
            file_script_fold.close()

            if pm_config.flag_fold_known_pulsars == 1:
                    key_cands_to_fold = 'candidates'
                    if verbosity_level >= 1:
                            print
                            print "5) CANDIDATE FOLDING: I will fold all the %d candidates (%s likely redetections included)" % (N_cands_all, count_candidates_to_fold_redet)
                    N_cands_to_fold = N_cands_all
                    
            elif pm_config.flag_fold_known_pulsars == 0:
                    key_cands_to_fold = 'candidates_new'
                    if verbosity_level >= 1:
                            print
                            print "5) CANDIDATE FOLDING: I will fold only the %d putative new pulsars (%s likely redetections will not be folded)" % (count_candidates_to_fold_new, count_candidates_to_fold_redet)
                    N_cands_to_fold = count_candidates_to_fold_new
            count_folded_ts = 1 
            if pm_config.flag_fold_timeseries == 1:
                    
                    LOG_basename = "05_folding_%s_timeseries" % (obs)
                    if verbosity_level >= 1:
                            print
                            print "Folding time series..."
                            print 
                            print "\033[1m >> TIP:\033[0m Follow folding progress with '\033[1mtail -f %s/LOG_%s.txt\033[0m'" % (LOG_dir, LOG_basename)
                            print
                    for seg in sorted(pm_config.dict_search_structure[obs].keys()):
                            for ck in sorted(pm_config.dict_search_structure[obs][seg].keys()):
                                    for j in range(len(pm_config.dict_search_structure[obs][seg][ck][key_cands_to_fold])):
                                            candidate = pm_config.dict_search_structure[obs][seg][ck][key_cands_to_fold][j]
                                    
                                            print "FOLDING CANDIDATE TIMESERIES %d/%d of %s: seg %s / %s..." % (count_folded_ts, N_cands_to_fold, obs, seg, ck), ; sys.stdout.flush()
                                            tstart_folding_cand_ts = time.time()
                                            file_to_fold = os.path.join(dir_dedispersion, obs, seg, ck, candidate.filename.split("_ACCEL")[0] + ".dat" )
                                            flag_remove_dat_after_folding = 0
                                            if os.path.exists(file_to_fold):
                                                    
                                                    fold_candidate(work_dir_candidate_folding,  
                                                                                LOG_basename, 
                                                                                LOG_dir,
                                                                                pm_config.list_Observations.file_abspath,
                                                                                dir_dedispersion,
                                                                                obs,
                                                                                seg,
                                                                                ck,
                                                                                pm_config.list_Observations.T_obs_s,
                                                                                candidate,
                                                                                pm_config.ignorechan_list,
                                                                                pm_config.list_Observations.mask,
                                                                                pm_config.prepfold_flags,
                                                                                pm_config.presto_env,
                                                                                verbosity_level,
                                                                                1,
                                                                                "timeseries",
                                                                                pm_config.num_simultaneous_folds
                                                    )
                                                    tend_folding_cand_ts = time.time()
                                                    time_taken_folding_cand_ts_s = tend_folding_cand_ts - tstart_folding_cand_ts
                                                    print "done in %.2f s!" % (time_taken_folding_cand_ts_s) ; sys.stdout.flush()
                                                    count_folded_ts = count_folded_ts + 1
                                            else:
                                                    print "dat file does not exists! Likely if you set FLAG_REMOVE_DATFILES_OF_SEGMENTS = 1 in the config file. Skipping..."
                                    
            count_folded_raw = 1 
            if pm_config.flag_fold_rawdata == 1:
                    LOG_basename = "05_folding_%s_rawdata" % (obs)
                    print
                    print "Folding raw data \033[1m >> TIP:\033[0m Follow folding progress with '\033[1mtail -f %s/LOG_%s.txt\033[0m'" % (LOG_dir,LOG_basename)
                    for seg in sorted(pm_config.dict_search_structure[obs].keys(), reverse=True):
                            #print "FOLD_RAW = %s of %s" % (seg, sorted(config.dict_search_structure[obs].keys(), reverse=True))
                            
                            for ck in sorted(pm_config.dict_search_structure[obs][seg].keys()):
                                    for j in range(len(pm_config.dict_search_structure[obs][seg][ck][key_cands_to_fold])):
                                            candidate = pm_config.dict_search_structure[obs][seg][ck][key_cands_to_fold][j]
                                            LOG_basename = "05_folding_%s_%s_%s_rawdata" % (obs, seg, ck)
                                            

                                            #print "FOLDING CANDIDATE RAW %d/%d of %s: seg %s / %s ..." % (count_folded_raw, count_candidates_to_fold, obs, seg, ck), ; sys.stdout.flush()
                                            fold_candidate(work_dir_candidate_folding,  
                                                                                LOG_basename, 
                                                                                LOG_dir,
                                                                                pm_config.list_Observations.file_abspath,
                                                                                dir_dedispersion,
                                                                                obs,
                                                                                seg,
                                                                                ck,
                                                                                pm_config.list_Observations.T_obs_s,
                                                                                candidate,
                                                                                pm_config.ignorechan_list,
                                                                                pm_config.list_Observations.mask,
                                                                                pm_config.prepfold_flags + " -nsub %d" % (pm_config.list_Observations.nchan),
                                                                                pm_config.presto_env,
                                                                                verbosity_level,
                                                                                1,
                                                                                "rawdata",
                                                                                pm_config.num_simultaneous_folds
                                            )
                                            
                                            #print "done!"

                                            count_folded_raw = count_folded_raw + 1


            os.chdir(work_dir_candidate_folding)
            # cmd_pm_run_multithread = "%s/pm_run_multithread -cmdfile %s -ncpus %d" % (working_dir, file_script_fold_abspath, pm_config.num_simultaneous_folds)
            # print
            # print 
            # print "5) CANDIDATE FOLDING - Now running:"
            # print "%s" % cmd_pm_run_multithread
            # os.system(cmd_pm_run_multithread)


def main():
    args = parser.parse_args()
    # Read config file and input data
    
    pulsarminer_config_file = args.pulsarminer_config
    observation_name = args.obs
    verbosity_level = args.verbosity_level
    working_dir = args.working_tmp_directory
    mask_file = args.rfifind_mask
    #output_dir = args.results_dir
    #
    observation_name_no_extension = os.path.basename(os.path.splitext(observation_name)[0])
    
    # Get the cluster, epoch and beam from the observation name
    cluster = observation_name_no_extension.split('_')[0]
    epoch = observation_name_no_extension.split('_')[1]
    beam = observation_name_no_extension.split('_')[2]
    working_dir = os.path.join(working_dir, cluster, epoch, beam)
    mkdir_p(working_dir)


   
    

    verbosity_level = 1
    pm_config = SurveyConfiguration(pulsarminer_config_file, verbosity_level)
    work_dir = pm_config.root_workdir
    dir_dedispersion = os.path.join(working_dir, cluster, epoch, beam, "03_DEDISPERSION")
    LOG_dir = os.path.join(pm_config.root_workdir, "LOG")
    
    if not os.path.exists(LOG_dir): 
         os.mkdir(LOG_dir)

    #Input is only a single observation...
    if observation_name!="":
            pm_config.list_datafiles             = os.path.basename(observation_name)
            pm_config.folder_datafiles           = os.path.dirname(os.path.abspath(observation_name))   


    # Single observation datapath
    pm_config.list_datafiles_abspath     = os.path.join(pm_config.folder_datafiles, pm_config.list_datafiles)

    # Single observation object
    pm_config.list_Observations          = Observation(pm_config.list_datafiles_abspath, pm_config.data_type)

    # Add the common birdies file
    pm_config.file_common_birdies = os.path.join(pm_config.root_workdir, "common_birdies.txt")
    pm_config.list_Observations.mask = mask_file
    pm_config = add_search_scheme_to_pm_config(pm_config, verbosity_level)
    sifting_output_dir = working_dir
    print('Starting Folding')
    N_cands_all, count_candidates_to_fold_redet, count_candidates_to_fold_new = call_pulsarminer_sifting(pm_config, dir_dedispersion, sifting_output_dir, LOG_dir, verbosity_level)
    print(N_cands_all, count_candidates_to_fold_redet, count_candidates_to_fold_new)
    pulsarminer_fold(pm_config, LOG_dir, dir_dedispersion, working_dir, N_cands_all, count_candidates_to_fold_redet, count_candidates_to_fold_new, verbosity_level)

main()
#def sift_candidates(work_dir, LOG_basename,  dedispersion_dir, observation_basename, segment_label, chunk_label, list_zmax, jerksearch_zmax, jerksearch_wmax, flag_remove_duplicates, flag_DM_problems, flag_remove_harmonics, minimum_numDMs_where_detected, minimum_acceptable_DM=2.0, period_to_search_min_s=0.001, period_to_search_max_s=15.0, verbosity_level=0 ):


