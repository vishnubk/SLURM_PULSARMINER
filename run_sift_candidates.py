import numpy as np
import os, glob, sys
from pulsar_miner import SurveyConfiguration, sift_candidates, check_if_cand_is_known, Observation
import argparse

parser = argparse.ArgumentParser(description='Run Presto Sifting algorithm on your detections')
parser.add_argument('-p', '--pulsarminer_config', help='PulsarMiner Configuration file', type=str,  default="sample_M30.config")
parser.add_argument('-o', '--obs', help='Observation file ($Cluster_$epoch_$beam.fil)', type=str,  required=True)
parser.add_argument('-r', '--results_root_dir', help='Directory where your results are stored', type=str,  default=os.getcwd())

parser.add_argument('-v', '--verbosity_level', help='Verbosity level', type=int,  default=1)

def add_search_scheme_to_pm_config(pm_config, verbosity_level):
    '''
    Adding directory search scheme to pm_config. Used later for sifting.
    '''
    if verbosity_level >= 1:
            print
            print
            print "*******************************************************************"
            print "SEARCH SCHEME:"
            print "*******************************************************************"

    if verbosity_level >= 1:
            print
            print "%s: \033[1m %s \033[0m  (%.2f s)" % ("Observation", pm_config.list_Observations.file_nameonly, pm_config.list_Observations.T_obs_s)
            print
                    
    pm_config.dict_search_structure[pm_config.list_Observations.file_basename] = {}
    for s in pm_config.list_segments:
            if verbosity_level >= 2:
                    print "Segment = %s of %s" % (s, pm_config.list_segments)
            if s == "full":
                    segment_length_s      = pm_config.list_Observations.T_obs_s
                    segment_length_min    = pm_config.list_Observations.T_obs_s /60.
                    segment_label         = s
            else:
                    segment_length_min  = np.float(s)
                    segment_length_s    = np.float(s) * 60
                    segment_label = "%dm" % (segment_length_min)
            pm_config.dict_search_structure[pm_config.list_Observations.file_basename][segment_label] = {}
            N_chunks = int(pm_config.list_Observations.T_obs_s / segment_length_s)
            

            for ck in range(N_chunks):
                    chunk_label = "ck%02d" % (ck)
                    pm_config.dict_search_structure[pm_config.list_Observations.file_basename][segment_label][chunk_label] = {'candidates': [] }
            print "    Segment: %8s     ---> %2d chunks (%s)" % (segment_label, N_chunks, ", ".join(sorted(pm_config.dict_search_structure[pm_config.list_Observations.file_basename][segment_label].keys())))
    if verbosity_level >= 1:
            print
            print "*******************************************************************"
            print
            print
            print
            
    if verbosity_level >= 2:                  
            print "config.dict_search_structure:"
            print pm_config.dict_search_structure

    return pm_config


def call_pulsarminer_sifting(pm_config, dir_dedispersion, output_dir, LOG_dir, verbosity_level, list_known_pulsars=[]):
    if pm_config.flag_step_sifting == 1:
        print
        print "##################################################################################################"
        print "#                                  STEP 4 - CANDIDATE SIFTING "
        print "##################################################################################################"


        dir_sifting = os.path.join(output_dir, "04_SIFTING")
        if verbosity_level >= 1:                    print "4) CANDIDATE SIFTING: Creating working directories...",; sys.stdout.flush()
        if not os.path.exists(dir_sifting):
                os.mkdir(dir_sifting)
        if verbosity_level >= 1:                    print "done!"


        dict_candidate_lists = {}
       
        obs = pm_config.list_Observations.file_basename
        

        if verbosity_level >= 2:     print "Sifting candidates for observation %3d/%d '%s'." % (1, 1, obs) 
        for seg in sorted(pm_config.dict_search_structure[obs].keys()):
                work_dir_segment = os.path.join(dir_sifting, pm_config.list_Observations.file_basename, "%s" % seg)
                if not os.path.exists(work_dir_segment):
                        os.makedirs(work_dir_segment)

                for ck in sorted(pm_config.dict_search_structure[obs][seg].keys()):
                        work_dir_chunk = os.path.join(work_dir_segment, ck)
                        if not os.path.exists(work_dir_chunk):
                                os.makedirs(work_dir_chunk)
                                
                        LOG_basename = "04_sifting_%s_%s_%s" % (obs, seg, ck)
                        work_dir_candidate_sifting = os.path.join(dir_sifting, obs, seg, ck)

                        if verbosity_level >= 1:        print "4) CANDIDATE SIFTING: Creating working directory '%s'..." % (work_dir_candidate_sifting),; sys.stdout.flush()
                        if not os.path.exists(work_dir_candidate_sifting):
                                os.mkdir(work_dir_candidate_sifting)
                        if verbosity_level >= 1:        print "done!"


                        if verbosity_level >= 1:
                                print "4) CANDIDATE SIFTING: Sifting observation %d) \"%s\" / %s / %s..." % (1, obs, seg, ck), ; sys.stdout.flush()



                        pm_config.dict_search_structure[obs][seg][ck]['candidates'] = sift_candidates( work_dir_chunk,
                        LOG_basename,
                        LOG_dir,
                        dir_dedispersion,
                        obs,
                        seg,
                        ck,
                        pm_config.accelsearch_list_zmax,
                        pm_config.jerksearch_zmax,
                        pm_config.jerksearch_wmax,
                        pm_config.sifting_flag_remove_duplicates,
                        pm_config.sifting_flag_remove_dm_problems,
                        pm_config.sifting_flag_remove_harmonics,
                        pm_config.sifting_minimum_num_DMs,
                        pm_config.sifting_minimum_DM,
                        pm_config.period_to_search_min,
                        pm_config.period_to_search_max,
                        verbosity_level=verbosity_level
                        ) 




        
        candidates_summary_filename = "%s/%s_cands.summary" % (dir_sifting, pm_config.list_Observations.file_basename)
        candidates_summary_file = open(candidates_summary_filename, 'w')

        count_candidates_to_fold_all = 0
        candidates_summary_file.write("\n*****************************************************************")
        candidates_summary_file.write("\nCandidates found in %s:\n\n" % (pm_config.list_Observations.file_nameonly))
        for seg in sorted(pm_config.dict_search_structure[obs].keys()):
                for ck in sorted(pm_config.dict_search_structure[obs][seg].keys()):
                        Ncands_seg_ck = len(pm_config.dict_search_structure[obs][seg][ck]['candidates'])
                        candidates_summary_file.write("%20s  |  %10s  ---> %4d candidates\n" % (seg, ck, Ncands_seg_ck))
                        count_candidates_to_fold_all = count_candidates_to_fold_all + Ncands_seg_ck
        candidates_summary_file.write("\nTOT = %d candidates\n" % (count_candidates_to_fold_all))
        candidates_summary_file.write("*****************************************************************\n\n")


        count_candidates_to_fold_redet = 0
        count_candidates_to_fold_new = 0
        list_all_cands = []
        for seg in sorted(pm_config.dict_search_structure[obs].keys()):
                for ck in sorted(pm_config.dict_search_structure[obs][seg].keys()):
                        pm_config.dict_search_structure[obs][seg][ck]['candidates_redetections'] = []
                        pm_config.dict_search_structure[obs][seg][ck]['candidates_new'] = []
        
                        for j in range(len(pm_config.dict_search_structure[obs][seg][ck]['candidates'])):
                                candidate = pm_config.dict_search_structure[obs][seg][ck]['candidates'][j]

                                flag_is_know, known_psrname, str_harmonic = check_if_cand_is_known(candidate, list_known_pulsars, numharm=16)

                                
                                if flag_is_know == True:
                                        pm_config.dict_search_structure[obs][seg][ck]['candidates_redetections'].append(candidate)
                                        count_candidates_to_fold_redet = count_candidates_to_fold_redet +1
                                elif flag_is_know == False:
                                        pm_config.dict_search_structure[obs][seg][ck]['candidates_new'].append(candidate)
                                        count_candidates_to_fold_new = count_candidates_to_fold_new + 1
                                        
                                dict_cand = {'cand': candidate, 'obs': obs, 'seg': seg, 'ck': ck, 'is_known': flag_is_know, 'known_psrname': known_psrname, 'str_harmonic': str_harmonic }
                                list_all_cands.append(dict_cand)
        N_cands_all = len(list_all_cands)
        
        for i_cand, dict_cand in zip(range(0, N_cands_all), sorted(list_all_cands, key=lambda k: k['cand'].p, reverse=False)):
                if dict_cand['cand'].DM < 2:
                        candidates_summary_file.write("Cand %4d/%d: %12.6f ms    |  DM: %7.2f pc cm-3    (%4s / %4s | sigma: %5.2f)  ---> Likely RFI\n" % (i_cand+1, N_cands_all, dict_cand['cand'].p * 1000., dict_cand['cand'].DM, dict_cand['seg'], dict_cand['ck'], dict_cand['cand'].sigma ))
                else:
                        if dict_cand['is_known'] == True:
                                candidates_summary_file.write("Cand %4d/%d:  %12.6f ms  |  DM: %7.2f pc cm-3    (%4s / %4s | sigma: %5.2f)  ---> Likely %s - %s\n" % (i_cand+1, N_cands_all, dict_cand['cand'].p * 1000., dict_cand['cand'].DM, dict_cand['seg'], dict_cand['ck'], dict_cand['cand'].sigma, dict_cand['known_psrname'], dict_cand['str_harmonic']))
                        elif dict_cand['is_known'] == False:
                                candidates_summary_file.write("Cand %4d/%d:  %12.6f ms  |  DM: %7.2f pc cm-3    (%4s / %4s | sigma: %5.2f)\n" % (i_cand+1, N_cands_all, dict_cand['cand'].p * 1000., dict_cand['cand'].DM, dict_cand['seg'], dict_cand['ck'], dict_cand['cand'].sigma))

        
        candidates_summary_file.close()
            
        if verbosity_level >= 1:
                candidates_summary_file = open(candidates_summary_filename, 'r')
                for line in candidates_summary_file:
                        print line,
                candidates_summary_file.close()


def main():
    args = parser.parse_args()
    # Read config file and input data
    
    pulsarminer_config_file = args.pulsarminer_config
    observation_name = args.obs
    verbosity_level = args.verbosity_level
    results_root_dir = args.results_root_dir
    #output_dir = args.results_dir
    #mkdir_p(output_dir)
    observation_name_no_extension = os.path.basename(os.path.splitext(observation_name)[0])
    
    # Get the cluster, epoch and beam from the observation name
    cluster = observation_name_no_extension.split('_')[0]
    epoch = observation_name_no_extension.split('_')[1]
    beam = observation_name_no_extension.split('_')[2]

   
    

    verbosity_level = 1
    pm_config = SurveyConfiguration(pulsarminer_config_file, verbosity_level)
    work_dir = pm_config.root_workdir
    dir_dedispersion = os.path.join(results_root_dir, cluster, epoch, beam, "03_DEDISPERSION")
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
    pm_config = add_search_scheme_to_pm_config(pm_config, verbosity_level)
    sifting_output_dir = os.path.join(results_root_dir, cluster, epoch, beam)
    print('Starting Pulsar Miner Sifting')
    call_pulsarminer_sifting(pm_config, dir_dedispersion, sifting_output_dir, LOG_dir, verbosity_level)


main()
#def sift_candidates(work_dir, LOG_basename,  dedispersion_dir, observation_basename, segment_label, chunk_label, list_zmax, jerksearch_zmax, jerksearch_wmax, flag_remove_duplicates, flag_DM_problems, flag_remove_harmonics, minimum_numDMs_where_detected, minimum_acceptable_DM=2.0, period_to_search_min_s=0.001, period_to_search_max_s=15.0, verbosity_level=0 ):


