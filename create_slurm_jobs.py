import numpy as np
import pandas as pd
import subprocess, shlex
import filterbank, rfifind
import os, glob, sys, errno, time
from pulsar_miner import SurveyConfiguration, Observation, get_command_output, get_DD_scheme_from_DDplan_output, make_rfifind_mask, prepdata, realfft, rednoise, make_zaplist
import configparser, argparse
from ast import literal_eval

parser = argparse.ArgumentParser(description='Run the Slurm wrapper on PulsarMiner/PRESTO')
parser.add_argument('-s', '--slurm_config', help='Slurm Configuration file', type=str,  default="slurm_config.cfg")
parser.add_argument('-p', '--pulsarminer_config', help='PulsarMiner Configuration file', type=str,  default="sample_M30.config")
parser.add_argument('-o', '--obs', help='Observation file ($Cluster_$epoch_$beam.fil)', type=str,  required=True)
parser.add_argument('-v', '--verbosity_level', help='Verbosity level', type=int,  default=1)

def run_ddplan(low_dm, high_dm, central_frequency, bandwidth, nchans, sampling_time, output_file, out_dir, coherent_dedisp_dm = 0):
    if coherent_dedisp_dm!=0:
        cmd_DDplan = 'DDplan.py -o ddplan_%s -l %.2f -d %.2f -c %.2f -f %.2f -b %d -n %d -t %.6f' % (output_file, low_dm, high_dm, coherent_dedisp_dm, central_frequency, abs(bandwidth), nchans, sampling_time)
    else:
        cmd_DDplan = 'DDplan.py -o ddplan_%s -l %.2f -d %.2f -f %.2f -b %d -n %d -t %.6f' % (output_file, low_dm, high_dm, central_frequency, abs(bandwidth), nchans, sampling_time)
    output_DDplan    = get_command_output(cmd_DDplan, shell_state=False, work_dir=out_dir)
    list_DD_schemes  = get_DD_scheme_from_DDplan_output(output_DDplan)
    
    return list_DD_schemes

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def call_pulsarminer_rfifind(cluster, epoch, beam, pm_config, LOG_dir, verbosity_level):
    """
    RFIFIND loop is identical to pulsarminer. 
    This has been copied over so that the slurm wrapper can be run from rfifind-folding.
    """

    if verbosity_level >= 1:
        print
        print "##################################################################################################"
        print "                                           STEP 1 - RFIFIND                                       "
        print "##################################################################################################"
        print
   
    rfifind_mask_dir = os.path.join(cluster, epoch, beam, "01_RFIFIND")
    mkdir_p(rfifind_mask_dir)
    
    rfifind_mask = "%s/%s_rfifind.mask" % (rfifind_mask_dir, pm_config.list_Observations.file_basename)
    pm_config.list_Observations.mask = rfifind_mask
    if pm_config.flag_step_rfifind == 1:
            LOG_basename = "01_rfifind_%s" % (pm_config.list_Observations.file_nameonly)
            log_abspath = "%s/LOG_%s.txt" % (LOG_dir, LOG_basename)
            if verbosity_level >= 1:
                    print "\033[1m >> TIP:\033[0m Check rfifind progress with '\033[1mtail -f %s\033[0m'" % (log_abspath)
                    print
                    print "Creating rfifind mask of observation: '%s'..." % (pm_config.list_Observations.file_nameonly), ; sys.stdout.flush()

    
            make_rfifind_mask(pm_config.list_Observations.file_abspath,
                                                rfifind_mask_dir,
                                                LOG_dir,
                                                LOG_basename,
                                                pm_config.rfifind_time,
                                                pm_config.rfifind_freqsig,
                                                pm_config.rfifind_timesig,
                                                pm_config.rfifind_intfrac,
                                                pm_config.rfifind_chanfrac,
                                                pm_config.rfifind_time_intervals_to_zap,
                                                pm_config.rfifind_chans_to_zap,
                                                pm_config.rfifind_flags,
                                                pm_config.presto_env,
                                                verbosity_level)

    elif pm_config.flag_step_rfifind == 0:
        if verbosity_level >= 1:
                print "STEP_RFIFIND = %s   ---> I will not create the mask, I will only look for the one already present in the 01_RFIFIND folder" % (pm_config.flag_step_rfifind)

        if not os.path.exists(pm_config.list_Observations.mask):
                print
                print 
                print "\033[1m  ERROR! File '%s' not found! \033[0m" % (pm_config.list_Observations.mask)
                print
                print "You must create a mask for your observation, in order to run the pipeline."
                print "Set STEP_RFIFIND = 1 in your configuration file and retry."
                print
                exit()
        else:
                print
                print "File '%s' found! Using it as the mask." % (pm_config.list_Observations.mask)
   
    mask = rfifind.rfifind(pm_config.list_Observations.mask)
    fraction_masked_channels = np.float(len(mask.mask_zap_chans))/mask.nchan
    if verbosity_level >= 1:
            print
            print "RFIFIND: Percentage of frequency channels masked: %.2f%%" % (fraction_masked_channels * 100.)
            print 
    if fraction_masked_channels > 0.5:
            print
            print "************************************************************************************************"
            print "\033[1m !!! WARNING : %.2f%% of the band was masked! That seems quite a lot! \033[0m !!!" % (fraction_masked_channels * 100.)
            print "\033[1m !!! You may want to adjust the RFIFIND parameters in the configuration file (e.g. try to increase RFIFIND_FREQSIG) \033[0m"
            print "************************************************************************************************"
            time.sleep(10)
                    
    weights_file = pm_config.list_Observations.mask.replace(".mask", ".weights")
    if os.path.exists(weights_file):
            array_weights = np.loadtxt(weights_file, unpack=True, usecols=(0,1,), skiprows=1)
            pm_config.ignorechan_list = ",".join([ str(x) for x in np.where(array_weights[1] == 0)[0] ])
            pm_config.nchan_ignored = len(pm_config.ignorechan_list.split(","))
            if verbosity_level >= 1:
                    print
                    print
                    print "WEIGHTS file '%s' found. Using it to ignore %d channels out of %d (%.2f%%)" % (os.path.basename(weights_file), pm_config.nchan_ignored, pm_config.list_Observations.nchan, 100*pm_config.nchan_ignored/np.float(pm_config.list_Observations.nchan) )
                    print "IGNORED CHANNELS: %s" % (pm_config.ignorechan_list)

    return rfifind_mask


def call_pulsarminer_birdies(cluster, epoch, beam, pm_config, LOG_dir, verbosity_level):
    '''
    Generating birdie list is identical to pulsarminer
    '''
    ##################################################################################################
    # 2) BIRDIES AND ZAPLIST
    ##################################################################################################
  
    if verbosity_level >= 1:
            print
            print
            print
            print "##################################################################################################"
            print "                                   STEP 2 - BIRDIES AND ZAPLIST                                   "
            print "##################################################################################################"
            print
    if verbosity_level >= 2:
            print "STEP_ZAPLIST = %s" % (pm_config.flag_step_zaplist)


    #dir_birdies = os.path.join(pm_config.root_workdir, "02_BIRDIES")
    dir_birdies = os.path.join(cluster, epoch, beam, "02_BIRDIES")
    #Copy common birdie files to the working directory
    os.system("cp %s %s" % ("common_birdies.txt", pm_config.root_workdir))

    if pm_config.flag_step_zaplist == 1:

            if verbosity_level >= 2:
                    print "# ====================================================================================="
                    print "# a) Create a 0-DM TOPOCENTRIC time series for each of the files, using the mask."
                    print "# ====================================================================================="
            if not os.path.exists(dir_birdies):         
                os.mkdir(dir_birdies)
            time.sleep(0.1)
            print "Running prepdata to create 0-DM and topocentric time series of \"%s\"..." % (pm_config.list_Observations.file_nameonly), ; sys.stdout.flush()
            LOG_basename = "02a_prepdata_%s" % (pm_config.list_Observations.file_nameonly)
            prepdata( pm_config.list_Observations.file_abspath,
                    dir_birdies,
                    LOG_dir,
                    LOG_basename,
                    0,
                    pm_config.list_Observations.N_samples,
                    pm_config.ignorechan_list,
                    pm_config.list_Observations.mask,
                    1,
                    "topocentric",
                    pm_config.prepdata_flags,
                    pm_config.presto_env,
                    verbosity_level
            )
            if verbosity_level >= 1:
                    print "done!"; sys.stdout.flush()

            if verbosity_level >= 2:
                    print "# ==============================================="
                    print "# b) Fourier transform all the files"
                    print "# ==============================================="
                    print

            pm_config.list_0DM_datfiles = glob.glob("%s/*%s*.dat" % (dir_birdies,pm_config.list_Observations.file_basename))   # Collect the *.dat files in the 02_BIRDIES_FOLDERS

            time.sleep(0.1)
            if verbosity_level >= 1:
                    print "Running realfft on the 0-DM topocentric timeseries %s" % (os.path.basename(pm_config.list_0DM_datfiles[0])), ; sys.stdout.flush()
          
            LOG_basename = "02b_realfft_%s" % (os.path.basename(pm_config.list_0DM_datfiles[0]))
            
            
            
          
            
            realfft(os.path.abspath(pm_config.list_0DM_datfiles[0]),
                    dir_birdies,
                    LOG_dir,
                    LOG_basename,
                    pm_config.realfft_flags,
                    pm_config.presto_env,
                    verbosity_level,
                    flag_LOG_append=0
            )
            
            if verbosity_level >= 1:
                    print "done!"; sys.stdout.flush()

            if verbosity_level >= 2:
                    print
                    print "# ==============================================="
                    print "# 02c) Remove rednoise"
                    print "# ==============================================="
                    print
            pm_config.list_0DM_fftfiles = [x for x in glob.glob("%s/*%s*DM00.00.fft" % (dir_birdies, pm_config.list_Observations.file_basename)) if not "_red" in x ]  # Collect the *.fft files in the 02_BIRDIES_FOLDERS, exclude red files
            
            time.sleep(0.1)                        
            print "Running rednoise on the FFT \"%s\"..." % (os.path.basename(pm_config.list_0DM_datfiles[0])) , ; sys.stdout.flush()
            LOG_basename = "02c_rednoise_%s" % (os.path.basename(pm_config.list_0DM_fftfiles[0]))
            rednoise(os.path.abspath(pm_config.list_0DM_fftfiles[0]),
                    dir_birdies,
                    LOG_dir,
                    LOG_basename,
                    pm_config.rednoise_flags,
                    pm_config.presto_env,
                    verbosity_level
            )
           
            if verbosity_level >= 1:
                    print "done!"; sys.stdout.flush()

            if verbosity_level >= 2:
                    print
                    print "# ==============================================="
                    print "# 02d) Accelsearch e zaplist"
                    print "# ==============================================="
                    print
                    
            pm_config.list_0DM_fft_rednoise_files = glob.glob("%s/*%s*_DM00.00.fft" % (dir_birdies, pm_config.list_Observations.file_basename))
            time.sleep(0.1)
            print "Making zaplist of 0-DM topocentric time series \"%s\"..." % (os.path.basename(pm_config.list_0DM_datfiles[0])), ; sys.stdout.flush() 
            LOG_basename = "02d_makezaplist_%s" % (os.path.basename(pm_config.list_0DM_fft_rednoise_files[0]))
            zaplist_filename = make_zaplist(os.path.abspath(pm_config.list_0DM_fft_rednoise_files[0]),
                                                dir_birdies,
                                                LOG_dir,
                                                LOG_basename,
                                                pm_config.file_common_birdies,
                                                2,
                                                pm_config.accelsearch_flags,
                                                pm_config.presto_env,
                                                verbosity_level
            )
            if verbosity_level >= 1:
                    print "done!"; sys.stdout.flush()
        #Zapping known pulsars will be added later.
        # if pm_config.zap_isolated_pulsars_from_ffts == 1:
        #         fourier_bin_size =  1./pm_config.list_Observations[0].T_obs_s
        #         zaplist_file = open(zaplist_filename, 'a')

        #         zaplist_file.write("########################################\n")
        #         zaplist_file.write("#              KNOWN PULSARS           #\n")
        #         zaplist_file.write("########################################\n")
        #         for psr in sorted(dict_freqs_to_zap.keys()):
        #                 zaplist_file.write("# Pulsar %s \n" % (psr))
        #                 for i_harm in range(1, pm_config.zap_isolated_pulsars_max_harm+1):
        #                         zaplist_file.write("B%21.14f   %19.17f\n" % (dict_freqs_to_zap[psr]*i_harm, fourier_bin_size*i_harm))
        #         zaplist_file.close()

def main():
    args = parser.parse_args()
    # Read config file and input data
    slurm_config_file = args.slurm_config
    pulsarminer_config_file = args.pulsarminer_config
    observation_name = args.obs
    verbosity_level = args.verbosity_level
    #output_dir = args.results_dir
    #mkdir_p(output_dir)
    observation_name_no_extension = os.path.basename(os.path.splitext(observation_name)[0])
    
    # Get the cluster, epoch and beam from the observation name
    cluster = observation_name_no_extension.split('_')[0]
    epoch = observation_name_no_extension.split('_')[1]
    beam = observation_name_no_extension.split('_')[2]

    config_slurm = configparser.ConfigParser()
    config_slurm.read(slurm_config_file)
    

    verbosity_level = 1
    pm_config = SurveyConfiguration(pulsarminer_config_file, verbosity_level)
    


    

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
    survey_config = pm_config.dict_survey_configuration
    LOG_dir = os.path.join(pm_config.root_workdir, "LOG")
    
    if not os.path.exists(LOG_dir): 
         os.mkdir(LOG_dir)
    #Rfifind
    rfifind_mask = call_pulsarminer_rfifind(cluster, epoch, beam, pm_config, LOG_dir, verbosity_level)
    rfifind_mask = os.path.abspath(rfifind_mask)
    
    pm_config.list_Observations.mask = rfifind_mask
    #Create birdie list.
    birdies = call_pulsarminer_birdies(cluster, epoch, beam, pm_config, LOG_dir, verbosity_level)
    #DDplan
    central_frequency = float(pm_config.list_Observations.freq_central_MHz)
    bandwidth = float(pm_config.list_Observations.bw_MHz)
    tobs = float(pm_config.list_Observations.T_obs_s) 
    nchans = int(pm_config.list_Observations.nchan)
    tsamp = float(pm_config.list_Observations.t_samp_s)
    low_dm = float(pm_config.dict_survey_configuration['DM_MIN'])
    high_dm = float(pm_config.dict_survey_configuration['DM_MAX'])
    coherent_dedisp_dm = float(pm_config.dict_survey_configuration['DM_COHERENT_DEDISPERSION'])

    
    time_segments = pm_config.dict_survey_configuration['LIST_SEGMENTS']
    time_segments = time_segments.split(',')

    dir_dedispersion = os.path.join(pm_config.root_workdir, cluster, epoch, beam, "03_DEDISPERSION")
    mkdir_p(dir_dedispersion)

    ddplan_results = run_ddplan(low_dm, high_dm, central_frequency, bandwidth, nchans, tsamp, observation_name_no_extension, dir_dedispersion, coherent_dedisp_dm)[0]
   
    ndm_trials = ddplan_results['num_DMs']
    downsamp = ddplan_results['downsamp']
    dDM = ddplan_results['dDM']
    
    full_data_path = os.path.join(pm_config.folder_datafiles, pm_config.list_datafiles)
    dedisp_cpu_cores = int(config_slurm['Dedispersion']['CPUS_PER_TASK'])
    accel_cpus = int(config_slurm['Acceleration_Search']['CPUS_PER_TASK'])
    jerk_cpus = int(config_slurm['Jerk_Search']['CPUS_PER_TASK'])
    sift_cpus = int(config_slurm['Sifting']['CPUS_PER_TASK'])

    #Get the wallclock time for each process.
    dedisp_wall_clock = config_slurm['Dedispersion']['WALL_CLOCK_TIME']
    accel_wall_clock = config_slurm['Acceleration_Search']['WALL_CLOCK_TIME']
    jerk_wall_clock = config_slurm['Jerk_Search']['WALL_CLOCK_TIME']
    sift_wall_clock = config_slurm['Sifting']['WALL_CLOCK_TIME']

    #Get ram for each process.
    dedisp_ram = config_slurm['Dedispersion']['RAM_PER_JOB']
    accel_ram = config_slurm['Acceleration_Search']['RAM_PER_JOB']
    jerk_ram = config_slurm['Jerk_Search']['RAM_PER_JOB']
    sift_ram = config_slurm['Sifting']['RAM_PER_JOB']

    #Get the partition for each process.
    dedisp_partition = config_slurm['Dedispersion']['JOB_PARTITION']
    accel_partition = config_slurm['Acceleration_Search']['JOB_PARTITION']
    jerk_partition = config_slurm['Jerk_Search']['JOB_PARTITION']
    sift_partition = config_slurm['Sifting']['JOB_PARTITION']


    jerk_search_zmax = int(pm_config.jerksearch_zmax)
    jerk_search_wmax = int(pm_config.jerksearch_wmax)
    jerk_search_numharm = int(pm_config.jerksearch_numharm)
    jerk_search_cpus_per_proc = int(pm_config.jerksearch_ncpus)
    dir_jerk_search = os.path.join(pm_config.root_workdir, cluster, epoch, beam, "03_JERK_SEARCH")

    singularity_image = config_slurm['Singularity_Image']['Singularity_Image_Path']
    mount_path = config_slurm['Singularity_Image']['MOUNT_PATH']
    code_directory = config_slurm['Singularity_Image']['CODE_DIRECTORY_ABS_PATH']
    #get current working directory
    cwd = os.getcwd()
    # Divide total number of DM trials by the number of cores per job
    loop_trials = ndm_trials//dedisp_cpu_cores
    if ndm_trials % dedisp_cpu_cores != 0:
          total_batches = loop_trials + 1
   
    # Create a slurm job file for dedispersion
    with open('slurm_jobs_%s.sh' %observation_name_no_extension, 'w') as f:
        f.write('#!/bin/bash' + '\n')
        f.write("logs='slurm_job_logs/'" + '\n')
        f.write('mkdir -p $logs' + '\n')
        #f.write('epoch=%s'%epoch + '\n')
        f.write('data_path=%s'%full_data_path + '\n')
        f.write('\n')
        slurmids=""
        f.write('\n')
        for i in range(loop_trials):
            batch_number = i + 1
            dm_loop_low = round(low_dm + i* (dDM * dedisp_cpu_cores), 2)
            dm_loop_high = round(dm_loop_low + dDM * dedisp_cpu_cores - dDM, 2)
            dm_loop_trials = np.linspace(dm_loop_low, dm_loop_high, dedisp_cpu_cores)
            # Start by running Dedispersion on the full data.
            time_segment = 'full'
            chunk_label = 'ck00'
            output_dir = os.path.join(cluster, epoch, beam, "03_DEDISPERSION", observation_name_no_extension, time_segment, chunk_label)
            output_dir = os.path.abspath(output_dir)
            mkdir_p(output_dir)
            if dedisp_partition == 'gpu.q':
                dedisp_script = '''dedisp=$(sbatch --parsable --job-name=dedisp --output=$logs/%s_dedisp_%s_%s_%s_%s_batch_%d.out --error=$logs/%s_dedisp_%s_%s_%s_%s_batch_%d.err -p gpu.q --gres=gpu:1 --export=ALL --cpus-per-task=%d --time=%s --mem=%s --wrap="%s/DEDISPERSE_AND_COPY_BACK.sh %s %s %s %s %s %.2f %.2f %d %d %s %s %s %s")''' \
                %(cluster, epoch, beam, time_segment, chunk_label, batch_number, cluster, epoch, beam, time_segment, chunk_label, batch_number, dedisp_cpu_cores, dedisp_wall_clock, dedisp_ram, cwd, singularity_image, mount_path, code_directory, full_data_path, rfifind_mask, dm_loop_low, dm_loop_high, dedisp_cpu_cores, dedisp_cpu_cores, time_segment, chunk_label, dir_dedispersion, output_dir)
            else:
                 dedisp_script = '''dedisp=$(sbatch --parsable --job-name=dedisp --output=$logs/%s_dedisp_%s_%s_%s_%s_batch_%d.out --error=$logs/%s_dedisp_%s_%s_%s_%s_batch_%d.err -p %s --export=ALL --cpus-per-task=%d --time=%s --mem=%s --wrap="%s/DEDISPERSE_AND_COPY_BACK.sh %s %s %s %s %s %.2f %.2f %d %d %s %s %s %s")''' \
                %(cluster, epoch, beam, time_segment, chunk_label, batch_number, cluster, epoch, beam, time_segment, chunk_label, batch_number, dedisp_partition, dedisp_cpu_cores, dedisp_wall_clock, dedisp_ram, cwd, singularity_image, mount_path, code_directory, full_data_path, rfifind_mask, dm_loop_low, dm_loop_high, dedisp_cpu_cores, dedisp_cpu_cores, time_segment, chunk_label, dir_dedispersion, output_dir)
                 
            f.write('########## Starting batch %d of %d ##########' %(batch_number, total_batches) + '\n')
            f.write('###################################### Running Dedispersion on Cluster %s, Epoch %s, Beam %s Segment %s, Chunk %s using %d CPUs   ##################################################################' %(cluster, epoch, beam, time_segment,chunk_label, dedisp_cpu_cores) + '\n')
            f.write(dedisp_script + '\n' + '\n')
            f.write('slurmids="$slurmids:$dedisp"' + '\n')

            full_length_dat_file_dir = output_dir

            # Now for each DM trial and time segment, run the Acceleration/Jerk search.
            for j in range(len(time_segments)):
                
                if time_segments[j] == 'full':
                    chunk_label = 'ck00'
                    output_dir = os.path.join(cluster, epoch, beam, "03_DEDISPERSION", observation_name_no_extension, time_segments[j], chunk_label)
                    output_dir = os.path.abspath(output_dir)
                    mkdir_p(output_dir)
                    
                    
                    for dm in dm_loop_trials:
                        #dat_file = os.path.join(output_dir, '%s_%s_%s_%s_%s_DM%.2f.dat' %(cluster, epoch, beam, time_segments[j], chunk_label, dm))
                        dat_file = os.path.join(full_length_dat_file_dir, '%s_%s_%s_full_ck00_DM%.2f.dat' %(cluster, epoch, beam, dm))
                        working_dir_search = os.path.join(dir_jerk_search, observation_name_no_extension, time_segments[j], chunk_label, 'DM%.2f' %dm)
                        f.write('###################################### Running Periodicity Search on Observation: %s, segment: %s chunk: %s    ##################################################################' %(os.path.basename(dat_file), time_segments[j],chunk_label) + '\n')

                        # Create a slurm job file for jerk search
                        if jerk_partition == 'gpu.q':
                            search_script = '''search=$(sbatch --parsable --job-name=jerk_search --dependency=afterok:$dedisp --output=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.out --error=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.err -p gpu.q --gres=gpu:1 --export=ALL --cpus-per-task=%d --time=%s --mem=%s --wrap="%s/PERIODICITY_SEARCH_AND_COPY_BACK.sh %s %s %s %s %d %d %d %d %s %s %s %s")''' \
                            %(cluster, epoch, beam, time_segments[j], chunk_label, dm, cluster, epoch, beam, time_segments[j], chunk_label, dm, jerk_cpus, jerk_wall_clock, jerk_ram, cwd, singularity_image, mount_path, code_directory, dat_file, jerk_search_zmax, jerk_search_wmax,jerk_search_numharm, jerk_search_cpus_per_proc, working_dir_search, output_dir, time_segments[j], chunk_label)
                        else:
                            search_script = '''search=$(sbatch --parsable --job-name=jerk_search --dependency=afterok:$dedisp --output=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.out --error=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.err -p %s --export=ALL --cpus-per-task=%d --time=%s --mem=%s --wrap="%s/PERIODICITY_SEARCH_AND_COPY_BACK.sh %s %s %s %s %d %d %d %d %s %s %s %s")''' \
                            %(cluster, epoch, beam, time_segments[j], chunk_label, dm, cluster, epoch, beam, time_segments[j], chunk_label, dm, jerk_partition, jerk_cpus, jerk_wall_clock, jerk_ram, cwd, singularity_image, mount_path, code_directory, dat_file, jerk_search_zmax, jerk_search_wmax,jerk_search_numharm, jerk_search_cpus_per_proc, working_dir_search, output_dir, time_segments[j], chunk_label)
                        f.write(search_script + '\n' + '\n')
                        f.write('slurmids="$slurmids:$search"' + '\n')
                
                else:
                    time_chunk = float(time_segments[j].replace('m','')) * 60
                    nchunks = int(tobs/time_chunk)
                   
                    for k in range(nchunks):
                        chunk_label = 'ck%02d' %k
                        output_segment_label = '%sm' %time_segments[j]
                        output_dir = os.path.join(cluster, epoch, beam, "03_DEDISPERSION", observation_name_no_extension, output_segment_label, chunk_label)
                        output_dir = os.path.abspath(output_dir)
                        mkdir_p(output_dir)
                        
                        for dm in dm_loop_trials:
                            
                            dat_file = os.path.join(full_length_dat_file_dir, '%s_%s_%s_full_ck00_DM%.2f.dat' %(cluster, epoch, beam, dm))
                            working_dir_search = os.path.join(dir_jerk_search, observation_name_no_extension, time_segments[j], chunk_label, 'DM%.2f' %(dm))

                            # Create a slurm job file for jerk search
                            f.write('###################################### Splitting timeseries and Running Periodicity Search on Observation: %s, segment: %sm chunk: %s    ##################################################################' %(os.path.basename(dat_file), time_segments[j],chunk_label) + '\n')
                            if jerk_partition == 'gpu.q':
                                  
                                search_script = '''search=$(sbatch --parsable --job-name=jerk_search --dependency=afterok:$dedisp --output=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.out --error=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.err -p gpu.q --gres=gpu:1 --export=ALL --cpus-per-task=%d --time=%s --mem=%s --wrap="%s/PERIODICITY_SEARCH_AND_COPY_BACK.sh %s %s %s %s %d %d %d %d %s %s %s %s")''' \
                                %(cluster, epoch, beam, time_segments[j], chunk_label, dm, cluster, epoch, beam, time_segments[j], chunk_label, dm, jerk_cpus, jerk_wall_clock, jerk_ram, cwd, singularity_image, mount_path, code_directory, dat_file, jerk_search_zmax, jerk_search_wmax,jerk_search_numharm, jerk_search_cpus_per_proc, working_dir_search, output_dir, time_segments[j], chunk_label)
                            else:
                                search_script = '''search=$(sbatch --parsable --job-name=jerk_search --dependency=afterok:$dedisp --output=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.out --error=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.err -p %s --export=ALL --cpus-per-task=%d --time=%s --mem=%s --wrap="%s/PERIODICITY_SEARCH_AND_COPY_BACK.sh %s %s %s %s %d %d %d %d %s %s %s %s")''' \
                                %(cluster, epoch, beam, time_segments[j], chunk_label, dm, cluster, epoch, beam, time_segments[j], chunk_label, dm, jerk_partition, jerk_cpus, jerk_wall_clock, jerk_ram, cwd, singularity_image, mount_path, code_directory, dat_file, jerk_search_zmax, jerk_search_wmax,jerk_search_numharm, jerk_search_cpus_per_proc, working_dir_search, output_dir, time_segments[j], chunk_label)

                            f.write(search_script + '\n' + '\n')
                            f.write('slurmids="$slurmids:$search"' + '\n')
        
        # In case the number of DMs is not divisible by the number of cores, we do the remaining now by adjusting the cpus-per-task.
        if ndm_trials % dedisp_cpu_cores != 0:
            batch_number += 1
            # Remaining DMs
            dm_loop_low = dm_loop_high + dDM
            dm_loop_high = high_dm
            ntrials_dm = round((dm_loop_high - dm_loop_low)/dDM) + 1
            dm_loop_trials = np.linspace(dm_loop_low, dm_loop_high, ntrials_dm)
            dedisp_cpu_cores = ntrials_dm
            time_segment = 'full'
            chunk_label = 'ck00'
            output_dir = os.path.join(cluster, epoch, beam, "03_DEDISPERSION", observation_name_no_extension, time_segment, chunk_label)
            output_dir = os.path.abspath(output_dir)
            mkdir_p(output_dir)
            if dedisp_partition == 'gpu.q':
                dedisp_script = '''dedisp=$(sbatch --parsable --job-name=dedisp --output=$logs/%s_dedisp_%s_%s_%s_%s_batch_%d.out --error=$logs/%s_dedisp_%s_%s_%s_%s_batch_%d.err -p gpu.q --gres=gpu:1 --export=ALL --cpus-per-task=%d --time=%s --mem=%s --wrap="%s/DEDISPERSE_AND_COPY_BACK.sh %s %s %s %s %s %.2f %.2f %d %d %s %s %s %s")''' \
                %(cluster, epoch, beam, time_segment, chunk_label, batch_number, cluster, epoch, beam, time_segment, chunk_label, batch_number, dedisp_cpu_cores, dedisp_wall_clock, dedisp_ram, cwd, singularity_image, mount_path, code_directory ,full_data_path, rfifind_mask, dm_loop_low, dm_loop_high, dedisp_cpu_cores, dedisp_cpu_cores, time_segment, chunk_label, dir_dedispersion, output_dir)
            else:
                 dedisp_script = '''dedisp=$(sbatch --parsable --job-name=dedisp --output=$logs/%s_dedisp_%s_%s_%s_%s_batch_%d.out --error=$logs/%s_dedisp_%s_%s_%s_%s_batch_%d.err -p %s --export=ALL --cpus-per-task=%d --time=%s --mem=%s --wrap="%s/DEDISPERSE_AND_COPY_BACK.sh %s %s %s %s %s %.2f %.2f %d %d %s %s %s %s")''' \
                %(cluster, epoch, beam, time_segment, chunk_label, batch_number, cluster, epoch, beam, time_segment, chunk_label, batch_number, dedisp_partition, dedisp_cpu_cores, dedisp_wall_clock, dedisp_ram, cwd, singularity_image, mount_path, code_directory ,full_data_path, rfifind_mask, dm_loop_low, dm_loop_high, dedisp_cpu_cores, dedisp_cpu_cores, time_segment, chunk_label, dir_dedispersion, output_dir)
                 
            f.write('########## Starting batch %d of %d ##########' %(batch_number, total_batches) + '\n')
            f.write('###################################### Running Dedispersion on Cluster %s, Epoch %s, Beam %s Segment %s, Chunk %s using %d CPUs   ##################################################################' %(cluster, epoch, beam, time_segment,chunk_label, dedisp_cpu_cores) + '\n')
            f.write(dedisp_script + '\n' + '\n')
            f.write('slurmids="$slurmids:$dedisp"' + '\n')

            
            for j in range(len(time_segments)):
                
                batch_number = loop_trials + 1
                if time_segments[j] == 'full':
                    chunk_label = 'ck00'
                    output_dir = os.path.join(cluster, epoch, beam, "03_DEDISPERSION", observation_name_no_extension, time_segments[j], chunk_label)
                    output_dir = os.path.abspath(output_dir)
                    mkdir_p(output_dir)
                    for dm in dm_loop_trials:
                        dat_file = os.path.join(full_length_dat_file_dir, '%s_%s_%s_full_ck00_DM%.2f.dat' %(cluster, epoch, beam, dm))
                        #dat_file = os.path.join(output_dir, '%s_%s_%s_%s_%s_DM%.2f.dat' %(cluster, epoch, beam, time_segments[j], chunk_label, dm))
                        working_dir_search = os.path.join(dir_jerk_search, observation_name_no_extension, time_segments[j], chunk_label, 'DM%.2f' %dm)
                        f.write('###################################### Running Periodicity Search on Observation: %s, segment: %s chunk: %s    ##################################################################' %(os.path.basename(dat_file), time_segments[j],chunk_label) + '\n')

                        # Create a slurm job file for jerk search
                        if jerk_partition == 'gpu.q':
                              
                            search_script = '''search=$(sbatch --parsable --job-name=jerk_search --dependency=afterok:$dedisp --output=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.out --error=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.err -p gpu.q --gres=gpu:1 --export=ALL --cpus-per-task=%d --time=%s --mem=%s --wrap="%s/PERIODICITY_SEARCH_AND_COPY_BACK.sh %s %s %s %s %d %d %d %d %s %s %s %s")''' \
                            %(cluster, epoch, beam, time_segments[j], chunk_label, dm, cluster, epoch, beam, time_segments[j], chunk_label, dm, jerk_cpus, jerk_wall_clock, jerk_ram, cwd, singularity_image, mount_path, code_directory, dat_file, jerk_search_zmax, jerk_search_wmax,jerk_search_numharm, jerk_search_cpus_per_proc, working_dir_search, output_dir, time_segments[j], chunk_label)
                        else:
                            search_script = '''search=$(sbatch --parsable --job-name=jerk_search --dependency=afterok:$dedisp --output=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.out --error=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.err -p %s --export=ALL --cpus-per-task=%d --time=%s --mem=%s --wrap="%s/PERIODICITY_SEARCH_AND_COPY_BACK.sh %s %s %s %s %d %d %d %d %s %s %s %s")''' \
                            %(cluster, epoch, beam, time_segments[j], chunk_label, dm, cluster, epoch, beam, time_segments[j], chunk_label, dm, jerk_partition, jerk_cpus, jerk_wall_clock, jerk_ram, cwd, singularity_image, mount_path, code_directory, dat_file, jerk_search_zmax, jerk_search_wmax,jerk_search_numharm, jerk_search_cpus_per_proc, working_dir_search, output_dir, time_segments[j], chunk_label)

                        f.write(search_script + '\n' + '\n')
                        f.write('slurmids="$slurmids:$search"' + '\n')
                
                else:
                    
                    time_chunk = float(time_segments[j].replace('m','')) * 60
                    nchunks = int(tobs/time_chunk)
                    for k in range(nchunks):
                        chunk_label = 'ck%02d' %k
                        output_segment_label = '%sm' %time_segments[j]
                        output_dir = os.path.join(cluster, epoch, beam, "03_DEDISPERSION", observation_name_no_extension, output_segment_label, chunk_label)
                        output_dir = os.path.abspath(output_dir)
                        mkdir_p(output_dir)
                    
                        for dm in dm_loop_trials:
                            dat_file = os.path.join(full_length_dat_file_dir, '%s_%s_%s_full_ck00_DM%.2f.dat' %(cluster, epoch, beam, dm))
                            #dat_file = os.path.join(output_dir, '%s_%s_%s_%s_%s_DM%.2f.dat' %(cluster, epoch, beam, time_segments[j], chunk_label, dm))
                            working_dir_search = os.path.join(dir_jerk_search, observation_name_no_extension, time_segments[j], chunk_label, 'DM%.2f' %(dm))

                            # Create a slurm job file for jerk search
                            f.write('###################################### Splitting timeseries and Running Periodicity Search on Observation: %s, segment: %sm chunk: %s    ##################################################################' %(os.path.basename(dat_file), time_segments[j],chunk_label) + '\n')
                            if jerk_partition == 'gpu.q':
                                search_script = '''search=$(sbatch --parsable --job-name=jerk_search --dependency=afterok:$dedisp --output=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.out --error=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.err -p gpu.q --gres=gpu:1 --export=ALL --cpus-per-task=%d --time=%s --mem=%s --wrap="%s/PERIODICITY_SEARCH_AND_COPY_BACK.sh %s %s %s %s %d %d %d %d %s %s %s %s")''' \
                                %(cluster, epoch, beam, time_segments[j], chunk_label, dm, cluster, epoch, beam, time_segments[j], chunk_label, dm, jerk_cpus, jerk_wall_clock, jerk_ram, cwd, singularity_image, mount_path, code_directory, dat_file, jerk_search_zmax, jerk_search_wmax,jerk_search_numharm, jerk_search_cpus_per_proc, working_dir_search, output_dir, time_segments[j], chunk_label)
                            else:
                                 search_script = '''search=$(sbatch --parsable --job-name=jerk_search --dependency=afterok:$dedisp --output=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.out --error=$logs/%s_jerk_search_%s_%s_%s_%s_DM%.2f.err -p %s --export=ALL --cpus-per-task=%d --time=%s --mem=%s --wrap="%s/PERIODICITY_SEARCH_AND_COPY_BACK.sh %s %s %s %s %d %d %d %d %s %s %s %s")''' \
                                %(cluster, epoch, beam, time_segments[j], chunk_label, dm, cluster, epoch, beam, time_segments[j], chunk_label, dm, jerk_partition, jerk_cpus, jerk_wall_clock, jerk_ram, cwd, singularity_image, mount_path, code_directory, dat_file, jerk_search_zmax, jerk_search_wmax,jerk_search_numharm, jerk_search_cpus_per_proc, working_dir_search, output_dir, time_segments[j], chunk_label)
                            f.write(search_script + '\n' + '\n')
                            f.write('slurmids="$slurmids:$search"' + '\n')
        
        #Start the slurm sifting job. This will run after all the dedispersion and periodicity search jobs have finished
        f.write('###################################### Running Sifting on Cluster %s, Epoch %s, Beam %s using %d CPUs   ##################################################################' %(cluster, epoch, beam, sift_cpus) + '\n')
        tmp_dir_sifting_and_fold_script_creation = os.path.join(pm_config.root_workdir, cluster, epoch, beam)
        output_dir = os.path.join(cluster, epoch, beam)
        output_dir = os.path.abspath(output_dir)
        sift_and_create_fold_script = '''sift_and_create_fold_command_file=$(sbatch --parsable --job-name=sift_and_create_fold_command_file --dependency=afterok$slurmids --output=$logs/%s_sift_and_create_fold_command_file_%s_%s.out --error=$logs/%s_sift_and_create_fold_command_file_%s_%s.err -p %s --export=ALL --cpus-per-task=%d --time=%s --mem=%s --wrap="%s/SIFT_CREATE_FOLD_SCRIPT_AND_COPY_BACK.sh %s %s %s %s %s %s %s")''' %(cluster, epoch, beam, cluster, epoch, beam, sift_partition, sift_cpus, sift_wall_clock, sift_ram, code_directory, singularity_image, mount_path, code_directory, tmp_dir_sifting_and_fold_script_creation, output_dir, full_data_path, os.path.abspath(pulsarminer_config_file))
        f.write(sift_and_create_fold_script + '\n' + '\n')
        f.write('slurmids="$slurmids:$sift_script"' + '\n')
                    
   


#maxjobs=$(sacctmgr list associations format=user,maxsubmitjobs -n | grep $user | awk '{print $2}')
if __name__ == '__main__':
    main()




