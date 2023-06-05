from joblib import Parallel, delayed
import subprocess
import sys, os, argparse, errno
import numpy as np
from pulsar_miner import Observation

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def dedisperse(data, mask_file, dm, outfile_name, numout, working_dir, start_fraction=0.0, remove_dat_files=False):
    os.chdir(working_dir)
   
    outfile_name = '%s_DM%.2f' % (outfile_name, dm)
    fft_filename = '%s.fft' % outfile_name
    inf_file_name = '%s.inf' % outfile_name

    dedisp_cmd = 'prepdata -dm %.2f -mask %s -start %.2f -numout %d -o %s %s' % (dm, mask_file, start_fraction, numout, outfile_name, data)
    #realfft_cmd = 'realfft %s.dat' % outfile_name
    subprocess.check_output(dedisp_cmd, shell=True)
    #subprocess.check_output(realfft_cmd, shell=True)
   
    
    if remove_dat_files:
        os.remove('%s.dat' % outfile_name)
    

    
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Dedisperse a filterbank file using PRESTO and multiple CPU cores')
    parser.add_argument('-i', '--input', help='Input Data (Filterbank or PSRFITS)', required=True)
    parser.add_argument('-d', '--dm_low', help='Low Value of DM', type=float, default=0)
    parser.add_argument('-D', '--dm_high', help='High Value of DM', type=float, default=10.0)
    parser.add_argument('-t', '--total_ndms', help='Total DM trials', type=int, default=1)
    parser.add_argument('-m', '--mask', help='RFI-find Mask File', required=True)
    parser.add_argument('-n', '--ncpus', help='Number of CPU cores to use', type=int, default=1)
    parser.add_argument('-s', '--segment_label', help='Segment label (Pulsar miner convention)', type=str, default='full')
    parser.add_argument('-c', '--chunk_label', help='Chunk label (Pulsar miner convention)', type=str, default='ck00')
    parser.add_argument('-w', '--working_dir', help='Working dir where do the processing', type=str, default='/tmp')

    args = parser.parse_args()

    data = args.input
    mask_file = args.mask
    dm_low = args.dm_low
    dm_high = args.dm_high
    ncpus = args.ncpus
    total_ndms = args.total_ndms
    segment_label = args.segment_label
    chunk_label = args.chunk_label
    #basename = os.path.splitext(data)[0]
    basename = os.path.basename(data)[:-4]
    cluster = basename.split('_')[0]
    epoch = basename.split('_')[1]
    beam = basename.split('_')[2]
    dedisp_filename = '%s_%s_%s' % (basename,segment_label, chunk_label)
   
    obs_class = Observation(data, 'filterbank')
    tobs = obs_class.T_obs_s
    nsamps = obs_class.N_samples
    working_dir = args.working_dir
   
    working_dir = os.path.join(working_dir, 'dedisp_%s_%s_DM_%.2f_%.2f' % (segment_label, chunk_label, dm_low, dm_high))
    
    mkdir_p(working_dir)
    dm_trials = np.around(np.linspace(dm_low, dm_high, total_ndms), 2)
    if segment_label == 'full': 
        start_fraction = 0
        numout = nsamps
        if numout %2 != 0:
            numout -= 1 
    else:
        required_tobs = float(segment_label.replace('m', '')) * 60
        fraction = required_tobs / tobs
        numout = int(nsamps/fraction)
        if numout %2 != 0:
            numout -= 1 
        start_fraction = int(chunk_label.replace('ck', '')) * fraction
    print('Dedispersing %s with %d DM trials' % (data, len(dm_trials)))
    
    Parallel(n_jobs = ncpus)(delayed(dedisperse)(data, mask_file, dm_trials[i], dedisp_filename, numout, working_dir, start_fraction) for i in range(len(dm_trials)))
