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

def periodicity_search(data, zmax, wmax, ncpus, working_dir, num_harm=8, remove_fft_files=True, remove_dat_files=True):
    os.chdir(working_dir)
    if data.endswith('.dat'):
        realfft_cmd = 'realfft %s' % data
        subprocess.check_output(realfft_cmd, shell=True)
        
        data = data.replace('.dat', '.fft')
    if wmax > 0:
        print('Running Jerk Search on %s using %d CPUs' % (data, ncpus))
        jerk_search_cmd = 'accelsearch -ncpus %d -numharm %d -zmax %d -wmax %d %s' % (ncpus, num_harm, zmax, wmax, data)
        subprocess.check_output(jerk_search_cmd, shell=True)
        
    else:
        print('Running Accel Search on %s using %d CPUs' % (data, ncpus))
        accel_search_cmd = 'accelsearch -ncpus %d -numharm %d -zmax %d %s' % (ncpus, num_harm, zmax, data)
        subprocess.check_output(accel_search_cmd, shell=True)
    
    
    if remove_fft_files:
      os.remove(data)
    if remove_dat_files:
      os.remove(data.replace('.fft', '.dat'))
    

    
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Accel/Jerk Search PRESTO using multiple CPU cores')
    parser.add_argument('-i', '--input', help='Input Data (Presto *dat/.fft file)', required=True)
    parser.add_argument('-z', '--zmax', help='zmax value to be used for jerk search', type=int, default=200)
    parser.add_argument('-w', '--wmax', help='wmax value to be used for jerk search', type=int, default=0)
    parser.add_argument('-s', '--harmonic_sums', help='No. harmonic sums to perform', type=int, default=8)
    parser.add_argument('-n', '--ncpus', help='Number of CPU cores to use', type=int, default=1)
    parser.add_argument('-t', '--tmp_working_dir', help='TMP Working dir where do the processing', type=str, default='/tmp')

    args = parser.parse_args()

    data = args.input
    ncpus = args.ncpus
    zmax = args.zmax
    wmax = args.wmax
    harmonic_sums = args.harmonic_sums
    basename = os.path.basename(data)[:-4]
    cluster = basename.split('_')[0]
    epoch = basename.split('_')[1]
    beam = basename.split('_')[2]
 
    working_dir = args.tmp_working_dir
    mkdir_p(working_dir)
    
    periodicity_search(data, zmax, wmax, ncpus, working_dir, num_harm=harmonic_sums)
