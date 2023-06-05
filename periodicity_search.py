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

def periodicity_search(data, zmax, wmax, ncpus, working_dir, num_harm=8, remove_fft_files=True, remove_dat_files=True, accel_search_gpu_flag=True):
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
        if accel_search_gpu_flag:
            original_ld_library_path = os.environ.get('LD_LIBRARY_PATH')
            original_path = os.environ.get('PATH')
            os.environ['LD_LIBRARY_PATH'] = "/usr/local/cuda/lib64:/usr/lib:/.singularity.d/libs:/software/presto2_on_gpu/lib"
            os.environ['PATH'] = "/usr/local/nvidia/bin:/usr/local/cuda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/::/software//psrcat_tar:/software//tempo/bin:/software/presto2_on_gpu/bin/"
            cuda_id = 0
            ncpus = 1
            print('Running Accel Search on %s using GPU %d' % (data, cuda_id))
            accel_search_cmd = 'accelsearch -cuda %d -ncpus %d -numharm %d -zmax %d %s' % (cuda_id, ncpus, num_harm, zmax, data)
            subprocess.check_output(accel_search_cmd, shell=True)
            os.environ['LD_LIBRARY_PATH'] = original_ld_library_path
            os.environ['PATH'] = original_path
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
    parser.add_argument("-g", "--gpu_flag", dest="gpu_flag", action='store_false', default=True, help="If you set this flag, code will disable GPU for accelsearch. Default is True.")

    args = parser.parse_args()

    data = args.input
    ncpus = args.ncpus
    zmax = args.zmax
    wmax = args.wmax
    harmonic_sums = args.harmonic_sums
    gpu_flag = args.gpu_flag
    basename = os.path.basename(data)[:-4]
    cluster = basename.split('_')[0]
    epoch = basename.split('_')[1]
    beam = basename.split('_')[2]
 
    working_dir = args.tmp_working_dir
    mkdir_p(working_dir)
    
    periodicity_search(data, zmax, wmax, ncpus, working_dir, num_harm=harmonic_sums, accel_search_gpu_flag=gpu_flag)
