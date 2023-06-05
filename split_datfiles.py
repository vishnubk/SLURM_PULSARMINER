import subprocess
import sys, os, argparse, errno
import numpy as np
from pulsar_miner import Inffile

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def split_dat_file_into_chunks(datfile, output_datfile, numout, working_dir, start_fraction=0.0, remove_dat_files=False):
    os.chdir(working_dir)
   

    split_cmd = 'prepdata -nobary -dm 0 -start %.2f -numout %d -o %s %s' % (start_fraction, numout, output_datfile_name, datfile_name)
    subprocess.check_output(split_cmd, shell=True)
    #print(split_cmd) 
    
    if remove_dat_files:
        os.remove('%s.dat' % datfile)
    

    
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Dedisperse a filterbank file using PRESTO and multiple CPU cores')
    parser.add_argument('-i', '--input', help='Input Data (Presto *.dat file)', required=True)
    parser.add_argument('-s', '--segment_label', help='Segment label (full,28m,14m)', type=str, default='full')
    parser.add_argument('-c', '--chunk_label', help='Chunk label (Pulsar miner convention)', type=str, default='ck00')
    parser.add_argument('-w', '--working_dir', help='Working dir where do the processing', type=str, default='/tmp')

    args = parser.parse_args()
    working_dir = args.working_dir
    os.chdir(working_dir)
    
    datfile_name = args.input
    segment_label = args.segment_label
    chunk_label = args.chunk_label
    
    basename = os.path.basename(datfile_name)[:-4]
    cluster = basename.split('_')[0]
    epoch = basename.split('_')[1]
    beam = basename.split('_')[2]
    dm = basename.split('_')[-1].replace('DM', '')
    obs_tag = '%s_%s_%s' % (cluster, epoch, beam)
    output_datfile_name = '%s_%s_%s_DM%s' % (obs_tag,segment_label, chunk_label, dm)
    inffile_name = datfile_name.replace(".dat", ".inf")
    inffile = Inffile(inffile_name)
    tsamp_s = inffile.tsamp_s
    nsamples = inffile.nsamples
    tobs = tsamp_s * nsamples
    
    
   
    #working_dir = os.path.join(working_dir, 'dedisp_%s_%s_DM_%s' % (segment_label, chunk_label, dm))
    
    #mkdir_p(working_dir)
    
    
    if segment_label == 'full': 
        start_fraction = 0
        numout = nsamples
        if numout %2 != 0:
            numout -= 1 
    else:
        required_tobs = float(segment_label.replace('m', '')) * 60
        fraction = float(required_tobs / tobs)
        numout = int(nsamples * fraction)
    
        if numout %2 != 0:
            numout -= 1 
        start_fraction = int(chunk_label.replace('ck', '')) * fraction
        
    
    split_dat_file_into_chunks(datfile_name, output_datfile_name, numout, working_dir, start_fraction) 
