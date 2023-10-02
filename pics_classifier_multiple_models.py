#!/usr/bin/env python2.7

import os, sys
import glob
import cPickle
import argparse
import pandas as pd
sys.path.append('/home/psr')
from ubc_AI.data import pfdreader
import subprocess, errno

def score_file(filename, classifier_model):
    try:
        return classifier_model.report_score([pfdreader(filename)])
    except Exception as e:
        return 0

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise



def run_pics_parallel(filenames, pics_model, model_name):
    classifier_model = cPickle.load(open(pics_model,'rb'))
    AI_scores = classifier_model.report_score([pfdreader(f) for f in filenames])
    df = pd.DataFrame({'filename': filenames, model_name: AI_scores})
    return df

def run_pics_sequential(filenames, pics_model, model_name):
    classifier_model = cPickle.load(open(pics_model, 'rb'))
    AI_scores = []
    for f in filenames:
        try:
            score = classifier_model.report_score([pfdreader(f)])
            AI_scores.append(score)
        except Exception as e:
            AI_scores.append(0)
    df = pd.DataFrame({'filename': filenames, model_name: AI_scores})
    return df


def main(input_dir, pics_model_dir):
    os.chdir(input_dir)
    filenames = glob.glob('*.pfd') + glob.glob('*.ar') + glob.glob('*.ar2')
    master_df = pd.DataFrame({'filename': filenames})
    models = glob.glob(pics_model_dir + '/' + '*.pkl')
   

    
    for pics_model in models:
       
        model_name = os.path.splitext(os.path.basename(pics_model))[0]
        try:
            df = run_pics_parallel(filenames, pics_model, model_name)
        except Exception as e:
            df = run_pics_sequential(filenames, pics_model, model_name)
        
        # Sort individual model DataFrame
        df = df.sort_values(by=[model_name], ascending=False)
        
        # Merge into the master DataFrame
        master_df = pd.merge(master_df, df, on='filename', how='left')
        
    # Save the master DataFrame
    master_df.to_csv('pics_scores.csv', index=False)

    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process a directory of files and output to a CSV file.')
    parser.add_argument('-i', '--input_dir', required=True, help='Input dir of files to process')
    parser.add_argument('-m', '--model_dir', required=True, help='PICS model directory used to score')


    args = parser.parse_args()
    input_dir = args.input_dir
    pics_model_dir = args.model_dir
    
    
    main(input_dir, pics_model_dir)
