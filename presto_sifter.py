import numpy as np
import sys
import pandas as pd
import re

def extract_value_and_error(cell):
    try:
        # Match numbers, errors, and optional scientific notation
        match = re.match(r"([-]?[0-9.]+)\(([\d.]+)\)(x10\^(-?\d+))?", str(cell))
        if match:
            value, error, _, exponent = match.groups()
            
            # Convert to float or integer 
            value = float(value)
            error = float(error) if '.' in error else int(error)
            
            # Handle scientific notation, if present
            if exponent:
                multiplier = 10 ** int(exponent)
                value *= multiplier
                error *= multiplier

            return value, error

        # If no error term or scientific notation
        else:
            return float(cell), None

    except Exception as e:
        return str(cell), str(e)





def parse_accel_search_cand_list(filename, cand_num=1):
    '''
    Reads the cand.list of Accelsearch from presto and outputs spin_period and sigma of top candidate
    '''
    data = []
    count = 0
    with open(filename,'r') as f:
        for line in f:
            if line.startswith("-") or line.startswith("\n"):
                continue
            if line.startswith("Cand"):
                count+=1
                if count <= 1:
                    data.append(line)
                else:
                    break
            else:
                data.append(line)

    cands = data[:-1]
    real_cands = cands[2:]
    final_cands = []
    for line in real_cands:
        final_cands.append(line.split())
    df = pd.DataFrame(final_cands)
    df1 = df.loc[:, 0:10]
   
    df1.columns=['Cand','Sigma','Summed Power','Coherent Power', \
    'Harm','Period','Frequency','FFT', 'Freq Deriv','FFT z','Accel']

    df2 = df1.loc[df1['Cand'] == str(cand_num)]
    columns_to_split = ['Period', 'Frequency', 'FFT', 'Freq Deriv', 'FFT z', 'Accel']
    
    df2 = df2.copy()
    for col in columns_to_split:
        df2[col + '_value'], df2[col + '_error'] = zip(*df2[col].map(extract_value_and_error))
    
    freq = float(df2['Frequency_value'].iloc[0])
    sigma = float(df2['Sigma'].iloc[0])
    acc = float(df2['Accel_value'].iloc[0])
    acc_error = float(df2['Accel_error'].iloc[0])
    fd = float(df2['Freq Deriv_value'].iloc[0])
    fd_error = float(df2['Freq Deriv_error'].iloc[0])
    
    decimals = str(freq)[::-1].find('.')

    freq_err = int(df2['Frequency_error'].iloc[0])
    digits = len(str(freq_err))
    number_zeros = decimals - digits
    freq_err = float('0.' + '0'*number_zeros + str(freq_err))
    fdd = 0.0
   
 
    
    return freq, fd, fdd, acc, sigma

#jerk_filename = "/hercules/scratch/vishnu/SLURM_PULSARMINER/M30/20210122/cfbf00025/03_DEDISPERSION/M30_20210122_cfbf00025/full/ck00/M30_20210122_cfbf00025_full_ck00_DM24.55_ACCEL_200_JERK_600"
#accel_filename = "/hercules/scratch/vishnu/SLURM_PULSARMINER/NGC6397/20201105/cfbf00000/03_DEDISPERSION/NGC6397_20201105_cfbf00000/29m/ck03/NGC6397_20201105_cfbf00000_29_ck03_DM69.65_ACCEL_200"


#parse_accel_search_cand_list(accel_filename)