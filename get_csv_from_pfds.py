from prepfold import pfd
import argparse
import glob
import astropy.units as u
from astropy.coordinates import Angle
from astropy.coordinates import SkyCoord
from astropy.time import Time
from presto import chi2_sigma
import os, sys
import bestprof_utils
import pandas as pd
import ast


def get_args():
    arg_parser = argparse.ArgumentParser(
        description="A utility tool to obtain a csv file from presto candidates to view with CandyJar ")
    arg_parser.add_argument("-pfds", dest="pfds",
                            help="PFD files", required=True, nargs='+')
    arg_parser.add_argument(
        "-meta", dest="meta_path", help="apsuse.meta file of that observation")
    arg_parser.add_argument("-pointing_id", dest="pointing_id",
                            help="Add pointing id if known", default=0)
    arg_parser.add_argument("-beam_id", dest="beam_id",
                            help="Add beam id if known", default=0)
    arg_parser.add_argument("-beam_name", dest="beam_name",
                            help="Name of APSUSE beam Eg: cfbf00004", required=True)
    arg_parser.add_argument("-bary", dest="bary",
                            help="Get optimised barycentric values instead of topocentric", action="store_true")
    arg_parser.add_argument("-out", dest="outfile",
                            help="Output csv file", default="candidates.csv") 
    arg_parser.add_argument("-utc", dest="utc",
                            help="UTC of observation in ISOT format - default=start MJD")      
    arg_parser.add_argument("-filterbank_path", dest="filterbank_path",default=None,help="Path to filterbank file")
    arg_parser.add_argument("-verbose", dest="verbose",action="store_true",help="Verbose output")
    arg_parser.add_argument('-copy_ml_cands_only', action='store_true', default=False, help='Copy high scoring ML candidates only')

                                                     
    return arg_parser.parse_args()

def get_galactic_from_equatorial(ra_hms, dec_dms):
    ra = Angle(ra_hms, unit=u.hour)
    dec = Angle(dec_dms, unit=u.degree)
    coord = SkyCoord(ra=ra, dec=dec, frame='icrs')
    return coord.galactic.l.deg, coord.galactic.b.deg


def get_isot_from_mjd(mjd):
    return Time(mjd, format='mjd', scale='utc').isot


def convert_to_float(value):
    result = ast.literal_eval(value)
    if isinstance(result, list) or isinstance(result, tuple):
        return float(result[0])
    return float(result)


def p_to_f(p, pd, pdd=None):
    """
    p_to_f(p, pd, pdd=None):
       Convert period, period derivative and period second
       derivative to the equivalent frequency counterparts.
       Will also convert from f to p.
    """
    f = 1.0 / p
    fd = -pd / (p * p)
    if (pdd is None):
        return [f, fd]
    else:
        if (pdd == 0.0):
            fdd = 0.0
        else:
            fdd = 2.0 * pd * pd / (p ** 3.0) - pdd / (p * p)
        return [f, fd, fdd]

if __name__ == '__main__':
    header = "pointing_id,beam_id,beam_name,source_name,ra,dec,gl,gb,mjd_start,utc_start,f0_user,f0_opt,f0_opt_err,f1_user,f1_opt,f1_opt_err,acc_user,acc_opt,acc_opt_err,dm_user,dm_opt,dm_opt_err,sn_fft,sn_fold,pepoch,maxdm_ymw16,dist_ymw16,pics_trapum_ter5,pics_palfa,pics_meerkat_l_sband_combined_best_recall,pics_palfa_meerkat_l_sband_best_fscore,png_path,metafile_path,filterbank_path,candidate_tarball_path"
    args = get_args()
    candidate_dir = os.path.dirname(args.pfds[0])
    cluster = candidate_dir.split('/')[0]
    if args.copy_ml_cands_only:
        output_dir = 'CANDIDATE_VIEWER/' + cluster + '/' + args.utc.split('T')[0].replace('-', '') + '/' + 'ML_SELECTED/' + 'plots' + '/' 
        output_meta_dir = 'CANDIDATE_VIEWER/' + cluster + '/' + args.utc.split('T')[0].replace('-', '') + '/'  + 'ML_SELECTED/' + 'metafiles' + '/' 

    else:
        output_dir = 'CANDIDATE_VIEWER/' + cluster + '/' + args.utc.split('T')[0].replace('-', '') + '/' + 'EVERYTHING/' + 'plots'
        output_meta_dir = 'CANDIDATE_VIEWER/' + cluster + '/' + args.utc.split('T')[0].replace('-', '') + '/' + 'EVERYTHING/' + 'metafiles'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if not os.path.exists(output_meta_dir):
        os.makedirs(output_meta_dir)

    # Copy meta file if not already there
    if not os.path.exists(output_meta_dir + '/' + os.path.basename(args.meta_path)):
        os.system("cp {} {}".format(args.meta_path, output_meta_dir))

    pics_recall = pd.read_csv(candidate_dir + '/' + 'pics_MeerKAT_L_SBAND_COMBINED_Best_Recall.csv')
    pics_fscore = pd.read_csv(candidate_dir + '/' + 'pics_PALFA_MeerKAT_L_SBAND_Best_Fscore.csv')
   
    if args.verbose:
        print(args.pfds)
    print("{} pfd files for beam {}".format(len(args.pfds), args.beam_name))
    # Check if the file exists
    file_exists = os.path.exists(args.outfile)

    # Open the file in append mode if it exists, else open in write mode
    with open(args.outfile, 'a' if file_exists else 'w') as out:
        if not file_exists:
            out.write(header + "\n")

        for f in args.pfds:
            #pics_m_LS_recall = convert_to_float(pics_recall[pics_recall['filename'] == os.path.basename(f)]['MeerKAT_L_SBAND_COMBINED_Best_Recall'].values[0])
            pics_m_LS_recall = pics_recall[pics_recall['filename'] == os.path.basename(f)]['MeerKAT_L_SBAND_COMBINED_Best_Recall'].values[0]
            pics_pm_LS_fscore = pics_fscore[pics_fscore['filename'] == os.path.basename(f)]['PALFA_MeerKAT_L_SBAND_Best_Fscore'].values[0]
            #pics_pm_LS_fscore = convert_to_float(pics_fscore[pics_fscore['filename'] == os.path.basename(f)]['PALFA_MeerKAT_L_SBAND_Best_Fscore'].values[0])
            if args.copy_ml_cands_only:
                if pics_m_LS_recall < 0.1 and pics_pm_LS_fscore < 0.1:
                    continue
            
            
            bestprof = bestprof_utils.parse_bestprof(f + ".bestprof")
            try:
                pfd_file = pfd(f)
            except:
                print("Error reading pfd file {}".format(f))
                continue
            #pfd_file = pfd(f)
            pointing_id = args.pointing_id
            beam_id = args.beam_id
            beam_name = args.beam_name
            source_name = pfd_file.candnm
            ra = pfd_file.rastr
            dec = pfd_file.decstr
            gl, gb = get_galactic_from_equatorial(ra, dec)
            mjd_start = pfd_file.tepoch
            if (args.utc is None):
                utc_start = get_isot_from_mjd(mjd_start)
            else:
                utc_start = args.utc
            f0_user, f1_user, f2_user = p_to_f(
                pfd_file.curr_p1, pfd_file.curr_p2, pfd_file.curr_p3)
            acc_user = f1_user * 2.99792458e8 / f0_user

            if (args.bary):
                f0_opt, f1_opt, f2_opt = p_to_f(
                    pfd_file.bary_p1, pfd_file.bary_p3, pfd_file.bary_p3)
            else:
                f0_opt, f1_opt, f2_opt = p_to_f(
                    pfd_file.topo_p1, pfd_file.topo_p2, pfd_file.topo_p3)
            f0_opt_err = 0.0
            f1_opt_err = 0.0
            f2_opt_err = 0.0
            acc_opt =  f1_opt * 2.99792458e8 / f0_opt
            acc_opt_err = 0.0
            dm_user = pfd_file.bestdm
            dm_opt = pfd_file.bestdm
            dm_opt_err = 0.0
            sn_fft = 0

            sn_fold = float(bestprof['Sigma'])
            # sn_fold = pfd_file.chi2_sigma(pfd_file.calc_redchi2() *
            #                     pfd_file.DOFcor, int(pfd_file.DOFcor))

            pepoch = mjd_start
            maxdm_ymw16 = 0
            dist_ymw16 = 0
            pics_trapum_ter5 = 0
            pics_palfa = 0
            png_path = f + ".png"
            metafile_path = args.meta_path
            filterbank_path = args.filterbank_path
            candidate_tarball_path = None
            out.write("{},".format(pointing_id))
            out.write("{:d},".format(beam_id))
            out.write("{},".format(beam_name))
            out.write("{},".format(source_name))
            out.write("{},".format(ra))
            out.write("{},".format(dec))
            out.write("{},".format(gl))
            out.write("{},".format(gb))
            out.write("{:15.10f},".format(mjd_start))
            out.write("{},".format(utc_start.split(".")[0]))
            out.write("{:13.9f},".format(f0_user))
            out.write("{:13.9f},".format(f0_opt))
            out.write("{:13.9f},".format(f0_opt_err))
            out.write("{:13.9f},".format(f1_user))
            out.write("{:13.9f},".format(f1_opt))
            out.write("{:13.9f},".format(f1_opt_err))
            # out.write("{:13.9f},".format(f2_user))
            # out.write("{:13.9f},".format(f2_opt))
            # out.write("{:13.9f},".format(f2_opt_err))
            out.write("{:13.9f},".format(acc_user))
            out.write("{:13.9f},".format(acc_opt))
            out.write("{:13.9f},".format(acc_opt_err))
            out.write("{:13.9f},".format(dm_user))
            out.write("{:13.9f},".format(dm_opt))
            out.write("{:13.9f},".format(dm_opt_err))
            out.write("{:13.9f},".format(sn_fft))
            out.write("{:13.9f},".format(sn_fold))
            out.write("{:15.10f},".format(pepoch))
            out.write("{:13.9f},".format(maxdm_ymw16))
            out.write("{:13.9f},".format(dist_ymw16))
            out.write("{:f},".format(pics_trapum_ter5))
            out.write("{:f},".format(pics_palfa))
            out.write("{:f},".format(pics_m_LS_recall))
            out.write("{:f},".format(pics_pm_LS_fscore))
            beam_num = beam_name.replace("cfbf00", "")
            out.write("{},".format('plots'  + "/" +  os.path.basename(png_path)))
            out.write("{},".format(metafile_path))
            out.write("{},".format(filterbank_path))
            out.write("{}".format(candidate_tarball_path))
            if args.verbose:
                print(pointing_id, beam_id, beam_name, source_name, ra, dec, gl, gb, mjd_start, utc_start, f0_user, f1_user, f2_user, acc_user, f0_opt, f1_opt, f2_opt, f0_opt, f1_opt, f2_opt, f0_opt_err, f1_opt_err,
              f2_opt_err, acc_opt, acc_opt_err, dm_user, dm_opt, dm_opt_err, sn_fft, sn_fold, pepoch, maxdm_ymw16, dist_ymw16, pics_trapum_ter5, pics_palfa, png_path, metafile_path, filterbank_path, candidate_tarball_path)
            out.write("\n")
            out.flush()

            # Copy png file to output directory
            os.system("cp {} {}".format(png_path, output_dir))
        print(beam_name, "Done.")


