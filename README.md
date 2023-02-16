# **A Slurm Wrapper Around PULSARMINER/PRESTO.**


SLURM_PULSARMINER is currently compatible with pulsarminer version 1.1.5 (08Jun2020) and PRESTO2.


**This software is written primary to run PULSARMINER on HPC clusters that use slurm and singularity. This maybe useful to you if priortising the processing of certain beams first is more important. For example if you would like to get the jerk search results of the central beam of a Globular Cluster observation before you process other beams, then this setup will significantly speed up your analysis. The software intelligently splits up your dedispersion, jerk search and folding trials efficiently to run them in parallel.**

Most of the routines have been imported from either PRESTO or PULSARMINER.   

Instructions on using this software:

1. Edit your standard PULSARMINER config file. An example is shown in "sample_M30.config". 
2. Now edit the "slurm_config.cfg" based on the specifications of your cluster and add the absolute path of your singularity image, along with any specific mount paths for your data.
3. Start the SLURM PULSARMINER launcher script with your pulsarminer config file and your observation file. Example given below. **I highly recommend running this launcher script inside a tmux or screen session**. 





Help Function for LAUNCH_SLURM_PULSARMINER.sh

**SLURM PULSARMINER Launch Script. With Great Parallelization Comes Great Responsibility. Ensure that you fill the slurm config file with the correct values for your system.**

Usage: ./LAUNCH_SLURM_PULSARMINER.sh [-h] [-o observation_file] [-p config_file] [-t tmp_directory]

Options:
  -h            Show this help message and exit
  -o FILE       Observation file to process
  -p FILE       Configuration file
  -t DIR        Temporary directory (default is /tmp)


If you would like to use the original repo of PULSARMINER or PRESTO instead, you can find the links below.


PULSARMINER: https://github.com/alex88ridolfi/PULSAR_MINER 


PRESTO: https://github.com/scottransom/presto
