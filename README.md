# **A Slurm Wrapper Around PULSARMINER/PRESTO.**


This is a Slurm Wrapper around PULSARMINER/PRESTO built to run on HPC Clusters. 



**SLURM_PULSARMINER is currently compatible with pulsarminer version 1.1.5 (08Jun2020) and PRESTO2. It can currently run till sifting. Folding Pulsar Candidates functionality is yet to be added.**


**This software is written primary to run PULSARMINER on HPC clusters that use slurm and singularity. This maybe useful to you if priortising the processing of certain beams first is more important. For example if you would like to get the jerk search results of the central beam of a Globular Cluster observation before you process other beams, then this setup will significantly speed up your analysis.** 

Most of the routines have been imported from either PRESTO or PULSARMINER. This maybe useful to you if you would like to split all your dedispersion, acceleration/jerk search trials across several cores in your cluster.

1. Edit your standard PULSARMINER config file. An example is shown in "sample_M30.config". 
2. Now edit the "slurm_config.cfg" based on the specifications of your cluster and add the absolute path of your singularity image, along with any specific mount paths for your data.
3. Run create_slurm_jobs.py (ideally inside a screen window). This will first run rfifind, and create birdies on the head/log-in node and then create a slurm job script file that you can then use to launch jobs to your cluster. For example: slurm_jobs_M30_20220901_cfbf00150.sh.
4. Run that using source slurm_jobs_M30_20220901_cfbf00150.sh


If you would like to use the original repo instead. Here are the links.


PULSARMINER: https://github.com/alex88ridolfi/PULSAR_MINER 


PRESTO: https://github.com/scottransom/presto
