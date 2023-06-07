# **SLURM Pulsarminer: Streamlined Binary Pulsar Search Processing**

**SLURM Pulsarminer**, is an efficient and tailored solution to facilitate the operation of the Pulsarminer software on High-Performance Computing (HPC) clusters. 

This tool is currently compatibile with **Pulsarminer version 1.1.5 (08Jun2020)** and **PRESTO2**. It supports AccelSearch on CPU/GPU and Jerk Search on CPU. It requires a Singularity image with the necessary software installed.

## Why SLURM Pulsarminer?

Designed for those who need to process certain beams prior to others, **SLURM Pulsarminer** offers the ability to intelligently divide your dedispersion, jerk search, and folding trials for efficient parallel execution. For example, it significantly expedites the analysis of the central beam in a Globular Cluster observation before other beams.

Many of the core routines are predominantly sourced from **PRESTO** or **Pulsarminer**.

## Getting Started

Follow the steps below to initiate **SLURM Pulsarminer**:

1. Modify your standard Pulsarminer config file. For reference, view the example in "sample_M30.config".

2. Adjust the "slurm_config.cfg" to match your cluster's specifications. Remember to include the absolute path of your Singularity image and specific mount paths for your data.

3. Execute the SLURM Pulsarminer launcher script with your Pulsarminer config file and your observation file. 

   Note: It's advised to execute this script within a tmux or screen session for uninterrupted operations since you will be launching hundreds to thousands of job per observation depending on your search range.

## LAUNCH_SLURM_PULSARMINER.sh Usage Guide

Fill the SLURM config file carefully, reflecting your system's specifications.

### Usage:

```bash
./LAUNCH_SLURM_PULSARMINER.sh [-h] [-m max_slurm_jobs] [-o observation_file] [-p config_file] [-t tmp_directory]
```

### Options:

- **-h**          Displays this help message and exits
- **-m NUM**      Sets the maximum number of SLURM jobs to submit at a time (default: five less than your max jobs you can submit)
- **-o FILE**     Defines the observation file to process
- **-p FILE**     Specifies the Pulsarminer configuration file to use
- **-t DIR**      Indicates the temporary directory (default: /tmp)

## Additional Resources

Should you prefer using the original repositories of PULSARMINER or PRESTO, find them here:

- [PULSARMINER](https://github.com/alex88ridolfi/PULSAR_MINER)
- [PRESTO](https://github.com/scottransom/presto)


Happy processing!
