#!/bin/bash

# Install the cpgdata package after installing Rust and Cargo
python -m pip install cpgdata

# Sync the cpg index to the specified directory
cpg index sync -o "../1_snakemake/inputs/cpg_index"