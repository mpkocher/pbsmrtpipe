#!/bin/bash

# Contrived example

# Source my python virtualenv with pbsmrtpipe
source /path/to/my-ve/bin/activate

# Use a custom version of blasr
MY_BLASR_BIN=/path/to/my-blasr/bin

# Make Sure qsub is in path to run jobs on cluster for SGE
export SGE_ROOT=/path/to/sge-root

# this must be consistent with the SGE_ROOT
SGE_BIN=/mnt/software/g/gridengine/6.2u5/usr/bin

# Now pbsmrtpipe will be able to find blasr, qsub
export PATH=$MY_BLASR_BIN:$SGE_BIN:$PATH