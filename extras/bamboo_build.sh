#!/bin/bash -ex
#
# Build script for Bamboo jobs - assumes that dependencies have been checked
# out into a 'repos' subdirectory
#
SCRIPT=`readlink -f "$0"`
MYDIR=`dirname "$SCRIPT"`
# cd into base pbsmrtpipe directory
cd $MYDIR/..

mkdir -p tmp
/opt/python-2.7.9/bin/python /mnt/software/v/virtualenv/13.0.1/virtualenv.py tmp/venv
source tmp/venv/bin/activate

(cd repos/pbcommand && make install)
pip install -r REQUIREMENTS_CI.txt
pip install -r REQUIREMENTS.txt
(cd repos/pbcore && make install)
(cd repos/pbcoretools && make install)

python setup.py install
make test-suite
