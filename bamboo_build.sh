#!/bin/bash -ex

source /mnt/software/Modules/current/init/bash
module load python/2.7.9

mkdir -p tmp
python /mnt/software/v/virtualenv/13.0.1/virtualenv.py tmp/venv
source tmp/venv/bin/activate
export PIP_CACHE_DIR=$PWD/.pip_cache
find $PIP_CACHE_DIR -name '*-linux_x86_64.whl' -delete || true

(cd repos/pbcommand && make install)
pip install -r REQUIREMENTS_CI.txt
pip install -r REQUIREMENTS.txt
(cd repos/pbcore && make install)
(cd repos/pbcoretools && make install)

python setup.py install
make test-suite
