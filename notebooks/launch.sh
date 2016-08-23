#!/bin/bash

set +x
use_port=7777
echo "Running on port ${use_port}"
#ipython notebook --certfile=/home/UNIXHOME/mkocher/mycert.pem --profile=nbserver  --port=$use_port --no-browser --log-level 50
jupyter notebook --port=${use_port} --no-browser --log-level 50
