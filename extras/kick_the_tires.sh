#!/bin/bash

set -e
set -u
set -o pipefail

pipeline_id="pbsmrtpipe.pipelines.dev_local"
output_dir="/tmp/example-job"
job_stdout="${output_dir}/stdout"
job_stderr="${output_dir}/stderr"

# File use as input to the pipeline
entry_point="/tmp/entry-point.txt"
echo "Entry Point Mock File" > $entry_point

if [ -d "${output_dir}" ]; then
    rm -rf ${output_dir}
fi

mkdir -p $output_dir
cd $output_dir

echo "Pipeline ${pipeline_id} Summary"
pbsmrtpipe show-template-details $pipeline_id

echo "Running Pipeline ${pipeline_id} in ${output_dir}"
pbsmrtpipe pipeline-id $pipeline_id --debug \
	   -e "e_01:${entry_point}" \
	   --output-dir=$output_dir \
	   > >(tee ${job_stdout}) \
	   2> >(tee ${job_stderr})

echo "View the results in job directory ${output_dir}"
# need to forward the port 8000
#echo "cd ${output_dir}/html && python -m SimpleHTTPServer"
