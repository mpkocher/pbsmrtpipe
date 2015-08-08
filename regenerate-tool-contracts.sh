#!/usr/bin/env bash +x

# This is for pbsmrtpipe (and subdependency, pbcommand) ONLY. DO NOT put random
# SA# tools in here. They should NOT be assumed to be installed.

TC_DIR=./pbsmrtpipe/pb_static_tasks
EMIT=" --emit-tool-contract "

python -m pbsmrtpipe.tools_dev.fasta $EMIT > $TC_DIR/dev_tools_fasta_tool_contract.json
python -m pbsmrtpipe.tools_dev.filter_fasta $EMIT > $TC_DIR/dev_tools_fasta_filter_tool_contract.json
python -m pbsmrtpipe.tools_dev.fasta_report $EMIT > $TC_DIR/dev_fasta_report_tool_contract.json

# Scatter/Chunk Tasks
python -m pbsmrtpipe.tools_dev.scatter_filter_fasta $EMIT > $TC_DIR/dev_scatter_filter_fasta_tool_contract.json

# Gather tasks
python -m pbsmrtpipe.tools_dev.gather_fasta $EMIT > $TC_DIR/dev_gather_fasta_tool_contract.json
python -m pbsmrtpipe.tools_dev.gather_fastq $EMIT > $TC_DIR/dev_gather_fastq_tool_contract.json
python -m pbsmrtpipe.tools_dev.gather_alignments $EMIT > $TC_DIR/dev_gather_alignments_tool_contract.json
python -m pbsmrtpipe.tools_dev.gather_subreads $EMIT > $TC_DIR/dev_gather_subreads_tool_contract.json
python -m pbsmrtpipe.tools_dev.gather_csv $EMIT > $TC_DIR/dev_gather_csv_tool_contract.json
python -m pbsmrtpipe.tools_dev.gather_gff $EMIT > $TC_DIR/dev_gather_gff_tool_contract.json


# pbcommand tasks, these are internal, so this might break if the
# version changes.

python -m pbcommand.cli.examples.dev_txt_app --emit-tool-contract > $TC_DIR/dev_pbcommand_example_tool_contract.json
python -m pbcommand.cli.examples.dev_app --emit-tool-contract > $TC_DIR/dev_pbcommand_example_dev_txt_app_tool_contract.json
python -m pbcommand.cli.examples.dev_gather_fasta_app --emit-tool-contract > $TC_DIR/dev_pbcommand_gather_fasta_app_tool_contract.json
python -m pbcommand.cli.examples.dev_scatter_fasta_app --emit-tool-contract > $TC_DIR/dev_pbcommand_scatter_fasta_app_tool_contract.json
