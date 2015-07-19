#!/usr/bin/env bash +x

TC_DIR=./pbsmrtpipe/pb_static_tasks/

python -m pbsmrtpipe.tools_dev.fasta --emit-tool-contract > $TC_DIR/dev_tools_fasta_tool_contract.json
python -m pbsmrtpipe.tools_dev.filter_fasta --emit-tool-contract > $TC_DIR/dev_tools_fasta_filter_tool_contract.json
python -m pbsmrtpipe.tools_dev.fasta_report --emit-tool-contract > $TC_DIR/dev_fasta_report_tool_contract.json
# pbcommand tasks, these are internal, so this might break if the
# version changes.
python -m pbcommand.cli.examples.dev_app --emit-tool-contract > $TC_DIR/dev_pbcommand_examples_dev_app_tool_contract.json
python -m pbcommand.cli.examples.dev_txt_app --emit-tool-contract > $TC_DIR/dev_pbcommand_examples_dev_txt_app_tool_contract.json