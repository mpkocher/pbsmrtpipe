#!/usr/bin/env bash +x

TC_DIR=./pbsmrtpipe/pb_static_tasks/

python -m pbsmrtpipe.tools_dev.fasta --emit-tool-contract > $TC_DIR/dev_tools_fasta_tool_contract.json
python -m pbsmrtpipe.tools_dev.filter_fasta --emit-tool-contract > $TC_DIR/dev_tools_fasta_filter_tool_contract.json
python -m pbsmrtpipe.tools_dev.fasta_report --emit-tool-contract > $TC_DIR/dev_fasta_report_tool_contract.json
# pbcommand tasks, these are internal, so this might break if the
# version changes.
python -m pbcommand.cli.examples.dev_app --emit-tool-contract > $TC_DIR/dev_pbcommand_examples_dev_app_tool_contract.json
python -m pbcommand.cli.examples.dev_txt_app --emit-tool-contract > $TC_DIR/dev_pbcommand_examples_dev_txt_app_tool_contract.json

python -m pbreports.report.sat --emit-tool-contract > $TC_DIR/pbreports_report_sat_tool_contract.json
python -m pbreports.report.mapping_stats --emit-tool-contract > $TC_DIR/mapping_stats_tool_contract.json

ipdSummary.py --emit-tool-contract > $TC_DIR/kinetics_tools_ipdsummary_tool_contract.json
variantCaller --emit-tool-contract > $TC_DIR/genomic_consensus_variantcaller_tool_contract.json
motifMaker_find.py --emit-tool-contract > $TC_DIR/motif_maker_find_tool_contract.json
motifMaker_reprocess.py --emit-tool-contract > $TC_DIR/motif_maker_reprocess_tool_contract.json
