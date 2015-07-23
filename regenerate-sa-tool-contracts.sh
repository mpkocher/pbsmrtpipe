#!/usr/bin/env bash


TC_DIR=./pbsmrtpipe/pb_static_tasks/

# For now, all the SA tools that can emit a TC can be done here.
python -m pbreports.report.sat --emit-tool-contract > $TC_DIR/pbreports_report_sat_tool_contract.json
python -m pbreports.report.mapping_stats --emit-tool-contract > $TC_DIR/mapping_stats_tool_contract.json
python -m pbreports.report.variants --emit-tool-contract > $TC_DIR/pbreports_report_variants_tool_contract.json
python -m pbreports.report.top_variants --emit-tool-contract > $TC_DIR/pbreports_report_top_variants_tool_contract.json

pbalign --emit-tool-contract > $TC_DIR/pbalign_tool_contract.json
ipdSummary.py --emit-tool-contract > $TC_DIR/kinetics_tools_ipdsummary_tool_contract.json
variantCaller --emit-tool-contract > $TC_DIR/genomic_consensus_variantcaller_tool_contract.json
motifMaker_find.py --emit-tool-contract > $TC_DIR/motif_maker_find_tool_contract.json
motifMaker_reprocess.py --emit-tool-contract > $TC_DIR/motif_maker_reprocess_tool_contract.json
gffToBed --emit-tool-contract > $TC_DIR/genomic_consensus_gff2bed_tool_contract.json
gffToVcf --emit-tool-contract > $TC_DIR/genomic_consensus_gff2vcf_tool_contract.json
