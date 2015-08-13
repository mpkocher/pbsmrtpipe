#!/usr/bin/env bash


TC_DIR=./pbsmrtpipe/registered_tool_contract_sa3/

# For now, all the SA tools that can emit a TC can be done here.
python -m pbreports.report.sat --emit-tool-contract > $TC_DIR/pbreports_report_sat_tool_contract.json
python -m pbreports.report.mapping_stats --emit-tool-contract > $TC_DIR/mapping_stats_tool_contract.json
python -m pbreports.report.mapping_stats_ccs --emit-tool-contract > $TC_DIR/mapping_stats_ccs_tool_contract.json
python -m pbreports.report.variants --emit-tool-contract > $TC_DIR/pbreports_report_variants_tool_contract.json
python -m pbreports.report.top_variants --emit-tool-contract > $TC_DIR/pbreports_report_top_variants_tool_contract.json
python -m pbreports.report.modifications --emit-tool-contract > $TC_DIR/pbreports_report_modifications_tool_contract.json
python -m pbreports.report.motifs --emit-tool-contract > $TC_DIR/pbreports_report_motifs_tool_contract.json
python -m pbreports.report.summarize_coverage.summarize_coverage --emit-tool-contract > $TC_DIR/pbreports_report_summarize_coverage_tool_contract.json
python -m pbreports.report.loading_xml --emit-tool-contract > $TC_DIR/pbreports_report_loading_xml_tool_contract.json
python -m pbreports.report.adapter_xml --emit-tool-contract > $TC_DIR/pbreports_report_adapter_xml_tool_contract.json
python -m pbreports.report.filter_stats_xml --emit-tool-contract > $TC_DIR/pbreports_report_filter_stats_xml_tool_contract.json
python -m pbreports.report.isoseq_classify --emit-tool-contract > $TC_DIR/pbreports_report_isoseq_classify_tool_contract.json
python -m pbreports.report.isoseq_cluster --emit-tool-contract > $TC_DIR/pbreports_report_isoseq_cluster_tool_contract.json
python -m pbreports.report.ccs --emit-tool-contract > $TC_DIR/pbreports_report_ccs_tool_contract.json

pbalign --emit-tool-contract > $TC_DIR/pbalign_tool_contract.json
python -m pbalign.ccs --emit-tool-contract > $TC_DIR/pbalign_ccs_tool_contract.json
python -m kineticsTools.ipdSummary --emit-tool-contract > $TC_DIR/kinetics_tools_ipdsummary_tool_contract.json
python -m kineticsTools.summarizeModifications --emit-tool-contract > $TC_DIR/kinetics_tools_summarize_modifications_tool_contract.json
variantCaller --emit-tool-contract > $TC_DIR/genomic_consensus_variantcaller_tool_contract.json
summarizeConsensus --emit-tool-contract > $TC_DIR/genomic_consensus_summarize_consensus_tool_contract.json
task_motifmaker_find --emit-tool-contract > $TC_DIR/motif_maker_find_tool_contract.json
task_motifmaker_reprocess --emit-tool-contract > $TC_DIR/motif_maker_reprocess_tool_contract.json
gffToBed --emit-tool-contract > $TC_DIR/genomic_consensus_gff2bed_tool_contract.json
gffToVcf --emit-tool-contract > $TC_DIR/genomic_consensus_gff2vcf_tool_contract.json
task_pbccs_ccs --emit-tool-contract > $TC_DIR/pbccs_ccs_tool_contract.json
