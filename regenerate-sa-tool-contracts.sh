#!/usr/bin/env bash
#
# For now, all the SA tools that can emit a TC can be done here.

TC_DIR=./pbsmrtpipe/registered_tool_contracts_sa3

REPORTS="\
sat
mapping_stats
mapping_stats_ccs
variants
top_variants
modifications
motifs
loading_xml
adapter_xml
filter_stats_xml
isoseq_classify
isoseq_cluster
ccs"

for REPORT in $REPORTS ; do
  MODULE="pbreports.report.${REPORT}"
  TC_FILE="pbreports.tasks.${REPORT}_tool_contract.json"
  echo "python -m ${MODULE} --emit-tool-contract > ${TC_DIR}/${TC_FILE}"
  python -m ${MODULE} --emit-tool-contract > ${TC_DIR}/${TC_FILE}
done

python -m pbreports.report.summarize_coverage.summarize_coverage --emit-tool-contract > $TC_DIR/pbreports.tasks.summarize_coverage_tool_contract.json

pbalign --emit-tool-contract > $TC_DIR/pbalign_tool_contract.json
python -m pbalign.ccs --emit-tool-contract > $TC_DIR/pbalign_ccs_tool_contract.json
python -m kineticsTools.ipdSummary --emit-tool-contract > $TC_DIR/kinetics_tools_ipdsummary_tool_contract.json
python -m kineticsTools.summarizeModifications --emit-tool-contract > $TC_DIR/kinetics_tools_summarize_modifications_tool_contract.json
variantCaller --emit-tool-contract > $TC_DIR/genomic_consensus.tasks.variantcaller_tool_contract.json
summarizeConsensus --emit-tool-contract > $TC_DIR/genomic_consensus.tasks.summarize_consensus_tool_contract.json
task_motifmaker_find --emit-tool-contract > $TC_DIR/motif_maker_find_tool_contract.json
task_motifmaker_reprocess --emit-tool-contract > $TC_DIR/motif_maker_reprocess_tool_contract.json
gffToBed --emit-tool-contract > $TC_DIR/genomic_consensus_gff2bed_tool_contract.json
gffToVcf --emit-tool-contract > $TC_DIR/genomic_consensus_gff2vcf_tool_contract.json
task_pbccs_ccs --emit-tool-contract > $TC_DIR/pbccs.tasks.ccs_tool_contract.json
task_pblaa_laa --emit-tool-contract > $TC_DIR/pblaa_laa_tool_contract.json

# XXX slightly automated below here...
PY_MODULES="\
pbtranscript.tasks.scatter_clusters
pbtranscript.tasks.scatter_contigset
pbtranscript.tasks.gather_nfl_pickle
pbtranscript.tasks.classify
pbtranscript.tasks.cluster
pbtranscript.tasks.ice_partial
pbtranscript.tasks.ice_quiver
pbtranscript.tasks.ice_quiver_postprocess"

for MODULE in ${PY_MODULES} ; do
  TC_FILE_PREFIX="`echo "${MODULE}" | tr 'A-Z' 'a-z'`"
  TC_FILE="$TC_DIR/${TC_FILE_PREFIX}_tool_contract.json"
  CMD="python -m ${MODULE} --emit-tool-contract > $TC_FILE"
  echo $CMD
  eval $CMD
done
