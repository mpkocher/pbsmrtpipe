#!/usr/bin/env python

"""
Specifies pipeline datastore view rules and generates JSON files for building
into smrtlink.
"""

import argparse
import logging
import os.path as op
import os
import sys

from pbcommand.models import (FileTypes, DataStoreViewRule,
                              PipelineDataStoreViewRules)

from pbsmrtpipe.loader import load_all


log = logging.getLogger(__name__)

REGISTERED_VIEW_RULES = {}
RTASKS, _, _, PIPELINES = load_all()


def _to_view_rule(args):
    return DataStoreViewRule(*args)


def __to_view_rule_triples(rules, is_hidden):
    assert all([len(rule)==2 for rule in rules])
    return [(rule[0], rule[1], is_hidden) for rule in rules]


def _to_whitelist(rules):
    """
    Shorthand for whitelisting files given just the source IDs and file types.
    Not suitable for cases where we need to override the label or description.
    """
    return __to_view_rule_triples(rules, is_hidden=False)


def _to_blacklist(rules):
    """
    Shorthand for blacklisting files given just the source IDs and file types.
    Not suitable for cases where we need to override the label or description.
    """
    return __to_view_rule_triples(rules, is_hidden=True)


def load_pipeline_view_rules(registered_views_d, pipeline_id, smrtlink_version,
                             view_rules):
    """
    Generate a complete set of view rules for output files in the specified
    pipeline.  This will use the built-in pipeline templates and registered
    tool contracts to automatically populate the rules with default
    blacklisting for any file that isn't explicitly whitelisted in the input
    view_rules list.

    :param registered_views_d: dictionary of pipeline datastore view rules,
                               keyed by pipeline id
    :param pipeline_id: string id, e.g. 'pbsmrtpipe.pipelines.sa3_sat'
    :param smrtlink_version: release version associated with the rule
    :param view_rules: list of explicit rules as tuples of at least 3 elements
    """
    pipeline_id_ = "pbsmrtpipe.pipelines.{p}".format(
        p=pipeline_id.split(".")[-1])
    view_rules_ = [_to_view_rule(r) for r in view_rules]
    explicit_rules = {r.source_id:r for r in view_rules_}
    pipeline = PIPELINES[pipeline_id_]
    for (bound_input, bound_output) in pipeline.all_bindings:
        out_ids = bound_output.split(":")
        task_id = out_ids[0]
        for i_file, file_type in enumerate(RTASKS[task_id].output_types):
            source_id = "{t}-out-{i}".format(t=task_id, i=i_file)
            if source_id in explicit_rules:
                rule_file_type_id = explicit_rules[source_id].file_type_id
                if rule_file_type_id != file_type.file_type_id:
                    log.warn("File type mismatch for %s: %s vs. %s",
                             source_id, rule_file_type_id,
                             file_type.file_type_id)
            else:
                log.info("Using default rule for %s in %s", source_id,
                         pipeline_id_)
                rule = DataStoreViewRule(source_id, file_type.file_type_id,
                                         True)
                view_rules_.append(rule)
    pvr = PipelineDataStoreViewRules(
        pipeline_id=pipeline_id_,
        smrtlink_version=smrtlink_version,
        rules=view_rules_)
    registered_views_d[pipeline_id_] = pvr
    return registered_views_d


def register_pipeline_rules(pipeline_id, smrtlink_version):
    """
    Decorator function which processes view rules for storage in the global
    dict.
    """
    def deco_wrapper(func):
        if pipeline_id in REGISTERED_VIEW_RULES:
            log.warn("'{i}' already has view rules defined".format(
                     i=pipeline_id))
        rules = func()
        load_pipeline_view_rules(REGISTERED_VIEW_RULES, pipeline_id,
                                 smrtlink_version, rules)

        def wrapper(*args, **kwds):
            return func(*args, **kwds)
    return deco_wrapper


def set_default_view_rules():
    """
    Any pipeline that is visible to customers but doesn't have view rules set
    here gets the default blacklist.  (In practice, we probably shouldn't let
    this happen.)
    """
    for pipeline_id, pipeline in PIPELINES.iteritems():
        if not "dev" in pipeline.tags and not "internal" in pipeline.tags:
            if not pipeline_id in REGISTERED_VIEW_RULES:
                log.warn("%s does not have view rules, using defaults",
                         pipeline_id)
                load_pipeline_view_rules(REGISTERED_VIEW_RULES, pipeline_id,
                                         "5.0", [])


def _mapping_report_rules():
    return [
        ("pbreports.tasks.mapping_stats-out-0", FileTypes.REPORT, True),
        ("pbreports.tasks.coverage_report-out-0", FileTypes.REPORT, True)
    ]


def _variant_report_rules():
    return [
        ("pbreports.tasks.variants_report-out-0", FileTypes.REPORT, True),
        ("pbreports.tasks.top_variants-out-0", FileTypes.REPORT, True)
    ]

def _basemod_report_rules():
    return [
        ("pbreports.tasks.summarize_coverage-out-0", FileTypes.GFF, True),
        ("kinetics_tools.tasks.summarize_modifications-out-0", FileTypes.GFF, True)
]

def _isoseq_report_rules():
    return [
        ("pbtranscript.tasks.classify-out-3", FileTypes.JSON, True),
        ("pbreports.tasks.isoseq_classify-out-0", FileTypes.REPORT, True)
    ]


def _laa_report_rules():
    return [
        ("pbreports.tasks.amplicon_analysis_input-out-0", FileTypes.REPORT, True),
        ("pbreports.tasks.amplicon_analysis_consensus-out-0", FileTypes.REPORT, True),
        ("pbcoretools.tasks.split_laa_fastq-out-0", FileTypes.GZIP, False, "Consensus Sequences (FASTQ)"),
        ("pbcoretools.tasks.split_laa_fastq-out-1", FileTypes.GZIP, False, "Chimeric/Noise Consensus Sequences (FASTQ)")
    ]


def _barcode_report_rules():
    return [
        ("pbreports.tasks.barcode_report-out-0", FileTypes.REPORT, True)
    ]


def _ccs_report_rules():
    return [
        ("pbreports.tasks.ccs_report-out-0", FileTypes.REPORT, True)
    ]


def _pbalign_alignmentset_rules():
    return [
        ("pbalign.tasks.pbalign-out-0", FileTypes.DS_ALIGN, True)
    ]


@register_pipeline_rules("sa3_ds_barcode", "3.2")
def barcode_view_rules():
    return _barcode_report_rules()


@register_pipeline_rules("sa3_ds_ccs", "3.2")
def ccs_view_rules():
    return _ccs_report_rules()


@register_pipeline_rules("sa3_ds_barcode_ccs", "3.2")
def ccs_barcoding_view_rules():
    return _ccs_report_rules() + _barcode_report_rules()


@register_pipeline_rules("sa3_ds_ccs_align", "3.2")
def ccs_mapping_view_rules():
    return _ccs_report_rules() + _mapping_report_rules() + [
        ("pbreports.tasks.mapping_stats_ccs-out-0", FileTypes.REPORT, True)
    ]


@register_pipeline_rules("ds_modification_detection", "3.2")
def basemod_view_rules():
    return _basemod_report_rules() + _mapping_report_rules() + _pbalign_alignmentset_rules() + [
        ("pbreports.tasks.modifications_report-out-0", FileTypes.REPORT, True)
    ]


@register_pipeline_rules("ds_modification_motif_analysis", "3.2")
def basemod_and_motif_view_rules():
    return _basemod_report_rules() + _mapping_report_rules() + _pbalign_alignmentset_rules() + [
        ("pbreports.tasks.modifications_report-out-0", FileTypes.REPORT, True),
        ("pbreports.tasks.motifs_report-out-0", FileTypes.REPORT, True),
        ("kinetics_tools.tasks.ipd_summary-out-0", FileTypes.GFF, True)
    ]


@register_pipeline_rules("polished_falcon_fat", "3.2")
def hgap4_view_rules():
    whitelist = _to_whitelist([
        ("falcon_ns.tasks.task_falcon0_run_merge_consensus_jobs-out-1", FileTypes.JSON),
        ("falcon_ns.tasks.task_falcon0_run_merge_consensus_jobs-out-2", FileTypes.JSON),
        ("falcon_ns.tasks.task_falcon0_merge-out-0", FileTypes.TXT),
        ("falcon_ns.tasks.task_falcon0_cons-out-0", FileTypes.TXT),
        ("falcon_ns.tasks.task_falcon1_run_merge_consensus_jobs-out-1", FileTypes.JSON),
        ("falcon_ns.tasks.task_falcon1_run_merge_consensus_jobs-out-2", FileTypes.JSON),
        ("falcon_ns.tasks.task_falcon1_merge-out-0", FileTypes.TXT),
        ("falcon_ns.tasks.task_falcon1_db2falcon-out-0", FileTypes.TXT)
    ])
    blacklist = _to_blacklist([
        ("pbcoretools.tasks.bam2fasta-out-0", FileTypes.FASTA),
        ("pbcoretools.tasks.fasta2fofn-out-0", FileTypes.FOFN),
        ("falcon_ns.tasks.task_falcon_make_fofn_abs-out-0", FileTypes.FOFN),
        ("falcon_ns.tasks.task_falcon0_build_rdb-out-0", FileTypes.TXT),
        ("falcon_ns.tasks.task_falcon0_build_rdb-out-1", FileTypes.TXT),
        ("genomic_consensus.tasks.variantcaller-out-0", FileTypes.GFF),
        ("genomic_consensus.tasks.variantcaller-out-1", FileTypes.VCF),
        ("genomic_consensus.tasks.gff2bed-out-0", FileTypes.BED),
        ("falcon_ns.tasks.task_falcon1_build_pdb-out-0", FileTypes.TXT),
        ("falcon_ns.tasks.task_falcon0_run_daligner_jobs-out-0", FileTypes.FOFN),
        ("falcon_ns.tasks.task_falcon0_run_merge_consensus_jobs-out-0", FileTypes.FOFN),
        ("falcon_ns.tasks.task_falcon1_run_daligner_jobs-out-0", FileTypes.FOFN),
        ("falcon_ns.tasks.task_falcon1_run_merge_consensus_jobs-out-0", FileTypes.FOFN),
        ("falcon_ns.tasks.task_falcon2_run_asm-out-0", FileTypes.FASTA),
        ("falcon_ns.tasks.task_falcon_gen_config-out-0", FileTypes.CFG),
        ("falcon_ns.tasks.task_falcon0_build_rdb-out-2", FileTypes.TXT),
        ("falcon_ns.tasks.task_falcon_config-out-0", FileTypes.JSON),
        ("pbreports.tasks.polished_assembly-out-0", FileTypes.REPORT),
        ("falcon_ns.tasks.task_report_preassembly_yield-out-0", FileTypes.REPORT),
        ("falcon_ns.tasks.task_falcon0_run_merge_consensus_jobs-out-2", FileTypes.JSON),
        ("falcon_ns.tasks.task_falcon0_run_merge_consensus_jobs-out-1", FileTypes.JSON),
        ("falcon_ns.tasks.task_falcon1_run_merge_consensus_jobs-out-1", FileTypes.JSON),
        ("falcon_ns.tasks.task_falcon1_run_merge_consensus_jobs-out-2", FileTypes.JSON),
        ("falcon_ns.tasks.task_falcon0_merge-out-0", FileTypes.TXT),
        ("falcon_ns.tasks.task_falcon0_cons-out-0", FileTypes.TXT),
        ("falcon_ns.tasks.task_falcon1_merge-out-0", FileTypes.TXT),
        ("falcon_ns.tasks.task_falcon1_db2falcon-out-0", FileTypes.TXT),
        ("falcon_ns.tasks.task_falcon0_rm_las-out-0", FileTypes.TXT),
        ("falcon_ns.tasks.task_falcon1_rm_las-out-0", FileTypes.TXT),
        ("falcon_ns.tasks.task_falcon2_rm_las-out-0", FileTypes.TXT)
    ])
    return blacklist + whitelist + [
        ("pbcoretools.tasks.contigset2fasta-out-0", FileTypes.FASTA, False, "Polished Assembly"),
        ("genomic_consensus.tasks.variantcaller-out-2", FileTypes.DS_CONTIG, False, "Polished Assembly"),
        ("genomic_consensus.tasks.variantcaller-out-3", FileTypes.FASTQ, False, "Polished Assembly"),
        ("pbcoretools.tasks.fasta2referenceset-out-0", FileTypes.DS_REF, False, "Draft Assembly")
    ] + _mapping_report_rules()


@register_pipeline_rules("hgap_fat", "3.2")
def hgap5_view_rules():
    return _to_blacklist([
        ("falcon_ns.tasks.task_hgap_prepare-out-0", FileTypes.JSON),
        ("falcon_ns.tasks.task_hgap_prepare-out-1", FileTypes.JSON),
        ("falcon_ns.tasks.task_hgap_prepare-out-2", FileTypes.LOG),
        ("falcon_ns.tasks.task_hgap_run-out-1", FileTypes.REPORT),
        ("falcon_ns.tasks.task_hgap_run-out-2", FileTypes.REPORT),
        ("falcon_ns.tasks.task_hgap_run-out-3", FileTypes.LOG)
    ])


def _isoseq_view_rules():
    whitelist = _to_whitelist([
        ("pbtranscript.tasks.combine_cluster_bins-out-0", FileTypes.FASTA),
        ("pbtranscript.tasks.combine_cluster_bins-out-2", FileTypes.CSV),
        ("pbtranscript.tasks.combine_cluster_bins-out-3", FileTypes.DS_CONTIG),
        ("pbtranscript.tasks.combine_cluster_bins-out-4", FileTypes.FASTQ),
        ("pbtranscript.tasks.combine_cluster_bins-out-5", FileTypes.DS_CONTIG),
        ("pbtranscript.tasks.combine_cluster_bins-out-6", FileTypes.FASTQ),
    ])
    blacklist = _to_blacklist([
        ("pbtranscript.tasks.separate_flnc-out-0", FileTypes.PICKLE),
        ("pbtranscript.tasks.create_chunks-out-0", FileTypes.PICKLE),
        ("pbtranscript.tasks.create_chunks-out-1", FileTypes.PICKLE),
        ("pbtranscript.tasks.create_chunks-out-2", FileTypes.PICKLE),
        ("pbtranscript.tasks.combine_cluster_bins-out-1", FileTypes.JSON),
        ("pbtranscript.tasks.combine_cluster_bins-out-7", FileTypes.PICKLE),
        ("pbtranscript.tasks.gather_ice_partial_cluster_bins_pickle-out-0",
         FileTypes.TXT),
        ("pbtranscript.tasks.cluster_bins-out-0", FileTypes.TXT),
        ("pbtranscript.tasks.ice_partial_cluster_bins-out-0", FileTypes.TXT),
        ("pbtranscript.tasks.ice_polish_cluster_bins-out-0", FileTypes.TXT),
        ("pbtranscript.tasks.gather_polished_isoforms_in_each_bin-out-0",
         FileTypes.TXT),
        ("pbtranscript.tasks.ice_cleanup-out-0", FileTypes.TXT),
        ("pbreports.tasks.isoseq_cluster-out-0", FileTypes.REPORT)
    ])
    return _isoseq_report_rules() + _ccs_report_rules() + blacklist + whitelist


@register_pipeline_rules("sa3_ds_isoseq", "3.2")
def isoseq_view_rules():
    return _isoseq_view_rules()


@register_pipeline_rules("sa3_ds_isoseq_with_genome", "3.2")
def isoseq_with_genome_view_rules():
    """View rules for isoseq with genome."""
    blacklist = _to_blacklist([
        ("pbtranscript.tasks.scatter_contigset_gmap-out-0", FileTypes.CHUNK)
    ])
    whitelist = _to_whitelist([
        ("pbtranscript.tasks.map_isoforms_to_genome-out-0", FileTypes.SAM),
        ("pbtranscript.tasks.post_mapping_to_genome-out-0", FileTypes.FASTQ),
        ("pbtranscript.tasks.post_mapping_to_genome-out-1", FileTypes.GFF),
        ("pbtranscript.tasks.post_mapping_to_genome-out-2", FileTypes.TXT),
        ("pbtranscript.tasks.post_mapping_to_genome-out-3", FileTypes.TXT),
        ("pbtranscript.tasks.post_mapping_to_genome-out-4", FileTypes.TXT)
    ])
    return _isoseq_view_rules() + blacklist + whitelist


@register_pipeline_rules("sa3_ds_isoseq_classify", "3.2")
def isoseq_classify_view_rules():
    return _isoseq_report_rules() + _ccs_report_rules()


@register_pipeline_rules("sa3_ds_laa", "3.2")
def laa_view_rules():
    r1 = [
        ("pblaa.tasks.laa-out-3", FileTypes.CSV, True)
    ]
    return r1 + _laa_report_rules()


@register_pipeline_rules("sa3_ds_barcode_laa", "3.2")
def laa_barcode_view_rules():
    r1 = [("pblaa.tasks.laa-out-3", FileTypes.CSV, True)]
    return _barcode_report_rules() + r1 + _laa_report_rules()


@register_pipeline_rules("sa3_sat", "3.2")
def sat_view_rules():
    return _pbalign_alignmentset_rules() + _mapping_report_rules() + _variant_report_rules() + [
        ("pbreports.tasks.sat_report-out-0", FileTypes.REPORT, True)
    ]


@register_pipeline_rules("sa3_ds_resequencing_fat", "3.2")
def resequencing_view_rules():
    return _pbalign_alignmentset_rules() + _mapping_report_rules() + _variant_report_rules() + [
        ("genomic_consensus.tasks.variantcaller-out-1", FileTypes.DS_CONTIG, False, "Consensus Sequences"),
        ("genomic_consensus.tasks.variantcaller-out-2", FileTypes.FASTQ, False, "Consensus Sequences")
]

@register_pipeline_rules("sa3_ds_sv", "3.2")
def structural_variant_view_rules():
    return [
        ("pbsvtools.tasks.prepare_reference-out-0", FileTypes.DS_REF, True),
        ("pbsvtools.tasks.prepare_reference-out-1", FileTypes.TXT, True),
        ("pbsvtools.tasks.make_reports-out-0", FileTypes.JSON, True),
        ("pbsvtools.tasks.make_reports-out-1", FileTypes.JSON, True),
        ("pbreports.tasks.structural_variants_report-out-0", FileTypes.REPORT, True),
        ("pbsvtools.tasks.config-out-0", FileTypes.CFG, True)

    ]


def main(argv):
    logging.basicConfig(level=logging.INFO)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-dir", action="store", default=os.getcwd(),
                   help="Output directory for JSON files")
    args = p.parse_args(argv[1:])
    set_default_view_rules()
    for pipeline_id, rules in REGISTERED_VIEW_RULES.iteritems():
        file_name = op.join(args.output_dir, "pipeline_datastore_view_rules-{p}.json".format(p=pipeline_id.split(".")[-1]))
        log.info("Writing {f}".format(f=file_name))
        rules.write_json(file_name)
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
