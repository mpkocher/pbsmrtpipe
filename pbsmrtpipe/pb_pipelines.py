"""Centralized Place to Store the Bindings and RS_{ID} Pipelines Definitions


Naming Convention as follows:

get_pb_{name} -> Is a single task group, or bindings
get_rs_{name} -> Is entire workflow up to the {name} step

New API to support running 'Pipelines' from Portal

Before any of the Pipelines can be processed, the task library
(pbsmrtpipe/task_modules/*.py) needs to be loaded.

This is aiming to encapsulate the existing P_Module magical binding layer where
there's logic within some P_Modules that are turning tasks, or other P_Modules
on and off.

"""
import logging
import functools

from pbsmrtpipe.core import register_pipeline
from pbsmrtpipe.constants import to_pipeline_ns, ENTRY_PREFIX


log = logging.getLogger(__name__)


def _to_entry(entry_prefix, value):
    return "".join([entry_prefix, value])


class Constants(object):
    to_entry = functools.partial(_to_entry, ENTRY_PREFIX)

    ENTRY_RS_MOVIE_XML = to_entry("rs_movie_xml")
    ENTRY_INPUT_XML = to_entry("eid_input_xml")
    ENTRY_REF_FASTA = to_entry("eid_ref_fasta")

    ENTRY_DS_REF = to_entry("eid_ref_dataset")
    ENTRY_BARCODE_FASTA = to_entry("eid_barcode_fasta")
    ENTRY_BAM_ALIGNMENT = to_entry("eid_bam_alignment")
    ENTRY_DS_HDF = to_entry("eid_hdfsubread")
    ENTRY_DS_SUBREAD = to_entry("eid_subread")


def get_fetch_task_groups():
    b = [("pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.movie_overview_report:0"),
         ("pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.adapter_report:0")]

    b2 = [(Constants.ENTRY_INPUT_XML, "pbsmrtpipe.tasks.input_xml_to_fofn:0")]

    return b + b2


def _get_pb_mapping_bindings():
    b = [
        ("pbsmrtpipe.tasks.cmph5_sort:0", "pbsmrtpipe.tasks.cmph5_repack:0"),
        ("pbsmrtpipe.tasks.cmph5_repack:0", "pbsmrtpipe.tasks.cmph5_to_sam:1"),
        (Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.cmph5_to_sam:0"),
        ("pbsmrtpipe.tasks.cmph5_repack:0", "pbsmrtpipe.tasks.coverage_summary:0"),
        ("pbsmrtpipe.tasks.coverage_summary:0", "pbsmrtpipe.tasks.mapping_gff_to_bed:0")
        ]
    return b


def get_pb_mapping_bindings():
    """
    Get the standard mapping bindings + Extract Unmapped task

    """
    b = _get_pb_mapping_bindings()

    b.append(("pbsmrtpipe.tasks.align:0", "pbsmrtpipe.tasks.cmph5_sort:0"))
    return b


def get_pb_mapping_with_reports_bindings():
    b = get_pb_mapping_bindings()

    # Mapping Report
    b1 = [('pbsmrtpipe.tasks.input_xml_to_fofn:0', 'pbsmrtpipe.tasks.mapping_stats_report:0'),
          ('pbsmrtpipe.tasks.filter:0', 'pbsmrtpipe.tasks.mapping_stats_report:1'),
          ('pbsmrtpipe.tasks.cmph5_repack:0', 'pbsmrtpipe.tasks.mapping_stats_report:2')]

    # Coverage Report
    b2 = [('pbsmrtpipe.tasks.coverage_summary:0', 'pbsmrtpipe.tasks.coverage_report:0'),
          (Constants.ENTRY_REF_FASTA, 'pbsmrtpipe.tasks.coverage_report:1')]

    return b + b1 + b2


def get_pb_ccs_mapping_bindings():
    b = _get_pb_mapping_bindings()
    b.append(("pbsmrtpipe.tasks.align_ccs:0", "pbsmrtpipe.tasks.cmph5_sort:0"))
    return b


def get_pb_mapping_with_unmapped_bindings():
    b = get_pb_mapping_bindings()

    # the assumes filtering has been done and FilterSubreads.fasta exists
    b.append(("pbsmrtpipe.tasks.cmph5_repack:0", "pbsmrtpipe.tasks.extract_unmapped_subreads:1"))
    return b


def get_pb_consensus_bindings():
    # Add Consensus Bindings

    # Many of these tasks still need to have the reference bound to the input
    b1 = [("pbsmrtpipe.tasks.cmph5_repack:0", "pbsmrtpipe.tasks.call_variants_with_fastx:1"),
          ("pbsmrtpipe.tasks.write_reference_contig_chunks:0", "pbsmrtpipe.tasks.call_variants_with_fastx:2")]

    # Convert GFF to Bed
    b2 = [("pbsmrtpipe.tasks.call_variants_with_fastx:0", "pbsmrtpipe.tasks.consensus_gff_to_bed:0")]

    # Convert GFF to VCF
    b3 = [("pbsmrtpipe.tasks.call_variants_with_fastx:0", "pbsmrtpipe.tasks.gff_to_vcf:0")]

    # Summarize Consensus
    b4 = [("pbsmrtpipe.tasks.call_variants_with_fastx:0", "pbsmrtpipe.tasks.enrich_summarize_consensus:0"),
          ("pbsmrtpipe.tasks.coverage_summary:0", "pbsmrtpipe.tasks.enrich_summarize_consensus:1")]

    return b1 + b2 + b3 + b4


def get_pb_barcode_task_group():

    b = [("pbsmrtpipe.tasks.label_zmws:0", "pbsmrtpipe.tasks.generate_barcode_fastqs:1")]

    return b


def get_pb_barcode_bindings():

    b1 = get_pb_barcode_task_group()

    b2 = [("pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.label_zmws:0"),
          (Constants.ENTRY_BARCODE_FASTA, "pbsmrtpipe.tasks.label_zmws:1")]

    b3 = [('pbsmrtpipe.tasks.input_xml_to_fofn:0', 'pbsmrtpipe.tasks.generate_barcode_fastqs:0')]

    b4 = [("pbsmrtpipe.tasks.label_zmws:0", "pbsmrtpipe.tasks.barcode_report:1"),
          ("pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.barcode_report:0")]

    return b1 + b2 + b3 + b4


def get_pb_consensus_reports_bindings():
    # Consensus Reports
    # Many of these tasks still need to have the reference bound to the input
    b1 = [("pbsmrtpipe.tasks.call_variants_with_fastx:0", "pbsmrtpipe.tasks.top_variants_report:1")]

    b2 = [("pbsmrtpipe.tasks.enrich_summarize_consensus:0", "pbsmrtpipe.tasks.variants_report:1"),
          ("pbsmrtpipe.tasks.call_variants_with_fastx:0", "pbsmrtpipe.tasks.variants_report:2")]

    return b1 + b2


def get_pb_consensus_report_task_groups():

    b = get_pb_consensus_reports_bindings()
    b.append((Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.top_variants_report:0"))
    b.append((Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.variants_report:0"))

    return b


def pb_fetch():
    # Only Fetch
    return get_fetch_task_groups()


def pb_mapping():
    # Only Mapping + Report
    b = get_pb_mapping_with_reports_bindings()
    return b


def pb_consensus():
    # Only Consensus + Consensus Reports
    b = get_pb_consensus_bindings()
    return b


@register_pipeline(to_pipeline_ns("rs_reference_converter"), "RS_ReferenceProcessor")
def rs_reference_uploader():
    b = [(Constants.ENTRY_REF_FASTA, 'pbsmrtpipe.tasks.reference_converter:0')]
    return b


@register_pipeline(to_pipeline_ns("rs_fetch_1"), "RS Fetch ")
def rs_fetch_1():
    """Description of RS Fetch"""
    return pb_fetch()


@register_pipeline(to_pipeline_ns("rs_filter_1"), "RS_Filter ")
def rs_filter():
    """Description of RS Filter"""

    b = get_fetch_task_groups()

    # Filter Task
    b1 = [('pbsmrtpipe.tasks.input_xml_to_fofn:0', 'pbsmrtpipe.tasks.filter:0'),
          ('pbsmrtpipe.tasks.input_xml_to_fofn:1', 'pbsmrtpipe.tasks.filter:1')]

    # Create a FOFN metadata report from the region FOFN
    b2 = [("pbsmrtpipe.tasks.filter:0", "pbsmrtpipe.tasks.rgn_fofn_to_report:0")]

    # Filter Subread
    b3 = [("pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.filter_subreads:0"),
          ("pbsmrtpipe.tasks.input_xml_to_fofn:1", "pbsmrtpipe.tasks.filter_subreads:1"),
          ("pbsmrtpipe.tasks.filter:0", "pbsmrtpipe.tasks.filter_subreads:2"),
          ("pbsmrtpipe.tasks.rgn_fofn_to_report:0", "pbsmrtpipe.tasks.filter_subreads:3")]

    # Reports/Summary
    b4 = [("pbsmrtpipe.tasks.filter:0", "pbsmrtpipe.tasks.filter_subread_summary:0")]

    b5 = [("pbsmrtpipe.tasks.filter:1", "pbsmrtpipe.tasks.filter_report:0")]
    b6 = [("pbsmrtpipe.tasks.filter_subread_summary:0", "pbsmrtpipe.tasks.filter_subread_report:0")]
    b7 = [("pbsmrtpipe.tasks.filter:1", "pbsmrtpipe.tasks.loading_report:0")]

    return b + b1 + b2 + b3 + b4 + b5 + b6 + b7


@register_pipeline(to_pipeline_ns("rs_subreads_1"), "RS Subreads:1")
def rs_subreads():
    """RS Subreads:1 Description"""
    return rs_filter()


@register_pipeline(to_pipeline_ns("rs_mapping_1"), "RS_Mapping 1")
def rs_mapping():
    """Description of RS Mapping"""
    b = rs_filter()

    # Convert Reference
    br = [(Constants.ENTRY_REF_FASTA, 'pbsmrtpipe.tasks.ref_to_report:0')]

    # Add Align task mappings
    bids = [("pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.align:0"),
            ("pbsmrtpipe.tasks.filter:0", "pbsmrtpipe.tasks.align:1"),
            (Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.align:2"),
            ('pbsmrtpipe.tasks.input_xml_to_fofn:1', "pbsmrtpipe.tasks.align:3")]

    # Unmapped subreads from filtering
    bu = [("pbsmrtpipe.tasks.filter_subreads:0", "pbsmrtpipe.tasks.extract_unmapped_subreads:0"),
          ("pbsmrtpipe.tasks.cmph5_repack:0", "pbsmrtpipe.tasks.extract_unmapped_subreads:1")]

    breports = get_pb_mapping_with_reports_bindings()

    return b + br + bids + bu + breports


@register_pipeline(to_pipeline_ns("rs_resequencing_1"), 'RS Resequencing Pipeline')
def rs_resequencing_1():
    """Official Pacbio Resequencing Pipeline Description"""

    # Standard Fetch + Filter + FilterReports + Mapping + Mapping Reports +
    # Consensus + Consensus Reports

    b = rs_mapping()

    b2 = get_pb_consensus_bindings()
    b3 = get_pb_consensus_reports_bindings()

    # Add Consensus Bindings
    b4 = [("pbsmrtpipe.tasks.cmph5_repack:0", "pbsmrtpipe.tasks.call_variants_with_fastx:1"),
          ("pbsmrtpipe.tasks.call_variants_with_fastx:0", "pbsmrtpipe.tasks.consensus_gff_to_bed:0"),
          ("pbsmrtpipe.tasks.call_variants_with_fastx:0", "pbsmrtpipe.tasks.gff_to_vcf:0"),
          ("pbsmrtpipe.tasks.coverage_summary:0", "pbsmrtpipe.tasks.enrich_summarize_consensus:1"),
          ("pbsmrtpipe.tasks.call_variants_with_fastx:0", "pbsmrtpipe.tasks.enrich_summarize_consensus:0")]

    # Add Entry Points for Reference
    b5 = [(Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.call_variants_with_fastx:0"),
          (Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.gff_to_vcf:1")]

    # Write Contigs
    b6 = [(Constants.ENTRY_REF_FASTA, 'pbsmrtpipe.tasks.write_reference_contig_chunks:0'),
          ('pbsmrtpipe.tasks.ref_to_report:0', 'pbsmrtpipe.tasks.write_reference_contig_chunks:1')]

    # Reports
    b7 = [(Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.variants_report:0"),
          (Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.top_variants_report:0")]

    return b + b2 + b3 + b4 + b5 + b6 + b7


@register_pipeline(to_pipeline_ns("rs_bam_mapping_1"), "RS BAM Mapping Pipeline")
def rs_bam_resequencing_pipeline():
    """Official Pacbio BAM driven Resequencing Pipeline"""

    # Generate FASTA metadata report and write contig headers FOFN
    br = [(Constants.ENTRY_REF_FASTA, 'pbsmrtpipe.tasks.ref_to_report:0'),
          (Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.write_reference_contig_idx_chunks:0"),
          ('pbsmrtpipe.tasks.ref_to_report:0', 'pbsmrtpipe.tasks.write_reference_contig_idx_chunks:1')]

    # Fundamental Align task
    b = [("pbsmrtpipe.pipelines.rs_filter_1:pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.bam_align:0"),
        ("pbsmrtpipe.pipelines.rs_filter_1:pbsmrtpipe.tasks.filter:0", "pbsmrtpipe.tasks.bam_align:1"),
        (Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.bam_align:2"),
        ("pbsmrtpipe.tasks.ref_to_report:0", "pbsmrtpipe.tasks.bam_align:3")]

    # Load Chemistry
    b2 = [("pbsmrtpipe.pipelines.rs_filter_1:pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.bam_load_chemistry:0"),
          ("pbsmrtpipe.tasks.bam_align:0", "pbsmrtpipe.tasks.bam_load_chemistry:1")]

    # To Movie via sentinel file
    b3 = [("pbsmrtpipe.tasks.bam_load_chemistry:0", "pbsmrtpipe.tasks.bam_to_by_movie_fofn:0")]

    # MergeSort BAM
    b4 = [("pbsmrtpipe.tasks.bam_to_by_movie_fofn:0", "pbsmrtpipe.tasks.bam_merge_sort:0"),
          ("pbsmrtpipe.tasks.write_reference_contig_idx_chunks:0", "pbsmrtpipe.tasks.bam_merge_sort:1")]

    # Create Index
    b5 = [("pbsmrtpipe.tasks.bam_merge_sort:0", "pbsmrtpipe.tasks.bam_create_index:0"),
          ("pbsmrtpipe.tasks.write_reference_contig_idx_chunks:0", "pbsmrtpipe.tasks.bam_create_index:1")]

    # Bam To By Contig FOFN
    b6 = [("pbsmrtpipe.tasks.bam_merge_sort:0", "pbsmrtpipe.tasks.to_contig_bam_fofn:0")]

    # Generate Coverage GFF Summary
    b7 = [("pbsmrtpipe.tasks.bam_to_by_movie_fofn:0", "pbsmrtpipe.tasks.bam_coverage_summary:0")]

    # Gff to Bed
    b8 = [("pbsmrtpipe.tasks.bam_coverage_summary:0", "pbsmrtpipe.tasks.mapping_gff_to_bed:0")]

    # Mapping Report
    b9 = [("pbsmrtpipe.pipelines.rs_filter_1:pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.mapping_stats_bam_report:0"),
          ("pbsmrtpipe.pipelines.rs_filter_1:pbsmrtpipe.tasks.filter:0", "pbsmrtpipe.tasks.mapping_stats_bam_report:1"),
          ("pbsmrtpipe.tasks.to_contig_bam_fofn:0", "pbsmrtpipe.tasks.mapping_stats_bam_report:2")
    ]

    # Add Consensus

    return br + b + b2 + b3 + b4 + b5 + b6 + b7 + b8 + b9


@register_pipeline(to_pipeline_ns("pb_bam_genomic_consensus"), "PacBio BAM Genomic Consensus (Only) Pipeline")
def f():
    # Because Datasets aren't used

    # Generate FASTA metadata report and write contig headers FOFN
    br = [(Constants.ENTRY_REF_FASTA, 'pbsmrtpipe.tasks.ref_to_report:0'),
          (Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.write_reference_contig_idx_chunks:0"),
          ('pbsmrtpipe.tasks.ref_to_report:0', 'pbsmrtpipe.tasks.write_reference_contig_idx_chunks:1')]

    # Quiver
    b = [(Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.bam_call_variants_with_fastx:0"),
         (Constants.ENTRY_BAM_ALIGNMENT, "pbsmrtpipe.tasks.bam_call_variants_with_fastx:1"),
         ("pbsmrtpipe.tasks.write_reference_contig_idx_chunks:0", "pbsmrtpipe.tasks.bam_call_variants_with_fastx:2")]

    # Make VCF
    b1 = [("pbsmrtpipe.tasks.bam_call_variants_with_fastx:0", "pbsmrtpipe.tasks.gff_to_vcf:0"),
          (Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.gff_to_vcf:1")]

    # Make BED
    b2 = [("pbsmrtpipe.tasks.bam_call_variants_with_fastx:0", "pbsmrtpipe.tasks.consensus_gff_to_bed:0")]

    # Coverage summary
    bs = [("pbsmrtpipe.tasks.bam_call_variants_with_fastx:0", "pbsmrtpipe.tasks.bam_coverage_summary:0")]

    # This adds too many entry points
    # Enrich Align Summary (Summary Consensus)
    # b3 = [("pbsmrtpipe.tasks.call_variants_with_fastx:0", "pbsmrtpipe.tasks.enrich_summarize_consensus:0"),
    #       ("pbsmrtpipe.tasks.coverage_summary:0", "pbsmrtpipe.tasks.enrich_summarize_consensus:1")]

    # Top Variants Report
    b4 = [(Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.top_variants_report:0"),
          ("pbsmrtpipe.tasks.bam_call_variants_with_fastx:0", "pbsmrtpipe.tasks.top_variants_report:1")]

    # Variants Report. This needs the output of enriched Alignment summary
    b5 = [(Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.variants_report:0"),
          ("pbsmrtpipe.tasks.bam_call_variants_with_fastx:0", "pbsmrtpipe.tasks.variants_report:1"),
          ("pbsmrtpipe.tasks.bam_coverage_summary:0", "pbsmrtpipe.tasks.variants_report:2")]

    return br + b + b1 + b2 + b4 + b5 + bs


@register_pipeline(to_pipeline_ns("rs_bam_resequencing_1"), "RS BAM Resequencing Pipeline")
def f():

    # Start from the RS BAM pipeline

    # Quiver
    b = [(Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.bam_call_variants_with_fastx:0"),
         ("pbsmrtpipe.pipelines.rs_bam_mapping_1:pbsmrtpipe.tasks.bam_merge_sort:0", "pbsmrtpipe.tasks.bam_call_variants_with_fastx:1"),
         ("pbsmrtpipe.pipelines.rs_bam_mapping_1:pbsmrtpipe.tasks.write_reference_contig_idx_chunks:0", "pbsmrtpipe.tasks.bam_call_variants_with_fastx:2")]

    # Make VCF
    b1 = [("pbsmrtpipe.tasks.bam_call_variants_with_fastx:0", "pbsmrtpipe.tasks.gff_to_vcf:0"),
          (Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.gff_to_vcf:1")]

    # Make BED
    b2 = [("pbsmrtpipe.tasks.bam_call_variants_with_fastx:0", "pbsmrtpipe.tasks.consensus_gff_to_bed:0")]

    # Enrich Align Summary (Summary Consensus)
    b3 = [("pbsmrtpipe.tasks.bam_call_variants_with_fastx:0", "pbsmrtpipe.tasks.enrich_summarize_consensus:0"),
          ("pbsmrtpipe.tasks.bam_coverage_summary:0", "pbsmrtpipe.tasks.enrich_summarize_consensus:1")]

    # Top Variants Report
    b4 = [(Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.top_variants_report:0"),
          ("pbsmrtpipe.tasks.bam_call_variants_with_fastx:0", "pbsmrtpipe.tasks.top_variants_report:1")]

    # Variants Report. This needs the output of enriched Alignment summary
    b5 = [(Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.variants_report:0"),
          ("pbsmrtpipe.tasks.bam_call_variants_with_fastx:0", "pbsmrtpipe.tasks.variants_report:1"),
          ("pbsmrtpipe.tasks.bam_coverage_summary:0", "pbsmrtpipe.tasks.variants_report:2")]

    return b + b1 + b2 + b4 + b5 + b3


@register_pipeline(to_pipeline_ns("rs_bridge_mapper_1"), 'RS_BridgeMapper')
def rs_bridgemapper_1():
    """RS BridgeMapper Description"""

    # Pipelines definition for RS_BridgeMapper:1
    # b = rs_resequencing_1(workflow_settings)

    # let's just use up to mapping. Consensus isn't needed to generate BM
    # output
    b = rs_mapping()

    x = [("pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.run_bridge_mapper:0"),
         ("pbsmrtpipe.tasks.cmph5_repack:0", "pbsmrtpipe.tasks.run_bridge_mapper:1"),
         (Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.run_bridge_mapper:2")]

    return b + x


@register_pipeline(to_pipeline_ns("rs_modification_detection_1"), 'RS_Modification_Detection:1')
def rs_modification_detection_1():
    """RS Modification Detection"""
    bs = rs_resequencing_1()

    def _add(b_):
        bs.append(b_)

    _add((Constants.ENTRY_REF_FASTA, 'pbsmrtpipe.tasks.compute_modifications:0'))
    _add(('pbsmrtpipe.tasks.ref_to_report:0', 'pbsmrtpipe.tasks.compute_modifications:1'))
    _add(('pbsmrtpipe.tasks.cmph5_repack:0', 'pbsmrtpipe.tasks.compute_modifications:2'))
    _add(('pbsmrtpipe.tasks.write_reference_contig_chunks:0', 'pbsmrtpipe.tasks.compute_modifications:3'))

    _add(('pbsmrtpipe.tasks.compute_modifications:1', 'pbsmrtpipe.tasks.modification_report:0'))

    return bs


@register_pipeline(to_pipeline_ns("rs_modification_motif_analysis_1"), 'RS_Modification_and_Motif_Analysis')
def rs_modification_and_motif_analysis_1():
    """Pacbio Official Modification and Motif Analysis Pipeline"""
    bs = rs_modification_detection_1()

    def _add(b_):
        bs.append(b_)

    # Find Motifs
    _add((Constants.ENTRY_REF_FASTA, 'pbsmrtpipe.tasks.find_motifs:1'))
    _add(('pbsmrtpipe.tasks.compute_modifications:0', 'pbsmrtpipe.tasks.find_motifs:0'))

    # Make Motifs
    _add(('pbsmrtpipe.tasks.compute_modifications:0', 'pbsmrtpipe.tasks.make_motif_gff:0'))
    _add(('pbsmrtpipe.tasks.compute_modifications:1', 'pbsmrtpipe.tasks.make_motif_gff:1'))
    _add(('pbsmrtpipe.tasks.find_motifs:0', 'pbsmrtpipe.tasks.make_motif_gff:2'))
    _add((Constants.ENTRY_REF_FASTA, 'pbsmrtpipe.tasks.make_motif_gff:3'))

    # add report
    _add(('pbsmrtpipe.tasks.make_motif_gff:0', 'pbsmrtpipe.tasks.motif_report:0'))
    _add(('pbsmrtpipe.tasks.find_motifs:0', 'pbsmrtpipe.tasks.motif_report:1'))

    return bs


@register_pipeline(to_pipeline_ns("rs_roi_1"), 'RS_ReadsOfInsert')
def rs_readsofinsert_1():
    """Official Reads of Insert Pipeline"""

    # Pipelines definition for RS_ReadsOfInsert:1
    b = get_fetch_task_groups()

    # rbids = [("pbsmrtpipe.tasks.reads_of_insert:0", "pbsmrtpipe.tasks.roi_to_fofn:0"),
    #          ("pbsmrtpipe.tasks.reads_of_insert:0", "pbsmrtpipe.tasks.roi_gather_fastx:0"),
    #          ("pbsmrtpipe.tasks.roi_to_fofn:0", "pbsmrtpipe.tasks.roi_report:0")]

    rbids = [("pbsmrtpipe.tasks.reads_of_insert:0", "pbsmrtpipe.tasks.roi_report:0")]

    return b + rbids + [("pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.reads_of_insert:0")]


@register_pipeline(to_pipeline_ns("rs_roi_mapping_1"), 'RS_ReadsOfInsert_Mapping')
def rs_readsofinsert_mapping_1():
    """Official Pacbio Reads of Insert Mapping Pipeline"""
    # Pipelines definition for RS_ReadsOfInsert_Mapping:1

    b = rs_readsofinsert_1()
    br = [(Constants.ENTRY_REF_FASTA, 'pbsmrtpipe.tasks.ref_to_report:0')]

    m = get_pb_ccs_mapping_bindings()

    rmbids = []
    rmbids.append(("pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.reads_of_insert:0"))
    rmbids.append((Constants.ENTRY_REF_FASTA, "pbsmrtpipe.tasks.align_ccs:1"))
    rmbids.append(('pbsmrtpipe.tasks.ref_to_report:0', "pbsmrtpipe.tasks.align_ccs:2"))
    rmbids.append(("pbsmrtpipe.tasks.reads_of_insert:0", "pbsmrtpipe.tasks.align_ccs:0"))

    return rmbids + b + m + br


@register_pipeline(to_pipeline_ns("rs_filter_barcode_1"), "RS Subreads Barcode")
def rs_filter_barcode():

    bs = rs_filter()

    x = get_pb_barcode_bindings()

    y = [(Constants.ENTRY_BARCODE_FASTA, "pbsmrtpipe.tasks.label_zmws:1")]

    z = [("pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.label_zmws:0"),
         ('pbsmrtpipe.tasks.input_xml_to_fofn:0', 'pbsmrtpipe.tasks.generate_barcode_fastqs:0'),
            ("pbsmrtpipe.tasks.label_zmws:0", "pbsmrtpipe.tasks.barcode_report:1"),
            ("pbsmrtpipe.tasks.input_xml_to_fofn:0", "pbsmrtpipe.tasks.barcode_report:0")]

    return bs + x + y + z


@register_pipeline(to_pipeline_ns("rs_resequencing_barcode_1"), 'RS_Resequencing_Barcode')
def rs_resequencing_barcode_1():
    """Official RS Sequencing Barcode"""

    # The Standard Resequencing Needs to be altered a bit because the
    # cmp.h5 file is mutated.
    # TODO  This is not quite right. This doesn't have the mutating cmp.h5

    b = rs_resequencing_1()

    b1 = get_pb_barcode_bindings()

    return b + b1


@register_pipeline(to_pipeline_ns("rs_sat_1"), 'RS_Site_Acceptance_Test')
def rs_site_acceptance_test_1():
    """Site Acceptance Test"""

    # Pipelines definition for RS_Site_Acceptance_Test:1
    b = rs_resequencing_1()

    x = [("pbsmrtpipe.tasks.cmph5_repack:0", "pbsmrtpipe.tasks.sat_report:0"),
         ("pbsmrtpipe.tasks.mapping_stats_report:0", "pbsmrtpipe.tasks.sat_report:1"),
         ("pbsmrtpipe.tasks.variants_report:0", "pbsmrtpipe.tasks.sat_report:2")]

    return b + x


@register_pipeline(to_pipeline_ns("rs_amplicon_assembly"), "RS_AmpliconAssembly")
def f():

    b1 = get_fetch_task_groups()

    b2 = [('pbsmrtpipe.tasks.input_xml_to_fofn:0', 'pbsmrtpipe.tasks.amplicon_assembly:0')]

    return b1 + b2


@register_pipeline(to_pipeline_ns("rs_amplicon_assembly_barcode"), "RS_AmpliconAssemblyBarcode")
def f():

    b1 = get_fetch_task_groups()

    # Call barcoding
    b2 = get_pb_barcode_bindings()

    b3 = [('pbsmrtpipe.tasks.input_xml_to_fofn:0', 'pbsmrtpipe.tasks.amplicon_assembly_barcode:0'),
           ('pbsmrtpipe.tasks.label_zmws:0', 'pbsmrtpipe.tasks.amplicon_assembly_barcode:1')]

    return b1 + b2 + b3


@register_pipeline(to_pipeline_ns('rs_amplicon_assembly_split_barcode'), "RS_AmpliconAssemblySplitBarcode")
def f():

    b1 = get_fetch_task_groups()

    # Call Barcoding
    b2 = get_pb_barcode_bindings()

    # Call amplicon assembly
    b3 = [('pbsmrtpipe.tasks.input_xml_to_fofn:0', 'pbsmrtpipe.tasks.amplicon_assembly_barcode:0'),
          ('pbsmrtpipe.tasks.label_zmws:0', 'pbsmrtpipe.tasks.amplicon_assembly_barcode:1')]

    return b1 + b2 + b3


@register_pipeline(to_pipeline_ns('rs_minor_variants'), 'RS_MinorVariants')
def f():

    b1 = rs_readsofinsert_mapping_1()

    # CCS bindings
    b2 = [('pbsmrtpipe.tasks.cmph5_repack:0', 'pbsmrtpipe.tasks.call_minor_variants:0'),
          (Constants.ENTRY_REF_FASTA, 'pbsmrtpipe.tasks.call_minor_variants:1')]

    return b1 + b2