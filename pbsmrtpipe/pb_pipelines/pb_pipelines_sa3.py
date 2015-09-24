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
    ENTRY_DS_ALIGN = to_entry("eid_alignment")
    ENTRY_DS_CCS = to_entry("eid_ccs")


def _core_export_fastx(subread_ds):
    b1 = [(subread_ds, "pbsmrtpipe.tasks.bam2fasta:0")]
    # bam2fastq
    b2 = [(subread_ds, "pbsmrtpipe.tasks.bam2fastq:0")]
    return b1 + b2


def _core_align(subread_ds, reference_ds):
    # Call blasr/pbalign
    b3 = [(subread_ds, "pbalign.tasks.pbalign:0"),
          (reference_ds, "pbalign.tasks.pbalign:1")]
    return b3


def _core_align_plus(subread_ds, reference_ds):
    bs = _core_align(subread_ds, reference_ds)

    b4 = [("pbalign.tasks.pbalign:0", "pbreports.tasks.mapping_stats:0")]

    b5 = [("pbalign.tasks.pbalign:0", "pbalign.tasks.consolidate_bam:0")]

    return bs + b4 + b5


def _core_gc(alignment_ds, reference_ds):
    b1 = [(reference_ds, "genomic_consensus.tasks.variantcaller:1"),
          (alignment_ds, "genomic_consensus.tasks.variantcaller:0")]

    return b1


def _core_gc_plus(alignment_ds, reference_ds):
    """
    Returns a list of core bindings
    """

    # Need to have a better model to avoid copy any paste. This is defined in the
    # fat resquencing pipeline.
    # Summarize Coverage
    b1 = _core_gc(alignment_ds, reference_ds)

    b2 = [(alignment_ds, "pbreports.tasks.summarize_coverage:0"),
          (reference_ds, "pbreports.tasks.summarize_coverage:1")]

    b3 = [("pbreports.tasks.summarize_coverage:0", "genomic_consensus.tasks.summarize_consensus:0"),
          ("genomic_consensus.tasks.variantcaller:0", "genomic_consensus.tasks.summarize_consensus:1")]

    # Consensus Reports - variants
    b4 = [(reference_ds, "pbreports.tasks.variants_report:0"),
          ("genomic_consensus.tasks.summarize_consensus:0", "pbreports.tasks.variants_report:1"),
          ("genomic_consensus.tasks.variantcaller:0", "pbreports.tasks.variants_report:2")]

    # Consensus Reports - top variants
    b5 = [("genomic_consensus.tasks.variantcaller:0", "pbreports.tasks.top_variants:0"),
          (reference_ds, "pbreports.tasks.top_variants:1")]

    return b1 + b2 + b3 + b4 + b5


@register_pipeline(to_pipeline_ns("sa3_fetch"), "RS Movie to Subread DataSet", "0.1.0", tags=("convert", ))
def sa3_fetch():
    """
    SA3 Convert RS movie metadata XML to Subread DataSet XML
    """

    # convert to RS dataset
    b1 = [(Constants.ENTRY_RS_MOVIE_XML, "pbscala.tasks.rs_movie_to_ds_rtc:0")]

    b2 = [("pbscala.tasks.rs_movie_to_ds_rtc:0", "pbsmrtpipe.tasks.h5_subreads_to_subread:0")]

    return b1 + b2


@register_pipeline(to_pipeline_ns("sa3_align"), "SA3 RS movie Align", "0.1.0", tags=("mapping", ))
def sa3_align():
    """
    SA3 Convert RS movie XML to Alignment DataSet XML
    """
    # convert to RS dataset
    b1 = [(Constants.ENTRY_RS_MOVIE_XML, "pbscala.tasks.rs_movie_to_ds_rtc:0")]

    # h5 dataset to subread dataset via bax2bam
    b2 = [("pbscala.tasks.rs_movie_to_ds_rtc:0", "pbsmrtpipe.tasks.h5_subreads_to_subread:0")]

    bxs = _core_align_plus("pbsmrtpipe.tasks.h5_subreads_to_subread:0", Constants.ENTRY_DS_REF)

    return b1 + b2 + bxs


@register_pipeline(to_pipeline_ns("sa3_resequencing"), "SA3 RS movie Resequencing", "0.1.0", tags=("mapping", "consensus"))
def sa3_resequencing():
    return _core_gc("pbsmrtpipe.pipelines.sa3_align:pbalign.tasks.pbalign:0", Constants.ENTRY_DS_REF)


@register_pipeline(to_pipeline_ns("sa3_hdfsubread_to_subread"), "Convert Hdf SubreadSet to SubreadSet", "0.1.0", tags=("convert", ))
def hdf_subread_converter():

    b2 = [(Constants.ENTRY_DS_HDF, "pbsmrtpipe.tasks.h5_subreads_to_subread:0")]

    return b2


@register_pipeline(to_pipeline_ns("sa3_ds_align"), "SA3 SubreadSet Mapping", "0.1.0", tags=("mapping", ))
def ds_align():
    return _core_align_plus(Constants.ENTRY_DS_SUBREAD, Constants.ENTRY_DS_REF)

RESEQUENCING_TASK_OPTIONS = {
    "genomic_consensus.task_options.diploid": False
}

@register_pipeline(to_pipeline_ns("sa3_ds_genomic_consensus"), "SA3 Genomic Consensus", "0.1.0",
                   task_options=RESEQUENCING_TASK_OPTIONS)
def ds_genomic_consenus():
    """Run Genomic Consensus"""
    return _core_gc_plus(Constants.ENTRY_DS_ALIGN, Constants.ENTRY_DS_REF)


@register_pipeline(to_pipeline_ns("sa3_ds_resequencing"), "SA3 SubreadSet Resequencing", "0.1.0",
                   task_options=RESEQUENCING_TASK_OPTIONS)
def ds_resequencing():
    """Core Resequencing Pipeline"""
    return _core_gc("pbsmrtpipe.pipelines.sa3_ds_align:pbalign.tasks.pbalign:0", Constants.ENTRY_DS_REF)


@register_pipeline(to_pipeline_ns("sa3_ds_resequencing_fat"), "SA3 SubreadSet Resequencing With GC Extras and Reports", "0.1.0",
                   task_options=RESEQUENCING_TASK_OPTIONS)
def ds_fat_resequencing():
    """DS RS + GC extras and Reports"""

    return _core_gc_plus("pbsmrtpipe.pipelines.sa3_ds_resequencing:pbalign.tasks.pbalign:0", Constants.ENTRY_DS_REF)


def _core_mod_detection(alignment_ds, reference_ds):
    bs = []
    _add = bs.append

    # AlignmentSet, ReferenceSet
    _add((alignment_ds, "kinetics_tools.tasks.ipd_summary:0"))
    _add((reference_ds, 'kinetics_tools.tasks.ipd_summary:1'))

    _add(('kinetics_tools.tasks.ipd_summary:1', 'pbreports.tasks.modifications_report:0'))
    return bs


@register_pipeline(to_pipeline_ns("ds_modification_detection"), 'SA3 Modification Detection', "0.1.0", tags=("modification-detection", ))
def rs_modification_detection_1():
    """RS Modification Detection"""
    b1 = _core_mod_detection("pbsmrtpipe.pipelines.sa3_ds_resequencing_fat:pbalign.tasks.pbalign:0", Constants.ENTRY_DS_REF)
    b2 = [
        # basemods.gff
        ("kinetics_tools.tasks.ipd_summary:0", "kinetics_tools.tasks.summarize_modifications:0"),
        # alignment_summary_final.gff
        ("pbsmrtpipe.pipelines.sa3_ds_resequencing_fat:pbreports.tasks.summarize_coverage:0", "kinetics_tools.tasks.summarize_modifications:1")
    ]
    return b1 + b2

def _core_motif_analysis(ipd_gff, reference_ds):
    bs = []
    x = bs.append
    # Find Motifs. AlignmentSet, ReferenceSet
    x((ipd_gff, 'motif_maker.tasks.find_motifs:0'))  # basemods GFF
    x((reference_ds, 'motif_maker.tasks.find_motifs:1'))

    # Make Motifs GFF: ipdSummary GFF, ipdSummary CSV, MotifMaker CSV, REF
    x((ipd_gff, 'motif_maker.tasks.reprocess:0'))  # GFF
    # XXX this is not currently used
    #_add(('pbsmrtpipe.pipelines.ds_modification_detection:kinetics_tools.tasks.ipd_summary:1', 'motif_maker.tasks.reprocess:1')) # CSV
    x(('motif_maker.tasks.find_motifs:0', 'motif_maker.tasks.reprocess:1'))  # motifs GFF
    x((reference_ds, 'motif_maker.tasks.reprocess:2'))

    # MK Note. Pat did something odd here that I can't remember the specifics
    x(('motif_maker.tasks.reprocess:0', 'pbreports.tasks.motifs_report:0'))
    x(('motif_maker.tasks.find_motifs:0', 'pbreports.tasks.motifs_report:1'))

    return bs

BASEMODS_TASK_OPTIONS = dict(RESEQUENCING_TASK_OPTIONS)
BASEMODS_TASK_OPTIONS["kinetics_tools.task_options.pvalue"] = 0.001

@register_pipeline(to_pipeline_ns("ds_modification_motif_analysis"), 'SA3 Modification and Motif Analysis', "0.1.0", tags=("motif-analysis", ),
        task_options=BASEMODS_TASK_OPTIONS)
def rs_modification_and_motif_analysis_1():
    """
    Pacbio Official Modification and Motif Analysis Pipeline
    """
    return _core_motif_analysis(
        'pbsmrtpipe.pipelines.ds_modification_detection:kinetics_tools.tasks.ipd_summary:0', Constants.ENTRY_DS_REF)


@register_pipeline(to_pipeline_ns("pb_modification_detection"), 'SA3 Internal Modification Analysis', "0.1.0", tags=("mapping", ), task_options={"kinetics_tools.task_options.pvalue":0.001})
def pb_modification_analysis_1():
    """
    Internal base modification analysis pipeline, starting from an existing
    AlignmentSet
    """
    return _core_mod_detection(Constants.ENTRY_DS_ALIGN, Constants.ENTRY_DS_REF)


@register_pipeline(to_pipeline_ns("pb_modification_motif_analysis"), 'SA3 Internal Modification and Motif Analysis', "0.1.0", tags=("motif-analysis", ), task_options={"kinetics_tools.task_options.pvalue":0.001})
def pb_modification_and_motif_analysis_1():
    """
    Internal base modification and motif analysis pipeline, starting from an
    existing AlignmentSet
    """
    return _core_motif_analysis('pbsmrtpipe.pipelines.pb_modification_detection:kinetics_tools.tasks.ipd_summary:0',
                                Constants.ENTRY_DS_REF)

SAT_TASK_OPTIONS = dict(RESEQUENCING_TASK_OPTIONS)
SAT_TASK_OPTIONS["genomic_consensus.task_options.algorithm"] = "plurality"
@register_pipeline(to_pipeline_ns("sa3_sat"), 'SA3 Site Acceptance Test', "0.1.0", tags=("sat", ),
                   task_options=SAT_TASK_OPTIONS)
def rs_site_acceptance_test_1():
    """Site Acceptance Test"""

    # AlignmentSet, GFF, mapping Report
    x = [("pbsmrtpipe.pipelines.sa3_ds_resequencing:pbalign.tasks.pbalign:0", "pbreports.tasks.sat_report:0"),
         ("pbsmrtpipe.pipelines.sa3_ds_resequencing_fat:pbreports.tasks.variants_report:0", "pbreports.tasks.sat_report:1"),
         ("pbsmrtpipe.pipelines.sa3_ds_resequencing_fat:pbreports.tasks.mapping_stats:0", "pbreports.tasks.sat_report:2")]

    return x


def _core_export_fastx(subread_ds):
    b1 = [(subread_ds, "pbsmrtpipe.tasks.bam2fasta:0")]
    b2 = [(subread_ds, "pbsmrtpipe.tasks.bam2fastq:0")]
    return b1 + b2


def _core_export_fastx_ccs(ccs_ds):
    b1 = [(ccs_ds, "pbsmrtpipe.tasks.bam2fasta_ccs:0")]
    b2 = [(ccs_ds, "pbsmrtpipe.tasks.bam2fastq_ccs:0")]
    return b1 + b2


def _core_laa(subread_ds):
    # Call ccs
    b3 = [(subread_ds, "pblaa.tasks.laa:0")]
    return b3

@register_pipeline(to_pipeline_ns("sa3_ds_laa"), "SA3 Consensus Reads", "0.1.0", tags=("laa", ))
def ds_laa():
    """
    Basic Long Amplicon Analysis (LAA) pipeline, starting from subreads.
    """
    subreadset = Constants.ENTRY_DS_SUBREAD
    return _core_laa(subreadset)


def _core_ccs(subread_ds):
    # Call ccs
    b3 = [(subread_ds, "pbccs.tasks.ccs:0")]
    # CCS report
    b4 = [("pbccs.tasks.ccs:0", "pbreports.tasks.ccs_report:0")]
    b5 = _core_export_fastx_ccs("pbccs.tasks.ccs:0")
    return b3 + b4 + b5


@register_pipeline(to_pipeline_ns("sa3_ds_ccs"), "SA3 Consensus Reads", "0.1.0", tags=("ccs", ))
def ds_ccs():
    """
    Basic ConsensusRead (CCS) pipeline, starting from subreads.
    """
    return _core_ccs(Constants.ENTRY_DS_SUBREAD)


def _core_ccs_align(ccs_ds):
    # pbalign w/CCS input
    b3 = [(ccs_ds, "pbalign.tasks.pbalign_ccs:0"),
          (Constants.ENTRY_DS_REF, "pbalign.tasks.pbalign_ccs:1")]
    # mapping_stats_report (CCS version)
    b4 = [("pbalign.tasks.pbalign_ccs:0",
           "pbreports.tasks.mapping_stats_ccs:0")]
    return b3+b4


@register_pipeline(to_pipeline_ns("sa3_ds_ccs_align"), "SA3 Consensus Read Mapping", "0.1.0", tags=("mapping", ))
def ds_align_ccs():
    """
    ConsensusRead (CCS) + Mapping pipeline, starting from subreads.
    """
    return _core_ccs_align("pbsmrtpipe.pipelines.sa3_ds_ccs:pbccs.tasks.ccs:0")


@register_pipeline(to_pipeline_ns("pb_ccs_align"), "Internal Consensus Read Mapping", "0.1.0", tags=("mapping", ))
def pb_align_ccs():
    """
    Internal ConsensusRead (CCS) alignment pipeline, starting from an existing
    ConsensusReadSet.
    """
    return _core_ccs_align(Constants.ENTRY_DS_CCS)


def _core_isoseq_classify(ccs_ds):
    b3 = [ # classify all CCS reads - CHUNKED (ContigSet scatter)
        (ccs_ds, "pbtranscript.tasks.classify:0")
    ]
    b4 = [ # pbreports isoseq_classify
        ("pbtranscript.tasks.classify:1", "pbreports.tasks.isoseq_classify:0"),
        ("pbtranscript.tasks.classify:3", "pbreports.tasks.isoseq_classify:1")
    ]
    return b3 + b4


def _core_isoseq_cluster(ccs_ds):
    b5 = [ # cluster reads and get consensus isoforms
        # full-length, non-chimeric transcripts
        ("pbtranscript.tasks.classify:1", "pbtranscript.tasks.cluster:0"),
        # non-full-length transcripts
        ("pbtranscript.tasks.classify:2", "pbtranscript.tasks.cluster:1"),
        (ccs_ds, "pbtranscript.tasks.cluster:2"),
        (Constants.ENTRY_DS_SUBREAD, "pbtranscript.tasks.cluster:3")
    ]
    b6 = [ # ice_partial to map non-full-lenth reads to consensus isoforms
        # non-full-length transcripts
        ("pbtranscript.tasks.classify:2", "pbtranscript.tasks.ice_partial:0"),
        # draft consensus isoforms
        ("pbtranscript.tasks.cluster:0", "pbtranscript.tasks.ice_partial:1"),
        (ccs_ds, "pbtranscript.tasks.ice_partial:2"),
    ]
    b7 = [
        (Constants.ENTRY_DS_SUBREAD, "pbtranscript.tasks.ice_quiver:0"),
        ("pbtranscript.tasks.cluster:0", "pbtranscript.tasks.ice_quiver:1"),
        ("pbtranscript.tasks.cluster:3", "pbtranscript.tasks.ice_quiver:2"),
        ("pbtranscript.tasks.ice_partial:0", "pbtranscript.tasks.ice_quiver:3")
    ]
    b8 = [
        (Constants.ENTRY_DS_SUBREAD, "pbtranscript.tasks.ice_quiver_postprocess:0"),
        ("pbtranscript.tasks.cluster:0", "pbtranscript.tasks.ice_quiver_postprocess:1"),
        ("pbtranscript.tasks.cluster:3", "pbtranscript.tasks.ice_quiver_postprocess:2"),
        ("pbtranscript.tasks.ice_partial:0", "pbtranscript.tasks.ice_quiver_postprocess:3"),
        ("pbtranscript.tasks.ice_quiver:0", "pbtranscript.tasks.ice_quiver_postprocess:4")
    ]
    b9 = [ # pbreports isoseq_cluster
        # draft consensus isoforms
        ("pbtranscript.tasks.cluster:0", "pbreports.tasks.isoseq_cluster:0"),
        # json report
        ("pbtranscript.tasks.cluster:1", "pbreports.tasks.isoseq_cluster:1"),
    ]

    return _core_isoseq_classify(ccs_ds) + b5 + b6 + b7 + b8 + b9


ISOSEQ_TASK_OPTIONS = {
    "pbccs.task_options.min_passes":1,
    "pbccs.task_options.min_length":300,
    "pbccs.task_options.min_zscore":-9999,
    "pbccs.task_options.max_drop_fraction":1.0
}

@register_pipeline(to_pipeline_ns("sa3_ds_isoseq_classify"),
                   "SA3 IsoSeq Classify", "0.2.0",
                   tags=("isoseq", ), task_options=ISOSEQ_TASK_OPTIONS)
def ds_isoseq_classify():
    """
    Partial IsoSeq pipeline (classify step only), starting from subreads.
    """
    return _core_isoseq_classify("pbsmrtpipe.pipelines.sa3_ds_ccs:pbccs.tasks.ccs:0")


@register_pipeline(to_pipeline_ns("sa3_ds_isoseq"), "SA3 IsoSeq", "0.2.0",
                   tags=("isoseq", ), task_options=ISOSEQ_TASK_OPTIONS)
def ds_isoseq():
    """
    Main IsoSeq pipeline, starting from subreads.
    """
    return _core_isoseq_cluster("pbsmrtpipe.pipelines.sa3_ds_ccs:pbccs.tasks.ccs:0")


@register_pipeline(to_pipeline_ns("pb_isoseq"), "Internal IsoSeq pipeline",
                   "0.2.0", tags=("isoseq",))
def pb_isoseq():
    """
    Internal IsoSeq pipeline starting from an existing CCS dataset.
    """
    return _core_isoseq_cluster(Constants.ENTRY_DS_CCS)


# XXX will resurrect in the future
#@register_pipeline(to_pipeline_ns("sa3_ds_isoseq_classify_align"),
#                   "SA3 IsoSeq Classification and GMAP Alignment", "0.1.0",
#                   tags=("isoseq", ),
#                   task_options=ISOSEQ_TASK_OPTIONS)
#def ds_isoseq_classify_align():
#    b1 = _core_isoseq_classify("pbsmrtpipe.pipelines.sa3_ds_ccs:pbccs.tasks.ccs:0")
#    b2 = [
#        # full-length, non-chimeric transcripts
#        ("pbtranscript.tasks.classify:1", "pbtranscript.tasks.gmap:0"),
#        (Constants.ENTRY_DS_REF, "pbtranscript.tasks.gmap:1")
#    ]
#    return b1 + b2
#
#
#@register_pipeline(to_pipeline_ns("sa3_ds_isoseq_align"),
#                   "SA3 IsoSeq Pipeline plus GMAP alignment", "0.1.0",
#                   tags=("isoseq", ),
#                   task_options=ISOSEQ_TASK_OPTIONS)
#def ds_isoseq_align():
#    b1 = _core_isoseq_cluster("pbsmrtpipe.pipelines.sa3_ds_ccs:pbccs.tasks.ccs:0")
#    b2 = [
#        # use high-quality isoforms here? or something else?
#        ("pbtranscript.tasks.ice_quiver_postprocess:2",
#         "pbtranscript.tasks.gmap:0"),
#        (Constants.ENTRY_DS_REF, "pbtranscript.tasks.gmap:1")
#    ]
#    return b1 + b2


@register_pipeline(to_pipeline_ns("sa3_ds_subreads_to_fastx"), "SA3 SubreadSet to .fastx Conversion", "0.1.0", tags=("convert",))
def ds_subreads_to_fastx():
    return _core_export_fastx(Constants.ENTRY_DS_SUBREAD)
