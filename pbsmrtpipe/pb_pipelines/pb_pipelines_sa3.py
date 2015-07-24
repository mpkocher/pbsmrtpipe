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


@register_pipeline(to_pipeline_ns("sa3_fetch"), "RS Movie to Subread DataSet")
def sa3_fetch():
    """
    SA3 Convert RS movie metadata XML to Subread DataSet XML
    """

    # convert to RS dataset
    b1 = [(Constants.ENTRY_RS_MOVIE_XML, "pbsmrtpipe.tasks.rs_movie_to_hdf5_dataset:0")]

    b2 = [("pbsmrtpipe.tasks.rs_movie_to_hdf5_dataset:0", "pbsmrtpipe.tasks.h5_subreads_to_subread:0")]

    return b1 + b2


@register_pipeline(to_pipeline_ns("sa3_align"), "SA3 RS movie Align")
def sa3_align():
    """
    SA3 Convert RS movie XML to Alignment DataSet XML
    """
    # convert to RS dataset
    b1 = [(Constants.ENTRY_RS_MOVIE_XML, "pbsmrtpipe.tasks.rs_movie_to_hdf5_dataset:0")]

    # h5 dataset to subread dataset via bax2bam
    b2 = [("pbsmrtpipe.tasks.rs_movie_to_hdf5_dataset:0", "pbsmrtpipe.tasks.h5_subreads_to_subread:0")]

    # Call blasr/pbalign
    b3 = [("pbsmrtpipe.tasks.h5_subreads_to_subread:0", "pbsmrtpipe.tasks.align_ds:0"),
          (Constants.ENTRY_DS_REF, "pbsmrtpipe.tasks.align_ds:1")]

    b4 = [("pbsmrtpipe.tasks.align_ds:0", "pbsmrtpipe.tasks.mapping_ds_report:0")]

    return b1 + b2 + b3 + b4


@register_pipeline(to_pipeline_ns("sa3_resequencing"), "SA3 RS movie Resequencing")
def sa3_resequencing():

    # Generate FASTA metadata report and write contig headers FOFN
    b0 = [(Constants.ENTRY_DS_REF, 'pbsmrtpipe.tasks.ref_to_report:0'),
          (Constants.ENTRY_DS_REF, "pbsmrtpipe.tasks.write_reference_contig_idx_chunks:0"),
          ('pbsmrtpipe.tasks.ref_to_report:0', 'pbsmrtpipe.tasks.write_reference_contig_idx_chunks:1')]

    # Quiver
    b1 = [(Constants.ENTRY_DS_REF, "pbsmrtpipe.tasks.bam_call_variants_with_fastx:0"),
         ("pbsmrtpipe.pipelines.sa3_align:pbsmrtpipe.tasks.align_ds:0", "pbsmrtpipe.tasks.bam_call_variants_with_fastx:1"),
         ("pbsmrtpipe.tasks.write_reference_contig_idx_chunks:0", "pbsmrtpipe.tasks.bam_call_variants_with_fastx:2")]

    return b0 + b1


@register_pipeline(to_pipeline_ns("sa3_hdfsubread_to_subread"), "Convert Hdf SubreadSet to SubreadSet")
def hdf_subread_converter():

    b2 = [(Constants.ENTRY_DS_HDF, "pbsmrtpipe.tasks.h5_subreads_to_subread:0")]

    return b2


@register_pipeline(to_pipeline_ns("sa3_ds_align"), "SA3 SubreadSet Mapping")
def ds_align():

    # Call blasr/pbalign
    b3 = [(Constants.ENTRY_DS_SUBREAD, "pbsmrtpipe.tasks.align_ds:0"),
          (Constants.ENTRY_DS_REF, "pbsmrtpipe.tasks.align_ds:1")]

    b4 = [("pbsmrtpipe.tasks.align_ds:0", "pbreports.tasks.mapping_stats:0")]

    return b3 + b4


@register_pipeline(to_pipeline_ns("sa3_ds_resequencing"), "SA3 SubreadSet Resequencing")
def ds_resequencing():

    # Call consensus
    b1 = [(Constants.ENTRY_DS_REF, "pbsmrtpipe.tasks.bam_call_variants_with_fastx_ds:0"),
         ("pbsmrtpipe.pipelines.sa3_ds_align:pbsmrtpipe.tasks.align_ds:0", "pbsmrtpipe.tasks.bam_call_variants_with_fastx_ds:1")]

    # Consensus Report
    # b3 = [(Constants.ENTRY_DS_REF, "pbsmrtpipe.tasks.ds_variants_report:0"),
    #       ("pbsmrtpipe.tasks.call_variants_with_fastx:0", "pbsmrtpipe.tasks.ds_variants_report:1")]
    #
    # # this won't work until the extension-less issue is sorted out.
    # #b4 = [("pbsmrtpipe.tasks.enrich_summarize_consensus:0", "pbsmrtpipe.tasks.variants_report:1")]
    #
    # # What is the other gff file
    # b5 = [(Constants.ENTRY_DS_REF, "pbsmrtpipe.tasks.ds_variants_report:0"),
    #       ("pbsmrtpipe.tasks.call_variants_with_fastx:0", "pbsmrtpipe.tasks.variants_report:2")]

    return b1


@register_pipeline(to_pipeline_ns("sa3_ds_resequencing_fat"), "SA3 SubreadSet Resequencing With GC Extras and Reports")
def ds_fat_resequencing():
    """DS RS + GC extras and Reports"""

    # Summarize Coverage
    b2 = [("pbsmrtpipe.pipelines.sa3_ds_resequencing:pbsmrtpipe.tasks.align_ds:0", "pbreports.tasks.summarize_coverage:0"),
          (Constants.ENTRY_DS_REF, "pbreports.tasks.summarize_coverage:1")]

    # Consensus Reports - variants
    b3 = [(Constants.ENTRY_DS_REF, "pbreports.tasks.variants_report:0"),
          ("pbreports.tasks.summarize_coverage:0", "pbreports.tasks.variants_report:1"),
          ("pbsmrtpipe.tasks.bam_call_variants_with_fastx_ds:0", "pbreports.tasks.variants_report:2")]

    # Consensus Reports - top variants
    b4 = [("pbsmrtpipe.pipelines.sa3_ds_resequencing:pbsmrtpipe.tasks.bam_call_variants_with_fastx_ds:0", "pbreports.tasks.top_variants:0"),
          (Constants.ENTRY_DS_REF, "pbreports.tasks.top_variants:1")]

    # Consensus Reports - minor top variants
    # b5 = [(Constants.ENTRY_DS_REF, "pbsmrtpipe.tasks.ds_variants_report:0"),
    #      ("pbsmrtpipe.tasks.call_variants_with_fastx:0", "pbsmrtpipe.tasks.variants_report:2")]

    return b2 + b3 + b4


@register_pipeline(to_pipeline_ns("ds_modification_detection"), 'SA3 Modification Detection')
def rs_modification_detection_1():
    """RS Modification Detection"""

    bs = []
    _add = bs.append

    # AlignmentSet, ReferenceSet
    _add(("pbsmrtpipe.pipelines.sa3_ds_resequencing:pbsmrtpipe.tasks.align_ds:0", "kinetics_tools.tasks.ipd_summary:0"))
    _add((Constants.ENTRY_DS_REF, 'kinetics_tools.tasks.ipd_summary:1'))

    # The Report isn't Ported to TCI yet?
    # _add(('kinetics_tools.tasks.ipd_summary:1', 'pbsmrtpipe.tasks.modification_report:0'))

    return bs


@register_pipeline(to_pipeline_ns("ds_modification_motif_analysis"), 'SA3 Modification and Motif Analysis')
def rs_modification_and_motif_analysis_1():
    """Pacbio Official Modification and Motif Analysis Pipeline


    The motif finder contract id needs to have the correct form.
    """
    bs = []
    _add = bs.append

    # Find Motifs. AlignmentSet, ReferenceSet
    _add(('pbsmrtpipe.pipelines.ds_modification_detection:kinetics_tools.tasks.ipd_summary:0', 'kinetic_tools.tasks.motif_maker_find:0'))
    _add((Constants.ENTRY_DS_REF, 'kinetic_tools.tasks.motif_maker_find:1'))

    # Make Motifs. This is not working
    # _add(('pbsmrtpipe.pipelines.ds_modification_detection:kinetics_tools.tasks.ipd_summary:0', 'kinetic_tools.tasks.motif_maker_reprocess:0'))
    # _add(('pbsmrtpipe.pipelines.ds_modification_detection:kinetics_tools.tasks.ipd_summary:1', 'kinetic_tools.tasks.motif_maker_reprocess:1'))
    # _add(('pbsmrtpipe.pipelines.ds_modification_detection:kinetic_tools.tasks.motif_maker_find:0', 'kinetic_tools.tasks.motif_maker_reprocess:2'))
    # _add((Constants.ENTRY_DS_REF, 'kinetic_tools.tasks.motif_maker_reprocess:3'))

    # MK Note. Pat did something odd here that I can't remember the specifics
    # Are the Reports using TCI yet?
    # _add(('pbsmrtpipe.tasks.make_motif_gff:0', 'pbsmrtpipe.tasks.motif_report:0'))
    # _add(('kinetic_tools.tasks.motif_maker_find:0', 'pbsmrtpipe.tasks.motif_report:1'))

    return bs


@register_pipeline(to_pipeline_ns("sa3_sat"), 'SA3 Site Acceptance Test')
def rs_site_acceptance_test_1():
    """Site Acceptance Test"""

    # AlignmentSet, GFF, mapping Report
    x = [("pbsmrtpipe.pipelines.sa3_ds_resequencing:pbsmrtpipe.tasks.align_ds:0", "pbreports.tasks.sat_report:0"),
         ("pbsmrtpipe.pipelines.sa3_ds_resequencing_fat:pbreports.tasks.mapping_stats:0", "pbreports.tasks.sat_report:1"),
         ("pbsmrtpipe.pipelines.sa3_ds_resequencing_fat:pbreports.tasks.variants_report:0", "pbreports.tasks.sat_report:2")]

    return x