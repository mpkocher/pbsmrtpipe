import logging

from pbsmrtpipe.core import register_pipeline
from pbsmrtpipe.constants import to_pipeline_ns, ENTRY_PREFIX

from .pb_pipelines import Constants

log = logging.getLogger(__name__)


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


@register_pipeline(to_pipeline_ns("sa3_ds_align"), "SA3 Resequencing")
def ds_align():

    # Call blasr/pbalign
    b3 = [(Constants.ENTRY_DS_SUBREAD, "pbsmrtpipe.tasks.align_ds:0"),
          (Constants.ENTRY_DS_REF, "pbsmrtpipe.tasks.align_ds:1")]

    b4 = [("pbsmrtpipe.tasks.align_ds:0", "pbsmrtpipe.tasks.mapping_ds_report:0")]

    return b3 + b4


@register_pipeline(to_pipeline_ns("sa3_ds_resequencing"), "SA3 Resequencing")
def ds_resequencing():

    # Generate FASTA metadata report and write contig headers FOFN
    b0 = [(Constants.ENTRY_DS_REF, 'pbsmrtpipe.tasks.ref_to_report:0'),
          (Constants.ENTRY_DS_REF, "pbsmrtpipe.tasks.write_reference_contig_idx_chunks:0"),
          ('pbsmrtpipe.tasks.ref_to_report:0', 'pbsmrtpipe.tasks.write_reference_contig_idx_chunks:1')]

    b4 = [("pbsmrtpipe.tasks.align_ds:0", "pbsmrtpipe.tasks.mapping_ds_report:0")]

    # Call consensus
    b1 = [(Constants.ENTRY_DS_REF, "pbsmrtpipe.tasks.bam_call_variants_with_fastx:0"),
         ("pbsmrtpipe.pipelines.sa3_ds_align:pbsmrtpipe.tasks.align_ds:0", "pbsmrtpipe.tasks.bam_call_variants_with_fastx:1"),
         ("pbsmrtpipe.tasks.write_reference_contig_idx_chunks:0", "pbsmrtpipe.tasks.bam_call_variants_with_fastx:2")]

    return b0 + b1 + b4
