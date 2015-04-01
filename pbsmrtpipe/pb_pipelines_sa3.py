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


@register_pipeline(to_pipeline_ns("sa3_align"), "SA3 Align")
def sa3_align():
    """
    SA3 Convert RS movie XML to Alignment DataSet XML
    """
    # convert to RS dataset
    b1 = [(Constants.ENTRY_RS_MOVIE_XML, "pbsmrtpipe.tasks.rs_movie_to_hdf5_dataset:0")]

    b2 = [("pbsmrtpipe.tasks.rs_movie_to_hdf5_dataset:0", "pbsmrtpipe.tasks.h5_subreads_to_subread:0")]

    # Call blasr/pbalign
    b3 = [("pbsmrtpipe.tasks.h5_subreads_to_subread:0", "pbsmrtpipe.tasks.align_ds:0"),
          (Constants.ENTRY_REF_DS, "pbsmrtpipe.tasks.align_ds:1")]

    return b1 + b2 + b3
