import logging

from pbsmrtpipe.core import MetaTaskBase
from pbsmrtpipe.models import FileTypes, TaskTypes, SymbolTypes
#import _mapping_opts as AOPTS

log = logging.getLogger(__name__)


class ConvertRsMovieMetaDataTask(MetaTaskBase):
    """
    Convert an RS Movie Metadata XML file to a Hdf5 Subread Dataset XML
    """
    TASK_ID = "pbsmrtpipe.tasks.rs_movie_to_hdf5_dataset"
    NAME = "RS Movie to Hdf5 Dataset"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.RS_MOVIE_XML, "rs_movie_metadata", "A RS Movie metadata.xml")]
    OUTPUT_TYPES = [(FileTypes.DS_SUBREADS_H5, "ds", "DS H5 Subread.xml")]
    OUTPUT_FILE_NAMES = [("subreads", "dataset.subreads_h5.xml")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        e = "movie-metadata-to-dataset"
        return "{e} --debug {i} {o}".format(e=e, i=input_files[0], o=output_files[0])


class ConvertH5SubreadsToBamDataSetTask(MetaTaskBase):
    """
    Convert a Hdf5 Dataset to an Unaligned Bam DataSet XML
    """
    TASK_ID = "pbsmrtpipe.tasks.h5_subreads_to_subread"
    NAME = "H5 Dataset to Subread Dataset"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.DS_SUBREADS_H5, "h5_subreads", "H5 Subread DataSet")]
    OUTPUT_TYPES = [(FileTypes.DS_SUBREADS, "ds", "Subread DataSet")]
    OUTPUT_FILE_NAMES = [("subreads", "dataset.subreads_h5.xml")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        e = "bax2bam"
        return "{e} --debug --subread --xml {i} {o}".format(e=e, i=input_files[0], o=output_files[0])


class AlignDataSetTask(MetaTaskBase):
    """
    Create an Aligned DataSet by calling pbalign/blasr
    """
    TASK_ID = "pbsmrtpipe.tasks.align_ds"
    NAME = "Align DataSet"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.DS_SUBREADS, "rs_movie_metadata", "A RS Movie metadata.xml"),
                   (FileTypes.DS_REF, "ds_reference", "Reference DataSet")]
    OUTPUT_TYPES = [(FileTypes.DS_BAM, "ds", "DS H5 Subread.xml")]
    OUTPUT_FILE_NAMES = [("subreads", "dataset.subreads_h5.xml")]

    SCHEMA_OPTIONS = {}
    NPROC = SymbolTypes.MAX_NPROC

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        e = "movie-metadata-to-dataset"
        return "{e} --debug {i} {o}".format(e=e, i=input_files[0], o=output_files[0])
