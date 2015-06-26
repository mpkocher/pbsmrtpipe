import logging
import os

from pbsmrtpipe.core import MetaTaskBase
from pbsmrtpipe.models import FileTypes, TaskTypes, SymbolTypes, ResourceTypes
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
    OUTPUT_FILE_NAMES = [("file", "dataset.subreads_h5.xml")]

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
    VERSION = "0.1.1"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.DS_SUBREADS_H5, "h5_subreads", "H5 Subread DataSet")]
    OUTPUT_TYPES = [(FileTypes.DS_SUBREADS, "ds", "Subread DataSet")]
    OUTPUT_FILE_NAMES = [("file", "dataset.subreads.xml")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        e = "bax2bam"
        # Doesn't support writing to an explicit file yet
        cmds = []
        cmds.append("{e} --subread --xml {i} ".format(e=e, i=input_files[0]))
        # FIXME when derek updates the interface
        cmds.append("x=$(ls -1t *.dataset.xml | head -n 1) && cp $x {o}".format(o=output_files[0]))
        return cmds


class AlignDataSetTask(MetaTaskBase):
    """
    Create an Aligned DataSet by calling pbalign/blasr
    """
    TASK_ID = "pbsmrtpipe.tasks.align_ds"
    NAME = "Align DataSet"
    VERSION = "0.1.1"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.DS_SUBREADS, "rs_movie_metadata", "A RS Movie metadata.xml"),
                   (FileTypes.DS_REF, "ds_reference", "Reference DataSet")]
    OUTPUT_TYPES = [(FileTypes.DS_ALIGNMENT, "bam", "Aligned BAM")]
    OUTPUT_FILE_NAMES = [("file", "aligned.bam")]

    SCHEMA_OPTIONS = {}
    NPROC = SymbolTypes.MAX_NPROC

    RESOURCE_TYPES = (ResourceTypes.TMP_FILE, )

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        e = "pbalign"
        # FIXME.
        d = os.path.dirname(output_files[0])
        tmp_bam = os.path.join(d, 'tmp.bam')
        cmds = []
        cmds.append("{e} --verbose --nproc={n} {i} {r} {t}".format(e=e, i=input_files[0], n=nproc, r=input_files[1], t=tmp_bam))
        # this auto naming stuff is nonsense
        cmds.append("samtools sort {t} sorted".format(t=tmp_bam))
        cmds.append("mv sorted.bam {o}".format(o=output_files[0]))
        cmds.append('samtools index {o}'.format(o=output_files[0]))
        return cmds


class MappingReportTask(MetaTaskBase):
    """
    Create a Alignment Report from a Alignment DataSet
    """
    TASK_ID = "pbsmrtpipe.tasks.mapping_ds_report"
    NAME = "Mapping DataSet Report"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.DS_ALIGNMENT, "ds", "Alignment DataSet")]
    OUTPUT_TYPES = [(FileTypes.REPORT, "rpt", "Alignment Mapping Report")]
    OUTPUT_FILE_NAMES = [("mapping_report", "json")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        exe = "mapping_stats"
        # this needs to be changed
        output_dir = os.path.dirname(output_files[0])
        report_json = os.path.basename(output_files[0])
        _d = dict(e=exe, i=input_files[0], o=output_dir, j=report_json)
        return "{e} --debug {i} --output {o} {j}".format(**_d)