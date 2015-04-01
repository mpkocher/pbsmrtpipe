import logging

from pbsmrtpipe.core import MetaTaskBase
from pbsmrtpipe.models import FileTypes, TaskTypes, ResourceTypes, SymbolTypes
import pbsmrtpipe.schema_opt_utils as OP

log = logging.getLogger(__name__)


def _to_cmd(input_files, output_files, ropts, nproc, resources):
    """Convert a generic FOFN to a Report """
    _d = dict(i=input_files[0], o=output_files[0])
    return "pbtools-converter fofn-to-report --debug {i} {o}".format(**_d)


class ConvertFofnToReport(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.fofn_to_report"
    NAME = "FOFN to pbreport JSON"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.FOFN, "my_fofn", "A Generic Fofn")]
    OUTPUT_TYPES = [(FileTypes.REPORT, "rpt", "A FOFN report file")]
    OUTPUT_FILE_NAMES = [("fofn_report", "json")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        return _to_cmd(input_files, output_files, ropts, nproc, resources)


class ConvertRegionFofnToReport(MetaTaskBase):
    # need to think about how to handle generic conversion will remaining
    # somewhat typesafe
    TASK_ID = "pbsmrtpipe.tasks.rgn_fofn_to_report"
    NAME = "Region FOFN to pbreport JSON"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.RGN_FOFN, "my_rgn_fofn", "A Region Fofn")]
    OUTPUT_TYPES = [(FileTypes.REPORT, "rpt", "A FOFN report file")]
    OUTPUT_FILE_NAMES = [("fofn_report", "json")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        return _to_cmd(input_files, output_files, ropts, nproc, resources)
