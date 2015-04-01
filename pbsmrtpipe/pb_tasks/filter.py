import logging

from pbsmrtpipe.models import ResourceTypes, FileTypes, TaskTypes
from pbsmrtpipe.core import register_task, MetaTaskBase
import pbsmrtpipe.schema_opt_utils as OP
from ._shared_options import GLOBAL_TASK_OPTIONS


log = logging.getLogger(__name__)


class Constants(object):
    READ_SCORE = "filter_read_score"
    MIN_READ_LENGTH = "filter_min_read_length"
    MAX_READ_LENGTH = "filter_max_read_length"
    MIN_SUBREAD_LENGTH = "filter_min_subread_length"
    MAX_SUBREAD_LENGTH = "filter_max_subread_length"
    WHITE_LIST = "filter_whitelist"
    MIN_SNR = "filter_min_snr"
    TRIM = "filter_trim"
    ARTIFACT_SCORE = "filter_artifact_score"


def get_filter_opts():
    d = {}

    read_score_id = OP.to_opt_id(Constants.READ_SCORE)
    d[read_score_id] = OP.to_option_schema(read_score_id, ("number", "null"), "ReadScore", "Filter by ReadScore", None)

    min_length_id = OP.to_opt_id(Constants.MIN_READ_LENGTH)
    d[min_length_id] = OP.to_option_schema(min_length_id, ("integer", "null"), "Min ReadLength", "Filter by Min ReadLength", None)

    max_length_id = OP.to_opt_id(Constants.MAX_READ_LENGTH)
    d[max_length_id] = OP.to_option_schema(max_length_id, ("integer", "null"), "Max ReadLength", "Filter by Max ReadLength", None)

    min_subread_length_id = OP.to_opt_id(Constants.MIN_SUBREAD_LENGTH)
    d[min_subread_length_id] = OP.to_option_schema(min_subread_length_id, ("integer", "null"), "min SubreadLength", "Filter by min SubreadLength", None)

    max_subread_length_id = OP.to_opt_id(Constants.MAX_SUBREAD_LENGTH)
    d[max_subread_length_id] = OP.to_option_schema(max_subread_length_id, ("integer", "null"), "max SubreadLength", "Filter by max SubreadLength", None)

    whitelist_id = OP.to_opt_id(Constants.WHITE_LIST)
    d[whitelist_id] = OP.to_option_schema(whitelist_id, ("string", "null"), "WhiteList ", "Filter by WhiteList file.", None)

    snr_id = OP.to_opt_id(Constants.MIN_SNR)
    d[snr_id] = OP.to_option_schema(snr_id, ("number", "null"), "min SNR", "Filter by min SNR.", None)

    trim_id = OP.to_opt_id(Constants.TRIM)
    d[trim_id] = OP.to_option_schema(trim_id, "boolean", "Use Trim", "Enable Trim option", True)

    artifact_score_id = OP.to_opt_id(Constants.ARTIFACT_SCORE)
    s = OP.to_option_schema(artifact_score_id, ("number", "null"), "Artifact Score", "Filter by Artifact Score", None)
    # Must <= 0
    s['properties'][artifact_score_id]['maximum'] = 0
    d[artifact_score_id] = s

    # Add Global/Shared option
    use_subreads_id = OP.to_opt_id('use_subreads')
    x = GLOBAL_TASK_OPTIONS[use_subreads_id]

    d[use_subreads_id] = x

    return d

# Common filters exposed
_OPT_ID_TO_FILTER_NAME = {Constants.READ_SCORE: "MinReadScore",
                          Constants.MIN_READ_LENGTH: "MinRL",
                          Constants.MAX_READ_LENGTH: "MaxRL",
                          Constants.MIN_SUBREAD_LENGTH: "MinSRL",
                          Constants.MAX_SUBREAD_LENGTH: "MaxSRL",
                          Constants.WHITE_LIST: "ReadWhitelist",
                          Constants.MIN_SNR: "MinSNR"}


def _to_filter_opt_str(ropts):

    f = []
    for opt_id, filter_name in _OPT_ID_TO_FILTER_NAME.iteritems():
        v = ropts[OP.to_opt_id(opt_id)]
        if v is not None:
            f.append("{i}={v}".format(i=filter_name, v=v))

    return "--filter='{s}'".format(s=",".join(f))


class FilterTask(MetaTaskBase):
    TASK_ID = 'pbsmrtpipe.tasks.filter'
    NAME = "Filter Movies"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.MOVIE_FOFN, "movie_fofn", "Movie FOFN"),
                   (FileTypes.REPORT, "rpt", "Movie metadata JSON Report")]

    OUTPUT_TYPES = [(FileTypes.RGN_FOFN, 'rgn_fofn', "Filter Region FOFN"),
                    (FileTypes.CSV, 'csv', "Filter Regions CSV")]
    OUTPUT_FILE_NAMES = [('filtered_regions', 'fofn'), ('filter_subreads', 'csv')]

    SCHEMA_OPTIONS = get_filter_opts()
    NPROC = 1
    RESOURCE_TYPES = (ResourceTypes.TMP_DIR, ResourceTypes.TMP_FILE, ResourceTypes.OUTPUT_DIR)

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):

        # this requires the options to be split into separate values
        filter_opts_str = _to_filter_opt_str(ropts)

        _d = dict(f=filter_opts_str, d=resources[2], c=output_files[1], r=output_files[0], i=input_files[0])
        standard_cmd = "filter_plsh5.py --debug {f} --outputDir={d} --outputSummary={c} --outputFofn={r} {i}"

        cmds = [standard_cmd.format(**_d)]

        artifact_id = OP.to_opt_id(Constants.ARTIFACT_SCORE)
        artifact_score = ropts[artifact_id]

        if artifact_score is not None:
            _ad = dict(a=artifact_score, d=resources[2], c=output_files[1],
                       t=resources[0], i=input_files[0], r=output_files[0])
            artifact_cmd = "filter_artifacts.py --debug --filter='ForwardReverseScore={a}' --outputDir={d} --outputSummary={c} --outputFofn={r} {i} {t}"
            cmds.append(artifact_cmd.format(**_ad))

        return cmds


def get_subread_opts():
     # Add Global/Shared option
    use_subreads_id = OP.to_opt_id('use_subreads')
    x = GLOBAL_TASK_OPTIONS[use_subreads_id]
    return {use_subreads_id: x}


class FilterSubreads(MetaTaskBase):
    TASK_ID = 'pbsmrtpipe.tasks.filter_subreads'
    NAME = "Filter Subreads"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.MOVIE_FOFN, "movie_fofn", "Movie FOFN"),
                   (FileTypes.REPORT, "movie_rpt", "Movie FOFN Report Metadata"),
                   (FileTypes.RGN_FOFN, "rgn_fofn", "Region FOFN"),
                   (FileTypes.REPORT, 'rgn_rpt', "Filtered Region FOFN Report Metadata")]

    OUTPUT_TYPES = [(FileTypes.FASTA, 'fa', "Filter subreads Fasta"),
                    (FileTypes.FASTQ, "fq", "Filter subreads Fastq")]
    OUTPUT_FILE_NAMES = [('filtered_subreads', 'fasta'),
                         ('filtered_subreads', 'fastq')]

    SCHEMA_OPTIONS = get_subread_opts()
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):

        p_opts = ["-trimByRegion", "-regionTable {r} ".format(r=input_files[2])]
        if ropts[OP.to_opt_id('use_subreads')]:
            p_opts.append(" -noSplitSubreads ")

        cmds = []
        _d=dict(e="pls2fasta", i=input_files[0], o=output_files[0], p=" ".join(p_opts))

        # usage: pls2fasta in.bax.h5 out.fasta [options]
        cmd_str = "{e} {i} {o} {p}"
        cmds.append(cmd_str.format(**_d))

        cmd_fq_str = "{e} {i} {o} {p}"
        _d['o'] = "-fastq {x}".format(x=output_files[1])
        cmds.append(cmd_fq_str.format(**_d))

        return cmds


class FilterSubreadSummary(MetaTaskBase):
    TASK_ID = 'pbsmrtpipe.tasks.filter_subread_summary'
    NAME = "Filter Subread Summary"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.RGN_FOFN, "rgn_fofn", "Filter Region FOFN")]
    OUTPUT_TYPES = [(FileTypes.CSV, "csv", "Filter Subreads Summary CSV")]
    OUTPUT_FILE_NAMES = [('filtered_subread_summary', 'csv')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None


    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """Generated Subread summary"""
        exe = "filter_subread_summary.py"
        _d = dict(e=exe, r=input_files[0], o=output_files[0])
        cmd = "{e} {r} --output={o} --debug".format(**_d)
        return cmd
