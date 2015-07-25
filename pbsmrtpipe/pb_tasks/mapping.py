import logging
# MK Notes. This should NOT be Allowed
import os
import re

from pbsmrtpipe.core import MetaTaskBase
from pbsmrtpipe.models import TaskTypes, FileTypes, SymbolTypes, ResourceTypes
import pbsmrtpipe.schema_opt_utils as OP
import pbsmrtpipe.pb_tasks._mapping_opts as AOP
from ._shared_options import GLOBAL_TASK_OPTIONS

log = logging.getLogger(__name__)


class Constants(object):
    EXE_SAMTOOLS = "samtools"
    EXE_BAMTOOLS = "bamtools"


class ReferenceToMetadataReport(MetaTaskBase):

    """Task to create a JSON Metadata Report from a Reference Repo Fasta file

    This should be replaced by the reference Dataset.XML
    """
    TASK_ID = 'pbsmrtpipe.tasks.ref_to_report'
    NAME = "Reference Metadata JSON Report"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.FASTA, 'fasta', "Reference Repo Fasta")]
    OUTPUT_TYPES = [(FileTypes.REPORT, "rpt", "Fasta Reference JSON Report")]
    OUTPUT_FILE_NAMES = [('reference_report', 'json')]

    NPROC = 1
    SCHEMA_OPTIONS = {}
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """
        Converts a Reference to a JSON Report. The report metadata can be
        passed as input to other tasks via $inputs.0.report_attr_id
        """
        # I want this to use the reference.info.xml as input
        exe = "pbtools-converter ref-to-report"
        _d = dict(e=exe, i=input_files[0], o=output_files[0])

        return "{e} --debug {i} {o}".format(**_d)


def _to_cmd_pbalign(input_files, output_files, ropts, nproc, resources):
    align_opts = AOP.to_align_opts_str(ropts, nproc, resources[0])

    align_opts += " --regionTable={r}".format(r=input_files[1])

    # this is stupid
    # Is it a reference
    reference = input_files[2]
    ref_dir = os.path.dirname(os.path.dirname(input_files[2]))
    if os.path.exists(os.path.join(ref_dir, 'reference.info.xml')):
        # pbalign will automatically add the -sa option when it
        # calls blasr if you pass it the reference entry dir
        reference = ref_dir

    exe = "pbalign"
    _d = dict(e=exe,
              r=reference,
              i=input_files[0],
              a=align_opts,
              o=output_files[0])

    cmds = []
    cmds.append('{e} "{i}" "{r}" "{o}" {a}'.format(**_d))

    cmds.append("loadChemistry.py {i} {c}".format(i=input_files[0], c=output_files[0]))

    if ropts[OP.to_opt_id("load_pulses")]:
        lp_opts = ropts[OP.to_opt_id("load_pulses_opts")]
        metric_opts = "-metrics \"{x}\"".format(x=ropts[OP.to_opt_id("load_pulses_metrics")])

        if '-bymetric' in lp_opts:
            by_read_opts = "-bymetric"
        else:
            by_read_opts = ""

        pcmd_str = "loadPulses {i} {c} {m} {r}"
        pcmd = pcmd_str.format(i=input_files[0], c=output_files[0], m=metric_opts, r=by_read_opts)
        cmds.append(pcmd)

    return cmds


class PbAlignTask(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.align"
    NAME = "Align"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.MOVIE_FOFN, 'movie_fofn', 'Movie FOFN'),
                   (FileTypes.RGN_FOFN, 'rgn_fofn', "Region FOFN"),
                   (FileTypes.FASTA, 'fasta_ref', "Pacbio Fasta Reference"),
                   (FileTypes.REPORT, 'rpt', "PacBio Fasta Reference metadata Report")]

    OUTPUT_TYPES = [(FileTypes.ALIGNMENT_CMP_H5, 'cmph5', "Aligned Reads CmpH5")]
    OUTPUT_FILE_NAMES = [('aligned_reads', 'cmp.h5')]

    SCHEMA_OPTIONS = AOP.to_align_schema_opts()
    NPROC = SymbolTypes.MAX_NPROC
    RESOURCE_TYPES = (ResourceTypes.TMP_DIR, )

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):

        cmds = _to_cmd_pbalign(input_files, output_files, ropts, nproc, resources)

        cmds.append("loadChemistry.py {i} {c}".format(i=input_files[0], c=output_files[0]))

        if ropts[OP.to_opt_id("load_pulses")]:
            lp_opts = ropts[OP.to_opt_id("load_pulses_opts")]
            metric_opts = "-metrics \"{x}\"".format(x=ropts[OP.to_opt_id("load_pulses_metrics")])

            if '-bymetric' in lp_opts:
                by_read_opts = "-bymetric"
            else:
                by_read_opts = ""

            pcmd_str = "loadPulses {i} {c} {m} {r}"
            pcmd = pcmd_str.format(i=input_files[0], c=output_files[0], m=metric_opts, r=by_read_opts)
            cmds.append(pcmd)

        return cmds


def _to_cmd_ccs_pbalign(movie_fofn, fasta_ref, align_cmp_h5, ropts, nproc, resources):
    align_opts = AOP.to_align_opts_str(ropts, nproc, resources[0])
    align_opts += ' --useccs=useccsdenovo '

    reference = fasta_ref
    ref_dir = os.path.dirname(os.path.dirname(fasta_ref))
    if os.path.exists(os.path.join(ref_dir, 'reference.info.xml')):
        # pbalign will automatically add the -sa option when it
        # calls blasr if you pass it the reference entry dir
        reference = ref_dir

    exe = "pbalign"
    _d = dict(e=exe,
              r=reference,
              i=movie_fofn,
              a=align_opts,
              o=align_cmp_h5)

    cmds = []
    cmds.append('{e} "{i}" "{r}" "{o}" {a}'.format(**_d))

    cmds.append("loadChemistry.py {i} {c}".format(i=movie_fofn, c=align_cmp_h5))

    if ropts[OP.to_opt_id("load_pulses")]:
        lp_opts = ropts[OP.to_opt_id("load_pulses_opts")]
        metric_opts = "-metrics \"{x}\"".format(x=ropts[OP.to_opt_id("load_pulses_metrics")])

        if '-bymetric' in lp_opts:
            by_read_opts = "-bymetric"
        else:
            by_read_opts = ""

        pcmd_str = "loadPulses {i} {c} {m} {r}"
        pcmd = pcmd_str.format(i=movie_fofn, c=align_cmp_h5, m=metric_opts, r=by_read_opts)
        cmds.append(pcmd)

    return cmds


class PbAlignCCSTask(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.align_ccs"
    NAME = "Reads Of Insert (CCS) Align"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.FOFN, 'movie_fofn', "Movie FOFN"),
                   (FileTypes.FASTA, 'fasta_ref', "Pacbio Fasta Reference"),
                   (FileTypes.REPORT, 'rpt', "Pacbio Fasta Metadata Report")]
    OUTPUT_TYPES = [(FileTypes.ALIGNMENT_CMP_H5, 'cmph5', "Aligned CCS Reads Cmp H5")]
    OUTPUT_FILE_NAMES = [('aligned_ccs_reads', 'cmp.h5')]

    SCHEMA_OPTIONS = AOP.to_align_schema_opts()
    NPROC = SymbolTypes.MAX_NPROC
    RESOURCE_TYPES = (ResourceTypes.TMP_DIR, )

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        return _to_cmd_ccs_pbalign(input_files[0], input_files[1], output_files[0], ropts, nproc, resources)


def _to_sort_opts():
    oid = OP.to_opt_id("cmph5_deep_sort")
    return {oid: OP.to_option_schema(oid, "boolean", "Deep Sort", "CMP.H5 deep sort.", True)}


def _to_coverage_summary_opts():
    d = {}
    oid = OP.to_opt_id("sam_read_groups")
    d[oid] = AOP._to_sam_read_groups()
    nid = OP.to_opt_id("num_stats_regions")
    d[nid] = GLOBAL_TASK_OPTIONS[nid]
    return d


def _to_summarize_coverage_cmd(input_files, output_files, ropts, nproc, resources):
    n = ropts[OP.to_opt_id("num_stats_regions")]
    exe = "summarize_coverage.py"
    _d = dict(e=exe, i=input_files[0], o=output_files[0], n=n)
    return '{e} --numRegions={n} {i} {o}'.format(**_d)


class AlignCoverageSummary(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.coverage_summary"
    NAME = "Align Coverage Summary"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.ALIGNMENT_CMP_H5, "cmph5", "CmpH5 Alignment")]
    OUTPUT_TYPES = [(FileTypes.GFF, "gff", "GFF Coverage Alignment Summary")]
    OUTPUT_FILE_NAMES = [('alignment_summary', 'gff')]

    SCHEMA_OPTIONS = _to_coverage_summary_opts()
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        return _to_summarize_coverage_cmd(input_files, output_files, ropts, nproc, resources)


class GffToBed(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.mapping_gff_to_bed"
    NAME = "Gff to Bed"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.GFF, "gff", "Gff file")]
    OUTPUT_TYPES = [(FileTypes.BED, "bed", "Bed File")]
    OUTPUT_FILE_NAMES = [('coverage', 'bed')]

    NPROC = 1
    SCHEMA_OPTIONS = {}
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        name = 'meanCoverage'
        purpose = 'coverage'
        description = 'Mean coverage of genome in fixed interval regions'
        exe = "gffToBed.py"

        _d = dict(e=exe, i=input_files[0], o=output_files[0], n=name, d=description, p=purpose)
        return 'gffToBed.py --name={n} --description="{d}" {p} {i} > {o}'.format(**_d)


def _to_bridge_opts():
    opt_id = OP.to_opt_id("bridge_mapper.min_affix_length")
    return {opt_id: OP.to_option_schema(opt_id, ("number", "null"), "Min Affix Length", "Bridge Mapper Minimum Affix Length", None)}
