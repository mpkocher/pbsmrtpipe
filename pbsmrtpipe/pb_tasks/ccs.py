import logging
import os

from pbsmrtpipe.core import MetaTaskBase
from pbsmrtpipe.models import TaskTypes, FileTypes, SymbolTypes
import pbsmrtpipe.schema_opt_utils as OP


log = logging.getLogger(__name__)


def _to_o(id_, types_, name_, desc_, default_):
    return OP.to_option_schema(OP.to_opt_id(id_), types_, name_, desc_, default_)


def _opt_list_to_d(opt_list):
    return {opt['required'][0]: opt for opt in opt_list}


def _to_min_passes():
    return _to_o("ccs.minFullPasses", ("null", "number"), "CCS min full passes", "CCS Number of min Full Passes", None)


def _to_minAccuracy():
    return _to_o('ccs.minAccuracy', ("null", "number"), "CCS Min Predicted Accuracy", "CCS Minimum Predicted Accuracy", None)


def _to_minLength():
    return _to_o('ccs.minLength', ("null", "number"), "CCS Min length", "CCS Minimum Length", None)


def _to_maxLength():
    return _to_o('ccs.maxLength', ('null', 'number'), "Max Length", 'Max Length', None)


def _to_roi_options():
    opts = [_to_min_passes(), _to_minAccuracy(), _to_minLength(), _to_maxLength()]
    return _opt_list_to_d(opts)


def _to_opt_or_emtpy(value, str_value):
    return " " if value is None else " {s} {v}".format(s=str_value, v=value)


class ReadsOfInsertTask(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.reads_of_insert"
    NAME = "Reads of Insert (CCS)"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.MOVIE_FOFN, 'movie_fofn', "Movie FOFN")]
    OUTPUT_TYPES = [(FileTypes.FOFN, "ccs_fofn", 'CCS FOFN'),
                    (FileTypes.FASTA, "ccs_fasta", 'CCS Fasta'),
                    (FileTypes.FASTQ, 'ccs_fastq', 'CCS Fastq')]
    OUTPUT_FILE_NAMES = [("ccs", "fofn"), ('ccs', 'fasta'), ('css', 'fastq')]

    SCHEMA_OPTIONS = _to_roi_options()
    NPROC = SymbolTypes.MAX_NPROC
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):

        task_dir = os.path.dirname(output_files[0])

        def _to_v(id_, str_value_):
            return _to_opt_or_emtpy(ropts[OP.to_opt_id(id_)], str_value_)

        minFullPasses = _to_v('ccs.minFullPasses', '--minFullPasses')
        min_pred_accuracy = _to_v('ccs.minAccuracy', '--minPredictedAccuracy')
        minLength = _to_v('ccs.minLength', '--minLength')
        maxLength = _to_v('ccs.maxLength', '--maxLength')

        exe = "ConsensusTools CircularConsensus"
        _d = dict(e=exe,
                  p=minFullPasses,
                  a=min_pred_accuracy,
                  n=nproc,
                  i=input_files[0],
                  o=task_dir,
                  min_l=minLength,
                  max_l=maxLength)
        cmd_str = "{e} {p} {a} {min_l} {max_l} --numThreads {n} --fofn {i} -o {o}"

        cmds = [cmd_str.format(**_d)]
        cmds.append("for i in $(ls {d}/*.ccs.h5); do readlink -f $i >> {c}; done".format(d=task_dir, c=output_files[0]))

        cmds.append("rm -rf {f}".format(f=output_files[1]))
        cmds.append("for i in $(ls {d}/*.ccs.fasta); do cat $i >> {c}; done".format(d=task_dir, c=output_files[1]))
        cmds.append("rm -rf {f}".format(f=output_files[2]))
        cmds.append("for i in $(ls {d}/*.ccs.fastq); do cat $i  >> {c}; done".format(d=task_dir, c=output_files[2]))

        return cmds


class ReadsOfInsertReportTask(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.roi_report"
    NAME = "Reads of Insert Report"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.FOFN, 'ccs_fofn', "CCS Fofn")]
    OUTPUT_TYPES = [(FileTypes.REPORT, "ccs_rpt", "CCS Report")]
    OUTPUT_FILE_NAMES = [('ccs_report', 'json')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):

        results_dir = os.path.dirname(output_files[0])

        exe = "reads_of_insert_report.py"

        _d = dict(e=exe,
                  i=input_files[0],
                  r=output_files[0],
                  d=results_dir)

        cmd = "{e} --debug --output-dir {d} {i} {r}".format(**_d)
        return cmd


def _to_minor_variants_opts():

    opts = [_to_minLength(), _to_minAccuracy()]

    opts.append(_to_o('ccs.maxMolecules', ("null", "int"), "Max Molecules", "Maximum Number of Molecules", None))
    opts.append(_to_o('ccs.minCoverage', ("null", "int"), "Min Converge", "Minimum Coverage", None))
    opts.append(_to_o('ccs.maxPValue', ("null", "int"), "Max P-Value", "Maximum P-value", None))
    opts.append(_to_o('ccs.confidenceInterval', ("null", "int"), "Confidence Interval", "Confidence Interval", None))
    opts.append(_to_o('ccs.aminoVariants', "boolean", "Amino Variants", "Amino Variants", False))
    opts.append(_to_o('ccs.countAmbiguous', "boolean", "Count Ambiguous", "Count Ambiguous", False))
    return _opt_list_to_d(opts)


class CallMinorVariants(MetaTaskBase):

    """
    Performs minor variant calling from a cmp.h5 generated from a user-
    provided reference.
    """
    TASK_ID = "pbsmrtpipe.tasks.call_minor_variants"
    NAME = "Call Minor Variants"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.ALIGNMENT_CMP_H5, "cmp_h5", "Alignment CMP H5"),
                   (FileTypes.FASTA, 'fasta', "Pacbio Fasta Reference")]
    OUTPUT_TYPES = [(FileTypes.VCF, 'vcf', 'Variants VCF'),
                    (FileTypes.CSV, 'csv', 'Variants CSV')]
    OUTPUT_FILE_NAMES = [('minor_variants', 'vcf'), ('minor_variants', 'csv')]

    SCHEMA_OPTIONS = _to_roi_options()
    NPROC = SymbolTypes.MAX_NPROC
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        output_dir = os.path.dirname(output_files[0])

        def _to_v(id_, str_value_):
            return _to_opt_or_emtpy(ropts[OP.to_opt_id(id_)], str_value_)

        exe = "ConsensusTools MinorVariants"
        _d = dict(exe=exe,
                  nproc=nproc,
                  Mm=_to_v('ccs.maxMolecules', '--maxMolecules'),
                  ml=_to_v('ccs.minLength', '--minLength'),
                  mc=_to_v('ccs.minCoverage', '--minCoverage'),
                  ma=_to_v('ccs.minAccuracy', '--minAccuracy'),
                  Mpv=_to_v('ccs.maxPValue', '--maxPValue'),
                  ci=_to_v('ccs.confidenceInterval', '--confidenceInterval'),
                  av=_to_v('ccs.aminoVariants', '--aminoVariants '),
                  ca=_to_v('ccs.countAmbiguous', '--countAmbiguous'),
                  ref=input_files[1],
                  out=output_dir,
                  cmp=input_files[0])

        return "{exe} -n {nproc} {Mm} {ml} {mc} {ma} {Mpv} {ci} {av} {ca} -r {ref} -o {out} --cmp {cmp}".format(**_d)
