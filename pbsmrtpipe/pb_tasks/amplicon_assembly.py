import logging
import os

from pbsmrtpipe.core import MetaTaskBase
from pbsmrtpipe.models import TaskTypes, FileTypes, ResourceTypes, SymbolTypes
import pbsmrtpipe.schema_opt_utils as OP


log = logging.getLogger(__name__)


def _to_amplicon_analysis_options():
    opts = []

    def _add(opt):
        opts.append(opt)

    _add(OP.to_option_schema(OP.to_opt_id("amplicon_assembly.ignore_ends"), "number", "AA ignore ends", "Amplicon Assembly Ignore Ends", 0))
    _add(OP.to_option_schema(OP.to_opt_id("amplicon_assembly.min_length"), "number", "AA min Length", "Amplicon Assembly min Length", 3000))

    _add(OP.to_option_schema(OP.to_opt_id("amplicon_assembly.min_readscore"), "number", "AA min readscore", "Amplicon Assembly min readscore", 0.75))
    _add(OP.to_option_schema(OP.to_opt_id("amplicon_assembly.max_reads"), "integer", "AA max Reads", "Amplicon Assembly Max Reads", 2000))

    _add(OP.to_option_schema(OP.to_opt_id("amplicon_assembly.min_snr"), ("null", "integer"), "AA min SNR", "Amplicon Assembly min SNR", 3))

    _add(OP.to_option_schema(OP.to_opt_id("amplicon_assembly.max_phasing_reads"), "integer", "AA min SNR", "Amplicon Assembly Max PhasingReads", 3000))

    _add(OP.to_option_schema(OP.to_opt_id("amplicon_assembly.take_n"), "integer", "AA min Take N sequences", "Report only the top N sequences (by number of supporting subreads) from each coarse cluster, the remainder will be considered 'noise'", 0))

    _add(OP.to_option_schema(OP.to_opt_id("amplicon_assembly.trim_ends"), "integer", "AA min SNR", "Amplicon Assembly When splitting, ignore N bases at the ends. Use to prevent excessive splitting caused by degenerate primers", 0))

    _add(OP.to_option_schema(OP.to_opt_id("amplicon_assembly.no_clustering"), "boolean", "AA No Clustering", "Amplicon Assembly No Clustering", False))

    _add(OP.to_option_schema(OP.to_opt_id("amplicon_assembly.no_phasing"), "boolean", "AA No Phasing", "Amplicon Assembly No Phasing", False))

    _add(OP.to_option_schema(OP.to_opt_id("amplicon_assembly.force_parallelization"), ("null", "integer"), "AA min SNR", "Amplicon Assembly Do Clustering", 3000))

    return {opt['required'][0]: opt for opt in opts}


def _to_barcode_options():
    opts_d = _to_amplicon_analysis_options()

    opt_id = OP.to_opt_id("amplicon_assembly.min_barcode_score")
    opts_d[opt_id] = OP.to_option_schema(opt_id, "integer", "AA min barcode score", "Amplicon Assembly min barcode score", 0)

    xopt_id = OP.to_opt_id('amplicon_assembly.do_barcodes')
    opts_d[xopt_id] = OP.to_option_schema(xopt_id, ("null", "string"), "AA barcodes", "Amplicon Assembly specific barcode list (common separated)", None)
    return opts_d


def __get_none_opt(ropts, ropt_id, opt_str):
    value = ropts[OP.to_opt_id(ropt_id)]
    render_str_opt = ''
    if value is None:
        render_str_opt = " ".join([' ', opt_str, str(value), ""])

    return render_str_opt


def __get_opt_bool(func, ropts, ropt_id, opt_str):
    value = ropts[OP.to_opt_id(ropt_id)]
    render_str_opt = ''
    if func(value):
        render_str_opt = " ".join([' ', opt_str, ""])

    return render_str_opt


def _get_opt_true_opt(ropts, ropt_id, opt_str):
    return __get_opt_bool(lambda x: x is True, ropts, ropt_id, opt_str)


def _get_opt_false_opt(ropts, ropt_id, opt_str):
    return __get_opt_bool(lambda x: x is False, ropts, ropt_id, opt_str)


def _to_cmd(input_files, output_files, ropts, nproc, resources):

    def _get_opts(ropt_id_, opt_str_):
        return __get_none_opt(ropts, ropt_id_, opt_str_)

    min_length = _get_opts("amplicon_assembly.min_length", '--minLength')
    ignore_ends = _get_opts('amplicon_assembly.ignore_ends', '--ignoreEnds')
    min_readscore = _get_opts('amplicon_assembly.min_readscore', '--minReadScore')

    max_reads = _get_opts('amplicon_assembly.max_reads', '--maxReads')
    min_snr = _get_opts('amplicon_assembly.min_snr', '--minSnr')
    max_phasing_reads = _get_opts('amplicon_assembly.max_phasing_reads', '--maxPhasingReads')
    take_n = _get_opts('amplicon_assembly.take_n', '--takeN')
    trim_ends = _get_opts('amplicon_assembly.trim_ends', '--trimEnds')

    no_clustering = _get_opt_false_opt(ropts, 'amplicon_assembly.no_clustering', '--noClustering')

    no_phasing = _get_opt_true_opt(ropts, 'amplicon_assembly.no_phasing', '--noPhasing')
    force_parallel = _get_opt_true_opt(ropts, 'amplicon_assembly.force_parallelization', '--useTheForce')

    exe = "ConsensusTools AmpliconAnalysis"
    _d = dict(e=exe, i=ignore_ends, r=max_reads,
              l=min_length, s=min_readscore,
              m=min_snr, h=max_phasing_reads,
              nc=no_clustering, np=no_phasing,
              n=nproc, te=trim_ends, tn=take_n, fp=force_parallel)
    opt_str = "{e} -vv {i} {r} {l} {s} {m} {h} {nc} {np} --numThreads {n} {te} {tn} {fp}"
    x = opt_str.format(**_d)

    output_dir = os.path.dirname(output_files[0])

    cmd = "{e} -o {o} --fofn {f} --logFile {l}".format(e=x, o=output_dir, f=input_files[0], l=resources[0])
    return cmd


class AmpliconAssemblyTask(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.amplicon_assembly"
    NAME = "Amplicon Assembly"
    VERSION = '0.1.0'

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.MOVIE_FOFN, 'movie_fofn', 'Movie Fofn')]
    OUTPUT_TYPES = [(FileTypes.FASTA, 'fasta', 'Assembly Fasta'),
                    (FileTypes.FASTQ, 'fastq', 'Assembly Fastq'),
                    (FileTypes.CSV, 'csv', 'Assembly CSV'),
                    (FileTypes.CSV, 'summary_csv', 'Assembly summary CSV'),
                    (FileTypes.CSV, 'zmw_summary_csv', 'ZMW Assembly Summary CSV')]
    # Note AA is hardcoded to write these file names
    OUTPUT_FILE_NAMES = [('amplicon_analysis', 'fasta'),
                         ('amplicon_analysis', 'fastq'),
                         ('amplicon_analysis', 'csv'),
                         ('amplicon_analysis_summary', 'csv'),
                         ('amplicon_analysis_zmws', 'csv')]

    SCHEMA_OPTIONS = _to_amplicon_analysis_options()
    NPROC = SymbolTypes.MAX_NPROC
    RESOURCE_TYPES = (ResourceTypes.LOG_FILE, )

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """
        Outputs: (Fasta, Fastq, Amplicon CSV, Amplicon Summary CSV, Amplicon ZMW Summary
        """
        return _to_cmd(input_files, output_files, ropts, nproc, resources)


class AmpliconAssemblyBarcode(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.amplicon_assembly_barcode"
    NAME = "Amplicon Analysis Barcode"
    VERSION = '0.1.0'

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.MOVIE_FOFN, 'movie_fofn', 'Movie FOFN'),
                   (FileTypes.FOFN, 'fofn', 'PacBio FOFN Barcode')]
    OUTPUT_TYPES = [(FileTypes.FASTA, 'fasta', 'Assembly Fasta'),
                    (FileTypes.FASTQ, 'fastq', 'Assembly Fastq'),
                    (FileTypes.CSV, 'csv', 'Assembly CSV'),
                    (FileTypes.CSV, 'summary_csv', 'Assembly summary CSV'),
                    (FileTypes.CSV, 'zmw_summary_csv', 'ZMW Assembly Summary CSV')]
    # Note AA is hardcoded to write these file names
    OUTPUT_FILE_NAMES = [('amplicon_analysis', 'fasta'),
                         ('amplicon_analysis', 'fastq'),
                         ('amplicon_analysis', 'csv'),
                         ('amplicon_analysis_summary', 'csv'),
                         ('amplicon_analysis_zmws', 'csv')]

    SCHEMA_OPTIONS = _to_barcode_options()
    NPROC = SymbolTypes.MAX_NPROC
    RESOURCE_TYPES = (ResourceTypes.LOG_FILE, )

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        cmd = _to_cmd(input_files, output_files, ropts, nproc, resources)

        _barcode_score = ropts[OP.to_opt_id('amplicon_assembly.min_barcode_score')]
        barcode_score = ' --minBarcodeScore {x}'.format(x=_barcode_score)
        _do_barcodes = ropts[OP.to_opt_id('amplicon_assembly.do_barcodes')]

        barcodesArg = ''
        if _do_barcodes is not None:
            barcodesArg = " ".join(" --doBc {0} ".format(bc.strip()) for bc in _do_barcodes.split(',') if bc.strip())

        return "{c} --barcodes {b} {x}".format(c=cmd, b=input_files[1], x=barcodesArg)


class AmpliconAssemblyReportTask(MetaTaskBase):
    TASK_ID = 'pbsmrtpipe.tasks.amplicon_assembly_report'
    NAME = 'Amplicon Assembly Report'
    VERSION = '0.1.0'

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.CSV, 'csv', 'Amplicon CSV')]
    OUTPUT_TYPES = [(FileTypes.REPORT, 'rpt', 'Amplicon Report')]
    OUTPUT_FILE_NAMES = [('amplicon_assembly_report', 'json')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """
        Task to generate a report for the Amplicon Assembly results
        """

        exe = "long_amplicon_analysis_report.py"
        _d = dict(i=input_files[0], o=output_files[0], e=exe)
        cmd = "{e} --debug {i} {r}".format(**_d)
        return cmd


class AmpliconAssemblyInputReport(MetaTaskBase):
    TASK_ID = 'pbsmrtpipe.tasks.amplicon_assembly_input_report'
    NAME = 'Amplicon Assembly Input Report'
    VERSION = '0.1.0'

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.CSV, 'csv', 'Amplicon Summary CSV'),
                   (FileTypes.CSV, 'csv', 'Amplicon ZMW Summary CSV')]
    OUTPUT_TYPES = [(FileTypes.REPORT, 'rpt', 'Amplicon Input Report')]
    OUTPUT_FILE_NAMES = [('amplicon_analysis_input_report', 'json')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """
        Inputs: (Amplicon Summary CSV, Amplicon Zmw Summary CSV)

        """
        exe = "amplicon_analysis_input_report.py"
        _d = dict(e=exe, s=input_files[0], z=input_files[1], r=output_files[0])
        cmd = "{e} --debug {s} {z} {r}".format(**_d)
        return cmd
