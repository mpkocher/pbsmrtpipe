import logging
import os

from pbsmrtpipe.core import MetaTaskBase
from pbsmrtpipe.models import TaskTypes, FileTypes, ResourceTypes, SymbolTypes
import pbsmrtpipe.schema_opt_utils as OP


log = logging.getLogger(__name__)


def _to_label_zmw_options():
    opts = []

    def _to_o(id_, types_, name_, desc_, default_):
        opts.append(OP.to_option_schema(OP.to_opt_id(id_), types_, name_, desc_, default_))

    _to_o("barcode.score_mode", "string", "Barcode Mode", "Barcode Mode", "symmetric")
    _to_o("barcode.adapter_side_pad", "number", "Barcode Adapter Side Pad", "Barcode Adapter Side Pad", 4)
    _to_o("barcode.insert_side_pad", "number", "Barcode Insert Side Pad", "Barcode Insert Side Pad", 4)
    _to_o("barcode.read_type", "string", "Barcode Read Type", "Barcode Read Type, ('subreads', or, 'CCS')", "subreads")
    _to_o("barcode.score", "number", "Barcode score", "Barcode score to Filter", 0)
    _to_o("barcode.score_first", "boolean", "Score First", "Enable Score First mode", False)

    return {op['required'][0]: op for op in opts}


class BarcodeLabelZmwsTask(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.label_zmws"
    NAME = 'Barcode Label ZMWs'
    VERSION = '0.1.0'

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.MOVIE_FOFN, 'movie_fofn', "Movie FOFN"),
                   (FileTypes.FASTA, 'barcode_fasta', "Pacbio Barcode Fasta")]
    OUTPUT_TYPES = [(FileTypes.FOFN, 'barcode_fofn', 'Barcode FOFN')]
    OUTPUT_FILE_NAMES = [("barcode", "fofn")]

    SCHEMA_OPTIONS = _to_label_zmw_options()
    NPROC = SymbolTypes.MAX_NPROC
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):

        score_mode = ropts[OP.to_opt_id('barcode.score_mode')]
        adapter_side_pad = ropts[OP.to_opt_id('barcode.adapter_side_pad')]
        insert_side_pad = ropts[OP.to_opt_id('barcode.insert_side_pad')]
        read_type = ropts[OP.to_opt_id('barcode.read_type')]
        score_first = ropts[OP.to_opt_id('barcode.score_first')]

        s_first = "" if score_first else " --scoreFirst "

        dir_name = os.path.dirname(output_files[0])

        _d = dict(n=nproc,
                  d=dir_name,
                  b=output_files[0],
                  s=score_mode,
                  x=s_first,
                  a=adapter_side_pad,
                  i=insert_side_pad,
                  fa=input_files[1],
                  m=input_files[0])

        cmd = "pbbarcode -vv labelZmws --nProcs {n} --outFofn {b} --outDir {d} --scoreMode {s} --adapterSidePad {a} --insertSidePad {i} {x} {fa} {m}"

        return cmd.format(**_d)


class GenerateBarcodeFastqsTask(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.generate_barcode_fastqs"
    NAME = 'Generate Barcode Fastqs'
    VERSION = '0.1.0'

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.MOVIE_FOFN, 'movie_fofn', "Movie FOFN"),
                   (FileTypes.FOFN, 'barcode_fofn', 'Barcode FOFN')]
    OUTPUT_TYPES = [FileTypes.FASTQ]
    OUTPUT_FILE_NAMES = [("barcode", "tgz")]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = (ResourceTypes.TMP_DIR, )

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """
        This isn't quite right. Not sure how to handle a tgz'ed dir of fastq
        files
        """
        score = 0
        dir_name = os.path.join(os.path.dirname(output_files[0]), "fstqs")
        _d = dict(m=input_files[0], b=output_files[0], o=dir_name, s=score, q=input_files[1])

        cmds = []
        cmds.append("mkdir {s}".format(s=dir_name))
        cmds.append("pbbarcode  emitFastqs --subreads --minAvgBarcodeScore {s} --outDir {o} {m} {q}".format(**_d))
        cmds.append("cd {s} && tar czf {o} *.fastq".format(s=dir_name, o=output_files[0]))
        cmds.append("rm -rf {d}".format(d=dir_name))
        return cmds


class BarcodeReportTask(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.barcode_report"
    NAME = 'Barcode Report'
    VERSION = '0.1.0'

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.MOVIE_FOFN, 'movie_fofn', 'Movie FOFN'),
                   (FileTypes.FOFN, 'barcode_fofn', 'Barcode FOFN')]
    OUTPUT_TYPES = [(FileTypes.REPORT, 'rpt', "Barcode Report")]
    OUTPUT_FILE_NAMES = [('barcode_report', 'json')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):

        # FIXME
        ccs_mode = False

        exe = "barcode_report.py"
        roi = " --roi " if ccs_mode else ""
        cmd = "{e} --debug {c} {i} {b} {r}".format(e=exe, i=input_files[0],
                                                   b=input_files[1],
                                                   r=output_files[0],
                                                   c=roi)
        log.debug(cmd)
        return cmd
