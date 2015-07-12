import logging

from pbsmrtpipe.core import (MetaTaskBase, MetaScatterTaskBase,
                             MetaGatherTaskBase)
from pbsmrtpipe.models import FileTypes, TaskTypes, ResourceTypes, SymbolTypes
import pbsmrtpipe.schema_opt_utils as OP

log = logging.getLogger(__name__)


def _get_opts():
    oid = OP.to_opt_id('dev.hello_message')
    return {oid: OP.to_option_schema(oid, "string", 'Hello Message', "Hello Message for dev Task.", "Default Message")}


class MyDevHelloTask(MetaTaskBase):
    """A Long Description of My Task Goes here.

    This is development task that cats the input into the output file.
    """
    TASK_ID = 'pbsmrtpipe.tasks.dev_hello_world'
    NAME = "My Hello Task"
    VERSION = "1.0.2"

    TASK_TYPE = TaskTypes.LOCAL
    INPUT_TYPES = [(FileTypes.TXT, "my_txt", "A general non-empty txt file")]
    OUTPUT_TYPES = [(FileTypes.TXT, "my_txt2", "A simple output txt file")]
    SCHEMA_OPTIONS = _get_opts()
    NPROC = 1

    OUTPUT_FILE_NAMES = (("my_text_file", "txt"),)
    RESOURCE_TYPES = ()

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        cmds = []
        cmds.append("cat {i} > {o}".format(i=input_files[0], o=output_files[0]))
        cmds.append('echo "{m}" >> {o}'.format(m=ropts[OP.to_opt_id('dev.hello_message')], o=output_files[0]))
        return cmds


class DevSubreadDataSet(MetaTaskBase):
    """Hello world PacBio Subread DataSet Report"""
    TASK_ID = "pbsmrtpipe.tasks.dev_subread_report"
    NAME = "Dev Subread Report"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL
    INPUT_TYPES = [(FileTypes.DS_SUBREADS, "ds", "Subread DataSet")]
    OUTPUT_TYPES = [(FileTypes.REPORT, "rpt", "Subread Report")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    OUTPUT_FILE_NAMES = (("subread-dataset.report", "json"), )
    RESOURCE_TYPES = ()

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        exe = "python -m pbsmrtpipe.tools.dev dataset-report"
        _d = dict(e=exe, i=input_files[0], o=output_files[0])
        return "{e} --debug {i} {o}".format(**_d)


class DevHelloWorlder(MetaTaskBase):
    TASK_ID = 'pbsmrtpipe.tasks.dev_hello_worlder'
    NAME = "Dev Hello Worlder"
    VERSION = "0.2.1"

    TASK_TYPE = TaskTypes.LOCAL
    INPUT_TYPES = [(FileTypes.TXT, "my_txt", "A simple Text file")]
    OUTPUT_TYPES = [(FileTypes.TXT, "my_out_txt", "A simple Output txt file")]
    SCHEMA_OPTIONS = {}
    NPROC = 2
    RESOURCE_TYPES = (ResourceTypes.TMP_FILE,)

    @staticmethod
    def to_cmd(input_files, output_files, resolved_opts, nproc, resources):
        cmds = []
        cmds.append('echo "creating TMP file" > {x}'.format(x=resources[0]))
        cmds.append('echo "Hello from Dev Hello Worlder" > {o}'.format(o=output_files[0]))
        cmds.append("cat {i} >> {o}".format(i=input_files[0], o=output_files[0]))
        return cmds


class DevHelloGarfield(MetaTaskBase):
    """I hate Mondays"""
    TASK_ID = 'pbsmrtpipe.tasks.dev_hello_garfield'
    NAME = "Dev Hello Garfield"
    VERSION = "0.2.1"

    TASK_TYPE = TaskTypes.LOCAL
    INPUT_TYPES = [(FileTypes.TXT, "my_txt1", "A General Txt file")]

    OUTPUT_TYPES = [(FileTypes.TXT, "my_txt1", "A new General Text file"),
                    (FileTypes.TXT, "txt2", "A general Output text file")]
    OUTPUT_FILE_NAMES = [("file1", "txt"), ("file2", "txt")]

    SCHEMA_OPTIONS = {}
    NPROC = 3
    RESOURCE_TYPES = (ResourceTypes.TMP_FILE, )

    @staticmethod
    def to_cmd(input_files, output_files, resolved_opts, nproc, resources):
        cmds = []
        cmds.append('echo "creating TMP file" > {x}'.format(x=resources[0]))
        cmds.append('echo "Hello from Hello Garfield" > {o}'.format(o=output_files[0]))
        cmds.append("cat {i} >> {o}".format(i=input_files[0], o=output_files[0]))
        cmds.append('echo "Writing to output 2" > {o}'.format(o=output_files[1]))
        return cmds


class DevHelloLasagna(MetaTaskBase):
    """I Create Lasagna"""
    TASK_ID = 'pbsmrtpipe.tasks.dev_hello_lasagna'
    NAME = "Dev Hello Lasagna"
    VERSION = "0.2.1"

    TASK_TYPE = TaskTypes.LOCAL
    INPUT_TYPES = [(FileTypes.TXT, "my_txt1", "A General Txt file")]

    OUTPUT_TYPES = [(FileTypes.TXT, "my_txt1", "A new General Text file")]
    OUTPUT_FILE_NAMES = [("lasanga", "txt")]

    SCHEMA_OPTIONS = {}
    NPROC = 3
    RESOURCE_TYPES = (ResourceTypes.TMP_FILE, )

    @staticmethod
    def to_cmd(input_files, output_files, resolved_opts, nproc, resources):
        cmds = []
        cmds.append('echo "creating TMP file" > {x}'.format(x=resources[0]))
        cmds.append('echo "Hello from Hello Garfield" > {o}'.format(o=output_files[0]))
        cmds.append("cat {i} >> {o}".format(i=input_files[0], o=output_files[0]))
        cmds.append('echo "Writing to output 2" > {o}'.format(o=output_files[1]))
        return cmds


def _di_example_nproc(max_nproc, report_value):
    return max_nproc / 2 + report_value


class DevDependencyInjectExample(MetaTaskBase):
    """
    Simple Example of using the Dependency Injection model to
    dynamically pass data into the task
    """
    TASK_ID = 'pbsmrtpipe.tasks.dev_di_example'
    NAME = "Dependency Inject Example"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.REPORT, 'rpt', "A Report that must contain 'di_example_report.di_example_attr_x' attribute")]
    OUTPUT_TYPES = [(FileTypes.TXT, "txt1", "A txt file with output")]
    OUTPUT_FILE_NAMES = [("di_example_file", "txt")]

    SCHEMA_OPTIONS = {}
    NPROC = ['$max_nproc', '$inputs.0.di_example_report.di_example_attr_x', _di_example_nproc]

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """
        Example DI Example.

        The input report is expected to have attribute key
        'di_example_report.di_example_attr_x' with an (int)
        """
        return "echo \"Di Example {n}\" > {o}".format(n=nproc, o=output_files[0])


class DevTxtToFofnTask(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.dev_txt_to_fofn"
    NAME = "Dev TXT to Fofn"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.TXT, "txt1", "A general txt file")]
    OUTPUT_TYPES = [(FileTypes.FOFN, "fofn1", "A general FOFN file")]
    OUTPUT_FILE_NAMES = [("my_file", "fofn")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """Mock generate a fofn"""
        nfofns = 10
        exe = 'python -m pbsmrtpipe.tools.dev fofn'
        return "{e} --debug --nfofns={n} {o}".format(e=exe, n=nfofns, o=output_files[0])


class DevFofnTask(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.dev_txt_to_fofn_report"
    NAME = "Dev FOFN"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.TXT, "txt", "A general txt file")]
    OUTPUT_TYPES = [(FileTypes.FOFN, "fofn", "A general FOFN"),
                    (FileTypes.REPORT, "rpt", "A general FOFN report")]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = (ResourceTypes.TMP_FILE,)

    @staticmethod
    def to_cmd(input_files, output_files, resolved_opts, nproc, resources):
        cmds = []

        nfofns = 10
        exe = 'python -m pbsmrtpipe.tools.dev fofn'
        cmds.append("{e} --debug --nfofns={n} {o}".format(e=exe, n=nfofns, o=output_files[0]))
        cmds.append("pbtools-converter fofn-to-report --debug {i} {o}".format(i=output_files[0], o=output_files[1]))
        return cmds


def _scatter_fofn_to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
    exe = "pbtools-chunker fofn"
    chunk_key = "fofn"
    mode = "fofn"
    _d = dict(e=exe,
              i=input_files[0],
              o=output_files[0],
              n=nchunks)
    return "{e} --debug --max-total-chunks {n} {i} {o}".format(**_d)


class DevFofnExampleTask(MetaTaskBase):
    """Echo's a msg to a txt file"""
    TASK_ID = "pbsmrtpipe.tasks.dev_fofn_example"
    NAME = "Generic Scatter FOFN"
    VERSION = '1.0.0'

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.FOFN, 'fofn', "Generic Fofn"),
                   (FileTypes.REPORT, "report", "Generic FOFN Metadata Report")]
    OUTPUT_TYPES = [(FileTypes.TXT, "txt", "Generic TXT file")]
    OUTPUT_FILE_NAMES = [("scattered_txt", "txt")]

    SCHEMA_OPTIONS = {}
    NPROC = 2
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        cmds = ["echo \"Scatter Txt \" > {o}".format(o=output_files[0])]
        for input_file in input_files:
            cmds.append("echo \"{i}\" >> {o} ".format(i=input_file, o=output_files[0]))
        return cmds


class DevFofnScatterTask(MetaScatterTaskBase):
    """Statically scatter a fofn to create a chunk.json file"""
    TASK_ID = "pbsmrtpipe.tasks.dev_fofn_scatter"
    NAME = "Scatter FOFN DI example"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.FOFN, 'fofn', "Generic FOFN"),
                   (FileTypes.REPORT, "report", "Fofn metadata JSON Report")]
    OUTPUT_TYPES = [(FileTypes.CHUNK, 'cfofn', 'Generic Chunked JSON Fofn')]
    OUTPUT_FILE_NAMES = [('fofn_chunked', 'json')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    NCHUNKS = SymbolTypes.MAX_NCHUNKS
    # Keys that are expected to be written to the chunk.json file
    CHUNK_KEYS = ('$chunk.txt_id', )

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
        return _scatter_fofn_to_cmd(input_files, output_files, ropts, nproc, resources, nchunks)


def _simple_nchunks_di(max_nchunks, nfofn_records):
    if max_nchunks > 2:
        return max_nchunks / 2
    else:
        return 1


class DevFofnScatterDependencyInjectionTask(MetaScatterTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.dev_fofn_scatter_di"
    NAME = "Scatter FOFN DI example"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.FOFN, 'fofn', "Generic FOFN"),
                   (FileTypes.REPORT, "report", "Fofn metadata JSON Report")]
    OUTPUT_TYPES = [(FileTypes.CHUNK, 'cfofn', 'Generic Chunked JSON Fofn')]
    OUTPUT_FILE_NAMES = [('fofn_chunked', 'json')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    NCHUNKS = [SymbolTypes.MAX_NCHUNKS, '$inputs.0.nrecords', _simple_nchunks_di]
    # Keys that are expected to be written to the chunk.json file
    CHUNK_KEYS = ('$chunk.fofn_id', '$chunk.fofn_report_id')

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
        return _scatter_fofn_to_cmd(input_files, output_files, ropts, nproc, resources, nchunks)


class DevTxtToFastaTask(MetaTaskBase):
    """Generate a random fasta file"""
    TASK_ID = "pbsmrtpipe.tasks.dev_txt_to_fasta"
    NAME = "TXT to Fasta"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL
    INPUT_TYPES = [(FileTypes.TXT, "txt", "Random txt file")]
    OUTPUT_TYPES = [(FileTypes.FASTA, 'fasta', "Random Fasta file")]
    OUTPUT_FILE_NAMES = [("random_file", "fasta")]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        exe = "python -m pbsmrtpipe.tools.dev fasta"
        _d = dict(o=output_files[0], e=exe)
        return "{e} {o}".format(**_d)


class FilterFastaTask(MetaTaskBase):
    """Filter a Fasta file by length of the sequence"""
    TASK_ID = "pbsmrtpipe.tasks.dev_filter_fasta"
    NAME = "Filter Fasta"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.FASTA, 'fasta', "General Fasta File")]
    OUTPUT_TYPES = [(FileTypes.FASTA, 'fasta', "Filtered Fasta File")]
    OUTPUT_FILE_NAMES = [('filtered', 'fasta')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        _d = dict(i=input_files[0], o=output_files[0])
        return "python -m pbsmrtpipe.tools.dev filter-fasta {i} {o}".format(**_d)


def _nchunks_fasta(max_nchunks, nfasta_records):
    return min(nfasta_records, 24)


class ScatterFilterFastaTask(MetaScatterTaskBase):
    """Scatter a filter tasks into a chunks.json file"""
    TASK_ID = "pbsmrtpipe.tasks.dev_scatter_filter_fasta"
    NAME = "Scatter Filter FASTA file"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.FASTA, 'fasta', 'General Fasta File')]
    OUTPUT_TYPES = [(FileTypes.CHUNK, 'chunk', 'Fasta chunks')]
    OUTPUT_FILE_NAMES = [('fasta', 'chunks.json')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    CHUNK_KEYS = ('$chunk.fasta_id',)
    # NCHUNKS = SymbolTypes.MAX_NCHUNKS
    # NCHUNKS = [SymbolTypes.MAX_NCHUNKS, '$inputs.0.nrecords', _nchunks_fasta]
    NCHUNKS = 3

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
        exe = "pbtools-chunker fasta"
        _d = dict(e=exe, i=input_files[0], o=output_files[0], n=nchunks)
        return "{e} --debug --max-total-chunks={n} {i} {o}".format(**_d)


class DevGatherFofnExample(MetaGatherTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.dev_gather_fofn"
    NAME = "Dev Gather FOFN"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.CHUNK, 'jchunk', "Chunked FOFN Json")]
    OUTPUT_TYPES = [(FileTypes.FOFN, 'fofn', "Generic FOFN")]
    OUTPUT_FILE_NAMES = [('gathered_chunk', 'fofn')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        return "echo \"Mock FOFN from chunks\" > {o}".format(o=output_files[0])


class DevHelloDistributedTask(MetaTaskBase):
    TASK_ID = 'pbsmrtpipe.tasks.dev_hello_distributed'
    NAME = "Dev Hello Distributed"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.TXT, 'my_txt', "A general Text file"),
                   (FileTypes.TXT, "txt2", "A general txt file")]
    OUTPUT_TYPES = [(FileTypes.TXT, "mytxt2", "A general text file")]
    OUTPUT_FILE_NAMES = [('myfile', 'txt')]

    SCHEMA_OPTIONS = {}
    NPROC = 3
    RESOURCE_TYPES = ()

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        cmds = []
        for input_file in input_files:
            cmds.append("cat {i} >> {o}".format(i=input_file, o=output_files[0]))
        cmds.append('echo "Ran task dev_hello_distributed" > {o}'.format(o=output_files[0]))
        return cmds


class DevReferenceSetReportTask(MetaTaskBase):
    TASK_ID = 'pbsmrtpipe.tasks.dev_reference_ds_report'
    NAME = "Dev ReferenceSet Report"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.DS_REF, 'ds', "Reference DataSet")]
    OUTPUT_TYPES = [(FileTypes.REPORT, "rpt", "A Report JSON")]
    OUTPUT_FILE_NAMES = [('reference.report', 'json')]

    SCHEMA_OPTIONS = {}
    NPROC = 3
    RESOURCE_TYPES = ()

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        _d = dict(i=input_files[0], o=output_files[0])
        return "python -m pbsmrtpipe.tools.dev reference-ds-report --debug {i} {o}".format(**_d)