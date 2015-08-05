import logging

from pbcommand.models import FileTypes, TaskTypes
from pbsmrtpipe.core import MetaGatherTaskBase, MetaScatterTaskBase

log = logging.getLogger(__name__)


class GatherCSV(MetaGatherTaskBase):

    """Gather for non-empty CSV files"""
    TASK_ID = "pbsmrtpipe.tasks.gather_csv"
    NAME = "Gather CSV"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = True
    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Chunk JSON")]
    OUTPUT_TYPES = [(FileTypes.CSV, "csv", "CSV File")]

    OUTPUT_FILE_NAMES = [("gathered", "csv")]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = ()

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        _d = dict(i=input_files[0], o=output_files[0])
        return 'pbtools-gather csv --chunk-key="csv_id" {i} --output="{o}"'.format(**_d)


def _gather_fastx(fasta_or_fastq, chunk_id, input_file, output_file):
    _d = dict(x=fasta_or_fastq, c=chunk_id, i=input_file, o=output_file)
    return 'pbtools-gather {x} --debug --chunk-key="{c}" {i} --output={o}'.format(**_d)


class GatherFastaTask(MetaGatherTaskBase):

    """Gather Fasta Files using chunk-key=fasta_id"""
    TASK_ID = "pbsmrtpipe.tasks.gather_fasta"
    NAME = "Gather Fasta"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = True

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    OUTPUT_TYPES = [(FileTypes.FASTA, "fasta", "Gathered Fasta")]
    OUTPUT_FILE_NAMES = [("gathered", "fasta")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        # having the chunk key hard coded here is a problem.
        return _gather_fastx('fasta', 'fasta_id', input_files[0], output_files[0])


class GatherFilterFastaTask(MetaGatherTaskBase):

    """Gather Fasta Files using chunk-key=filtered_fasta_id"""
    TASK_ID = "pbsmrtpipe.tasks.gather_filtered_fasta"
    NAME = "Gather Filtered Fasta"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = True

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    OUTPUT_TYPES = [(FileTypes.FASTA, "fasta", "Gathered Fasta")]
    OUTPUT_FILE_NAMES = [("gathered", "fasta")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        # FIXME.
        # having the chunk key hard coded here is a problem. This will be fixed
        # in subsequent versions. The chunk key will be passed to to_cmd func
        # and general GatherFasta will be used.
        chunk_key = 'filtered_fasta_id'
        return _gather_fastx('fasta', chunk_key, input_files[0], output_files[0])


class GatherFastqTask(MetaGatherTaskBase):

    """Gather Fastq Files"""
    TASK_ID = "pbsmrtpipe.tasks.gather_fastq"
    NAME = "Gather Fastq"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = True

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    OUTPUT_TYPES = [(FileTypes.FASTQ, "fastq", "Gathered Fastq")]
    OUTPUT_FILE_NAMES = [("gathered", "fastq")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        return _gather_fastx('fastq', 'fastq_id', input_files[0], output_files[0])


class GatherGffTask(MetaGatherTaskBase):

    """Gather Gff Files using chunk-key=gff_id"""
    TASK_ID = "pbsmrtpipe.tasks.gather_gff"
    NAME = "Gather Gff"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = True

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    OUTPUT_TYPES = [(FileTypes.GFF, "gff", "Gathered Gff")]
    OUTPUT_FILE_NAMES = [("gathered", "gff")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        return _gather_fastx('gff', 'gff_id', input_files[0], output_files[0])
