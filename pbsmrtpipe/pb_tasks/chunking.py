import logging
import os

from pbsmrtpipe.models import SymbolTypes, FileTypes, TaskTypes
from pbsmrtpipe.core import MetaGatherTaskBase, MetaScatterTaskBase

log = logging.getLogger(__name__)


def _scatter_filter_nchunks(max_chunks, nfofn):
    return min(nfofn, max_chunks)


class FilterScatterTask(MetaScatterTaskBase):
    """Filter Scatter FOFN files"""
    TASK_ID = "pbsmrtpipe.tasks.filter_scatter"
    NAME = "Scatter Movie FOFN"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL
    INPUT_TYPES = [(FileTypes.MOVIE_FOFN, "movie_fofn", "Movie FOFN"),
                   (FileTypes.REPORT, "rpt", "Report FOFN metadata")]
    OUTPUT_TYPES = [(FileTypes.CHUNK, "chunk", "CHUNK scatter File")]

    OUTPUT_FILE_NAMES = [('filter_scatter', 'chunk.json')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = ()

    NCHUNKS = [SymbolTypes.MAX_NCHUNKS, '$inputs.1.input_xml_report.nfofns', _scatter_filter_nchunks]
    CHUNK_KEYS = ("fofn_id", "rgn_fofn_id")

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
        exe = "pbtools-chunker"
        _d = dict(e=exe, i=input_files[0], o=output_files[0], n=nchunks)
        return "{e} fofn --debug --max-total-chunks={n} {i} {o}".format(**_d)


class GatherCSV(MetaGatherTaskBase):
    """Gather for non-empty CSV files"""
    TASK_ID = "pbsmrtpipe.tasks.gather_csv"
    NAME = "Gather CSV"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL
    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Chunk JSON")]
    OUTPUT_TYPES = [(FileTypes.CSV, "csv", "CSV File")]

    OUTPUT_FILE_NAMES = [("gathered", "csv")]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = ()

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        _d = dict(i=input_files[0], o=output_files[1])
        return 'pbtools-gather csv --chunk-key="csv_id" {i} --output="{o}"'.format(**_d)


class GatherRegionFofn(MetaGatherTaskBase):
    """Gather FOFN """
    TASK_ID = "pbsmrtpipe.tasks.gather_fofn"
    NAME = "Gather REGION FOFN"
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
        cmds = []
        cmds.append('pbtools-gather fofn --chunk-key="fofn_id" {i} --output="{o}"'.format(i=input_files[0], o=output_files[0]))
        return cmds


def _gather_fastx(fasta_or_fastq, chunk_id, input_file, output_file):
    _d = dict(x=fasta_or_fastq, c=chunk_id, i=input_file, o=output_file)
    return 'pbtools-gather {x} --debug --chunk-key="{c}" {i} --output={o}'.format(**_d)


class GatherFastaTask(MetaGatherTaskBase):
    """Gather Fasta Files"""
    TASK_ID = "pbsmrtpipe.tasks.gather_fasta"
    NAME = "Gather Fasta"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    OUTPUT_TYPES = [(FileTypes.FASTA, "fasta", "Gathered Fasta")]
    OUTPUT_FILE_NAMES = [("gathered", "fasta")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        # having the chunk key hard coded here is a problem.
        return _gather_fastx('fasta', 'fasta_id', input_files[0], output_files[0])


class GatherFastqTask(MetaGatherTaskBase):
    """Gather Fastq Files"""
    TASK_ID = "pbsmrtpipe.tasks.gather_fastq"
    NAME = "Gather Fastq"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    OUTPUT_TYPES = [(FileTypes.FASTQ, "fastq", "Gathered Fastq")]
    OUTPUT_FILE_NAMES = [("gathered", "fastq")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        return _gather_fastx('fastq', 'fastq_id', input_files[0], output_files[0])


class GatherGffTask(MetaGatherTaskBase):
    """Gather Fastq Files"""
    TASK_ID = "pbsmrtpipe.tasks.gather_gff"
    NAME = "Gather Gff"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    OUTPUT_TYPES = [(FileTypes.GFF, "gff", "Gathered Gff")]
    OUTPUT_FILE_NAMES = [("gathered", "gff")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        return _gather_fastx('gff', 'gff_id', input_files[0], output_files[0])
