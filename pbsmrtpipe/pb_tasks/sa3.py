import logging

from pbsmrtpipe.core import (MetaTaskBase, MetaScatterTaskBase,
                             MetaGatherTaskBase)
from pbcommand.models import FileTypes, TaskTypes, SymbolTypes, ResourceTypes
log = logging.getLogger(__name__)


class ConvertRsMovieMetaDataTask(MetaTaskBase):

    """
    Convert an RS Movie Metadata XML file to a Hdf5 Subread Dataset XML
    """
    TASK_ID = "pbsmrtpipe.tasks.rs_movie_to_hdf5_dataset"
    NAME = "RS Movie to Hdf5 Dataset"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = False

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

    IS_DISTRIBUTED = True

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


class H5SubreadSetScatter(MetaScatterTaskBase):

    """
    Scatter an HDF5SubreadSet.
    """
    TASK_ID = "pbsmrtpipe.tasks.h5_subreadset_scatter"
    NAME = "H5 SubreadSet scatter"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = False

    INPUT_TYPES = [(FileTypes.DS_SUBREADS_H5, "h5_subreads", "H5 Subread DataSet")]

    OUTPUT_TYPES = [(FileTypes.CHUNK, 'cdataset',
                     'Generic Chunked JSON HdfSubreadSet')]

    OUTPUT_FILE_NAMES = [('hdfsubreadset_chunked', 'json'), ]

    NPROC = 1
    SCHEMA_OPTIONS = {}
    RESOURCE_TYPES = None
    NCHUNKS = SymbolTypes.MAX_NCHUNKS
    # Keys that are expected to be written to the chunk.json file
    CHUNK_KEYS = ('$chunk.hdf5subreadset_id', )

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
        exe = "pbtools-chunker hdfsubreadset"
        _d = dict(e=exe,
                  i=input_files[0],
                  o=output_files[0],
                  n=nchunks)
        return "{e} --debug --max-total-chunks {n} {i} {o}".format(**_d)


class GatherGFFTask(MetaGatherTaskBase):

    """Gather GFF Files"""
    TASK_ID = "pbsmrtpipe.tasks.gather_gff"
    NAME = "Gather GFF"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = True

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    # TODO: change this when quiver outputs xmls
    OUTPUT_TYPES = [(FileTypes.GFF, "gff", "Gathered GFF")]
    OUTPUT_FILE_NAMES = [("gathered", "gff")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        # having the chunk key hard coded here is a problem.
        return ('pbtools-gather {t} --debug --chunk-key="{c}" {i} '
                '--output={o}'.format(t='gff', c='gff_id', i=input_files[0],
                                      o=output_files[0]))
        # return 'touch {o}'.format(o=output_files[0])


def _gather_dataset(ds_type, chunk_id, input_file, output_file):
    _d = dict(x=ds_type, c=chunk_id, i=input_file, o=output_file)
    return 'pbtools-gather {x} --debug --chunk-key="{c}" {i} --output={o}'.format(**_d)


class GatherGFFTask2(MetaGatherTaskBase):
    # FIXME. This should be removed once the chunk-keys are properly passed
    # Need the KT tools in the build to debug.
    TASK_ID = "pbsmrtpipe.tasks.gather_gff2"
    NAME = "Gather GFF"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = True

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    OUTPUT_TYPES = [(FileTypes.GFF, "gff", "Gathered GFF")]
    OUTPUT_FILE_NAMES = [("gathered", "gff")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        # having the chunk key hard coded here is a problem.
        return ('pbtools-gather {t} --debug --chunk-key="{c}" {i} '
                '--output={o}'.format(t='gff', c='gff_id', i=input_files[0],
                                      o=output_files[0]))


class GatherContigSetTask(MetaGatherTaskBase):

    """Gather ContigSet Files"""
    TASK_ID = "pbsmrtpipe.tasks.gather_contigset"
    NAME = "Gather ContigSet"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = True

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    # TODO: change this when quiver outputs xmls
    OUTPUT_TYPES = [(FileTypes.DS_CONTIG, "contigset", "Gathered ContigSet")]
    OUTPUT_FILE_NAMES = [("gathered_contigset", "xml")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        # having the chunk key hard coded here is a problem.
        return _gather_dataset('contigset', 'fasta_id', input_files[0], output_files[0])


class GatherSubreadSetTask(MetaGatherTaskBase):

    """Gather SubreadSet Files"""
    TASK_ID = "pbsmrtpipe.tasks.gather_subreadset"
    NAME = "Gather SubreadSet"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = True

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    # TODO: change this when quiver outputs xmls
    OUTPUT_TYPES = [(FileTypes.DS_ALIGN, "ds_bam", "Gathered SubreadSets")]
    OUTPUT_FILE_NAMES = [("gathered", "xml")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        # having the chunk key hard coded here is a problem.
        return _gather_dataset('subreadset', 'subreadset_id', input_files[0], output_files[0])


class GatherAlignmentSetTask(MetaGatherTaskBase):

    """Gather AlignmentSet Files"""
    TASK_ID = "pbsmrtpipe.tasks.gather_alignmentset"
    NAME = "Gather AlignmentSet"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = True

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    OUTPUT_TYPES = [(FileTypes.DS_ALIGN, "ds_bam", "Gathered AlignmentSets")]
    OUTPUT_FILE_NAMES = [("gathered", "xml")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        # having the chunk key hard coded here is a problem.
        return _gather_dataset('alignmentset', 'alignmentset_id', input_files[0], output_files[0])


class AlignmentSetScatterContigs(MetaScatterTaskBase):

    """AlignmentSet scattering by Contigs
    """
    # MK. Inheritance is specifically not allowed

    TASK_ID = "pbsmrtpipe.tasks.alignment_contig_scatter"
    NAME = "AlignmentSet Contig Scatter"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = False
    INPUT_TYPES = [(FileTypes.DS_ALIGN, "alignment_ds", "Pacbio DataSet AlignmentSet"),
                   (FileTypes.DS_REF, "ref_ds", "Reference DataSet file")]

    OUTPUT_TYPES = [(FileTypes.CHUNK, 'cdataset',
                     'Generic Chunked JSON AlignmentSet')]

    OUTPUT_FILE_NAMES = [('alignmentset_chunked', 'json'), ]

    NPROC = 1
    SCHEMA_OPTIONS = {}
    RESOURCE_TYPES = None
    NCHUNKS = SymbolTypes.MAX_NCHUNKS
    # Keys that are expected to be written to the chunk.json file
    CHUNK_KEYS = ('$chunk.alignmentset_id', "$chunk.reference_id")

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
        exe = "pbtools-chunker alignmentset"
        chunk_key = "alignmentset_id"
        mode = "alignmentset"
        _d = dict(e=exe,
                  i=input_files[0],
                  r=input_files[1],
                  o=output_files[0],
                  n=nchunks)
        return "{e} --debug --max-total-chunks {n} {i} {r} {o}".format(**_d)


class AlignmentSetScatterContigsKineticTools(MetaScatterTaskBase):
    """Specific task for scattering for KineticTools"""

    # FIXME. This should be removed once I have a chance to debug this.
    # Need the KT tools in the build first.

    TASK_ID = "pbsmrtpipe.tasks.alignment_contig_scatter2"
    NAME = "AlignmentSet Contig Scatter"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = False
    INPUT_TYPES = [(FileTypes.DS_ALIGN, "alignment_ds", "Pacbio DataSet AlignmentSet"),
                   (FileTypes.DS_REF, "ref_ds", "Reference DataSet file")]

    OUTPUT_TYPES = [(FileTypes.CHUNK, 'cdataset',
                     'Generic Chunked JSON AlignmentSet')]

    OUTPUT_FILE_NAMES = [('alignmentset_chunked', 'json'), ]

    NPROC = 1
    SCHEMA_OPTIONS = {}
    RESOURCE_TYPES = None
    NCHUNKS = SymbolTypes.MAX_NCHUNKS
    # Keys that are expected to be written to the chunk.json file
    CHUNK_KEYS = ('$chunk.alignmentset_id', "$chunk.reference_id")

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
        exe = "pbtools-chunker alignmentset"
        chunk_key = "alignmentset_id"
        mode = "alignmentset"
        _d = dict(e=exe,
                  i=input_files[0],
                  r=input_files[1],
                  o=output_files[0],
                  n=nchunks)
        return "{e} --debug --max-total-chunks {n} {i} {r} {o}".format(**_d)


class SubreadSetScatter(MetaScatterTaskBase):

    """
    Scatter a subreadset to create an Aligned DataSet by calling pbalign/blasr

    Write a subreadset_id and reference_id to chunk.json
    """
    TASK_ID = "pbsmrtpipe.tasks.subreadset_align_scatter"
    NAME = "Scatter Subreadset DataSet"
    VERSION = "0.1.0"

    IS_DISTRIBUTED = False

    INPUT_TYPES = [(FileTypes.DS_SUBREADS, "ds_subreads", "Subread DataSet"),
                   (FileTypes.DS_REF, "ds_reference", "Reference DataSet")]
    OUTPUT_TYPES = [(FileTypes.CHUNK, 'cdataset',
                     'Generic Chunked JSON SubreadSet')]
    OUTPUT_FILE_NAMES = [('subreadset_chunked', 'json'), ]

    NPROC = 1
    SCHEMA_OPTIONS = {}
    RESOURCE_TYPES = None
    NCHUNKS = SymbolTypes.MAX_NCHUNKS
    CHUNK_KEYS = ('$chunk.subreadset_id', "$chunk.reference_id")

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
        exe = "pbtools-chunker subreadset"
        chunk_key = "subreadset_id"
        _d = dict(e=exe,
                  i=input_files[0],
                  r=input_files[1],
                  o=output_files[0],
                  n=nchunks)
        return "{e} --debug --max-total-chunks {n} {i} {r} {o}".format(**_d)


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
