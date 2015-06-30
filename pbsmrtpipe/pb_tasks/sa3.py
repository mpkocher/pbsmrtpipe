import logging
import os

from pbsmrtpipe.core import MetaTaskBase, MetaScatterTaskBase
from pbsmrtpipe.models import FileTypes, TaskTypes, SymbolTypes, ResourceTypes
#import _mapping_opts as AOPTS
import pbsmrtpipe.schema_opt_utils as OP
from pbsmrtpipe.pb_tasks.genomic_consensus import _to_call_variants_opts_schema

log = logging.getLogger(__name__)


class ConvertRsMovieMetaDataTask(MetaTaskBase):
    """
    Convert an RS Movie Metadata XML file to a Hdf5 Subread Dataset XML
    """
    TASK_ID = "pbsmrtpipe.tasks.rs_movie_to_hdf5_dataset"
    NAME = "RS Movie to Hdf5 Dataset"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL

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

    TASK_TYPE = TaskTypes.DISTRIBUTED

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


class AlignDataSetTask(MetaTaskBase):
    """
    Create an Aligned DataSet by calling pbalign/blasr
    """
    TASK_ID = "pbsmrtpipe.tasks.align_ds"
    NAME = "Align DataSet"
    VERSION = "0.1.1"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.DS_SUBREADS, "rs_movie_metadata", "A RS Movie metadata.xml"),
                   (FileTypes.DS_REF, "ds_reference", "Reference DataSet")]
    OUTPUT_TYPES = [(FileTypes.DS_ALIGNMENT, "bam", "Aligned BAM")]
    OUTPUT_FILE_NAMES = [("file", "aligned.bam")]

    SCHEMA_OPTIONS = {}
    NPROC = SymbolTypes.MAX_NPROC

    RESOURCE_TYPES = (ResourceTypes.TMP_FILE, )

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        e = "pbalign"
        # FIXME.
        d = os.path.dirname(output_files[0])
        tmp_bam = os.path.join(d, 'tmp.bam')
        cmds = []
        cmds.append("{e} --verbose --nproc={n} {i} {r} {t}".format(e=e, i=input_files[0], n=nproc, r=input_files[1], t=tmp_bam))
        # this auto naming stuff is nonsense
        cmds.append("samtools sort {t} sorted".format(t=tmp_bam))
        cmds.append("mv sorted.bam {o}".format(o=output_files[0]))
        cmds.append('samtools index {o}'.format(o=output_files[0]))
        return cmds


class MappingReportTask(MetaTaskBase):
    """
    Create a Alignment Report from a Alignment DataSet
    """
    TASK_ID = "pbsmrtpipe.tasks.mapping_ds_report"
    NAME = "Mapping DataSet Report"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.DS_ALIGNMENT, "ds", "Alignment DataSet")]
    OUTPUT_TYPES = [(FileTypes.REPORT, "rpt", "Alignment Mapping Report")]
    OUTPUT_FILE_NAMES = [("mapping_report", "json")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        exe = "mapping_stats"
        # this needs to be changed
        output_dir = os.path.dirname(output_files[0])
        report_json = os.path.basename(output_files[0])
        _d = dict(e=exe, i=input_files[0], o=output_dir, j=report_json)
        return "{e} --debug {i} --output {o} {j}".format(**_d)


def _to_consensus_cmd(input_files, output_files, ropts, nproc, resources):
    """Generic to_cmd for CallVariants"""
    algorithm = ropts[OP.to_opt_id("consensus.algorithm")]
    minConfidence = ropts[OP.to_opt_id("consensus.min_confidence")]
    minCoverage = ropts[OP.to_opt_id("consensus.min_coverage")]
    diploid_mode = ropts[OP.to_opt_id("consensus.diploid_mode")]

    diploid_str = ' --diploid' if diploid_mode else ""

    if minConfidence is not None:
        confidenceFilter = "-q %s" % minConfidence
    else:
        confidenceFilter = ""

    if minCoverage is not None:
        coverageFilter = "-x {x}".format(x=minCoverage)
    else:
        coverageFilter = ""

    #
    _d = dict(d=diploid_str,
              vf=coverageFilter,
              cf=confidenceFilter,
              n=nproc,
              a=algorithm,
              h=input_files[1],
              r=input_files[0],
              g=output_files[0],
              f=output_files[1],
              q=output_files[2])

    c = "variantCaller --alignmentSetRefWindows {d} {vf} {cf} -vv --numWorkers {n} --algorithm={a} {h} --reference '{r}' -o {g} -o {f} -o {q}"

    return c.format(**_d)



class DataSetCallVariants(MetaTaskBase):
    """BAM interface to quiver. The contig 'ids' (using the pbcore 'id' format)
    are passed in via a FOFN
    """

    TASK_ID = "pbsmrtpipe.tasks.bam_call_variants_with_fastx_ds"
    NAME = "DataSet Driven Call Variants"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.DS_REF, "ref_ds", "Reference DataSet file"),
                   (FileTypes.DS_ALIGNMENT, "bam", "DataSet BAM Alignment")]

    OUTPUT_TYPES = [(FileTypes.GFF, "gff", "Consensus GFF"),
                    (FileTypes.FASTA, "fasta", "Consensus Fasta"),
                    (FileTypes.FASTQ, "fastq", "Consensus Fastq")]

    OUTPUT_FILE_NAMES = [('variants', 'gff'),
                         ('consensus', 'fasta'),
                         ('consensus', 'fastq')]

    NPROC = SymbolTypes.MAX_NPROC
    SCHEMA_OPTIONS = _to_call_variants_opts_schema()
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        return _to_consensus_cmd(input_files, output_files, ropts, nproc, resources)


class AlignmentSetScatterContigs(MetaScatterTaskBase):
    """AlignmentSet scattering by Contigs
    """
    # MK. Inheritance is specifically not allowed

    TASK_ID = "pbsmrtpipe.tasks.alignment_contig_scatter"
    NAME = "AlignmentSet Contig Scatter"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL
    INPUT_TYPES = [(FileTypes.DS_REF, "ref_ds", "Reference DataSet file"),
                   (FileTypes.DS_ALIGNMENT, "bam", "DataSet BAM Alignment")]

    OUTPUT_TYPES = [(FileTypes.CHUNK, 'cdataset',
                     'Generic Chunked JSON AlignmentSet')]

    OUTPUT_FILE_NAMES = [('alignmentset_chunked', 'json'),]

    NPROC = 1
    SCHEMA_OPTIONS = {}
    RESOURCE_TYPES = None
    NCHUNKS = SymbolTypes.MAX_NCHUNKS
    # Keys that are expected to be written to the chunk.json file
    CHUNK_KEYS = ('$chunk.alignmentset_id', )

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
        exe = "pbtools-chunker alignmentset"
        chunk_key = "alignmentset"
        mode = "alignmentset"
        _d = dict(e=exe,
                  i=input_files[1],
                  r=input_files[0],
                  o=output_files[0],
                  n=nchunks)
        return "{e} --debug --max-total-chunks {n} {i} {r} {o}".format(**_d)


