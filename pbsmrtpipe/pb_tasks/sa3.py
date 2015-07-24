import logging
import os

from pbsmrtpipe.core import (MetaTaskBase, MetaScatterTaskBase,
                             MetaGatherTaskBase)
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


class H5SubreadSetScatter(MetaScatterTaskBase):
    """
    Scatter an HDF5SubreadSet.
    """
    TASK_ID = "pbsmrtpipe.tasks.h5_subreadset_scatter"
    NAME = "H5 SubreadSet scatter"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.DS_SUBREADS_H5, "h5_subreads", "H5 Subread DataSet")]

    OUTPUT_TYPES = [(FileTypes.CHUNK, 'cdataset',
                     'Generic Chunked JSON HdfSubreadSet')]

    OUTPUT_FILE_NAMES = [('hdfsubreadset_chunked', 'json'),]

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

    TASK_TYPE = TaskTypes.LOCAL

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
        #return 'touch {o}'.format(o=output_files[0])


def _gather_dataset(ds_type, chunk_id, input_file, output_file):
    _d = dict(x=ds_type, c=chunk_id, i=input_file, o=output_file)
    return 'pbtools-gather {x} --debug --chunk-key="{c}" {i} --output={o}'.format(**_d)


class GatherContigSetTask(MetaGatherTaskBase):
    """Gather ContigSet Files"""
    TASK_ID = "pbsmrtpipe.tasks.gather_contigset"
    NAME = "Gather ContigSet"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    # TODO: change this when quiver outputs xmls
    OUTPUT_TYPES = [(FileTypes.FASTA, "fasta", "Gathered Fasta")]
    OUTPUT_FILE_NAMES = [("gathered", "xml")]

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

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    # TODO: change this when quiver outputs xmls
    OUTPUT_TYPES = [(FileTypes.DS_BAM, "ds_bam", "Gathered SubreadSets")]
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

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.CHUNK, "chunk", "Gathered Chunk")]
    OUTPUT_TYPES = [(FileTypes.DS_BAM, "ds_bam", "Gathered AlignmentSets")]
    OUTPUT_FILE_NAMES = [("gathered", "xml")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        # having the chunk key hard coded here is a problem.
        return _gather_dataset('alignmentset', 'alignmentset_id', input_files[0], output_files[0])


class AlignDataSetTask(MetaTaskBase):
    """
    Create an Aligned DataSet by calling pbalign/blasr
    """
    TASK_ID = "pbsmrtpipe.tasks.align_ds"
    NAME = "Align DataSet"
    VERSION = "0.1.2"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.DS_SUBREADS, "ds_subreads", "Subread DataSet"),
                   (FileTypes.DS_REF, "ds_reference", "Reference DataSet")]
    OUTPUT_TYPES = [(FileTypes.DS_BAM, "align_ds", "Alignment DataSet")]
    OUTPUT_FILE_NAMES = [("file", "alignment_set.xml")]

    SCHEMA_OPTIONS = {}
    NPROC = SymbolTypes.MAX_NPROC

    RESOURCE_TYPES = (ResourceTypes.TMP_FILE, )

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        e = "pbalign"
        cmds = []
        cmds.append("{e} --verbose --nproc={n} {i} {r} {t}".format(e=e, i=input_files[0], n=nproc, r=input_files[1], t=output_files[0]))
        return cmds


class MappingReportTask(MetaTaskBase):
    """
    Create a Alignment Report from a Alignment DataSet
    """
    TASK_ID = "pbsmrtpipe.tasks.mapping_ds_report"
    NAME = "Mapping DataSet Report"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.DS_BAM, "ds", "Alignment DataSet")]
    OUTPUT_TYPES = [(FileTypes.REPORT, "rpt", "Alignment Mapping Report")]
    OUTPUT_FILE_NAMES = [("mapping_report", "json")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        exe = "mapping_stats"
        _d = dict(e=exe, i=input_files[0], j=output_files[0])
        return "{e} --debug {i} {j}".format(**_d)


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

    c = "variantCaller --alignmentSetRefWindows {d} {vf} {cf} --verbose --numWorkers {n} --algorithm={a} {h} --reference '{r}' -o {g} -o {f} -o {q}"

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
                   (FileTypes.DS_BAM, "bam", "DataSet BAM Alignment")]

    #TODO change/add fasta/xml output once quiver outputs contigsets
    OUTPUT_TYPES = [(FileTypes.GFF, "gff", "Consensus GFF"),
                    (FileTypes.FASTA, "fasta", "Consensus ContigSet"),
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


class GffToVcf(MetaTaskBase):
    """Utility for converting variant GFF3 files to 1000 Genomes VCF"""
    TASK_ID = "pbsmrtpipe.tasks.ds_gff_to_vcf"
    NAME = "GFF to VCF"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.GFF, "gff", "Gff File"),
                   (FileTypes.DS_REF, "ds_ref", "Reference DataSet")]
    OUTPUT_TYPES = [(FileTypes.VCF, "vcf", "VCF File")]
    OUTPUT_FILE_NAMES = [("consensus", "vcf")]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        _d = dict(r=input_files[1], g=input_files[0], o=output_files[0])
        cmd = "gffToVcf --globalReference={r} {g} > {o}"
        return cmd.format(**_d)


class GffToBed(MetaTaskBase):
    """Utility for converting GFF3 to BED format. Currently supports regional coverage or variant .bed output"""
    TASK_ID = "pbsmrtpipe.tasks.consensus_gff_to_bed2"
    NAME = "GFF to Bed"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.GFF, "gff", "Gff Consensus")]
    OUTPUT_TYPES = [(FileTypes.BED, "bed", "Bed Consensus")]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        name = 'variants'
        purpose = 'variants'
        trackDescription = 'PacBio: snps, insertions, and deletions derived from consensus calls against reference'
        exe = "gffToBed"
        cmd = "{e} --name={n} --description='{d}' '{p}' {i} > {o}"
        _d = dict(e=exe, n=name, p=purpose, d=trackDescription,
                  i=input_files[0], o=output_files[0])
        return cmd.format(**_d)


class SummarizeConsensus(MetaTaskBase):
    """ Enrich Alignment Summary"""
    TASK_ID = "pbsmrtpipe.tasks.enrich_summarize_consensus2"
    NAME = "Enrich Alignment Summarize Consensus"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.GFF, "algn_gff", "GFF Alignment"),
                   (FileTypes.GFF, "var_gff", "Gff Variants")]
    OUTPUT_TYPES = [(FileTypes.GFF, "gff", "Gff Alignment Summary")]
    OUTPUT_FILE_NAMES = [('alignment_summary_with_variants', 'gff')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = (ResourceTypes.TMP_FILE,)

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """
        Augment the alignment_summary.gff file with consensus and variants
        information.

        positional arguments:
          inputAlignmentSummaryGff
                                Input alignment_summary.gff filename

        optional arguments:
          -h, --help            show this help message and exit
          --variantsGff VARIANTSGFF
                                Input variants.gff or variants.gff.gz filename
          --output OUTPUT, -o OUTPUT
                                Output alignment_summary.gff filename

        """
        # This has a bit of mutable nonsense.
        cmds = []
        tmp_file = resources[0]

        exe = 'summarizeConsensus'
        cmd = "{e} --variantsGff {g} {f} --output {t}"
        _d = dict(e=exe, g=input_files[0], f=input_files[1], t=output_files[0])
        cmds.append(cmd.format(**_d))
        return cmds


class AlignmentSetScatterContigs(MetaScatterTaskBase):
    """AlignmentSet scattering by Contigs
    """
    # MK. Inheritance is specifically not allowed

    TASK_ID = "pbsmrtpipe.tasks.alignment_contig_scatter"
    NAME = "AlignmentSet Contig Scatter"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL
    INPUT_TYPES = [(FileTypes.DS_REF, "ref_ds", "Reference DataSet file"),
                   (FileTypes.DS_BAM, "alignment_ds", "Pacbio DataSet AlignmentSet")]

    OUTPUT_TYPES = [(FileTypes.CHUNK, 'cdataset',
                     'Generic Chunked JSON AlignmentSet')]

    OUTPUT_FILE_NAMES = [('alignmentset_chunked', 'json'), ]

    NPROC = 1
    SCHEMA_OPTIONS = {}
    RESOURCE_TYPES = None
    NCHUNKS = SymbolTypes.MAX_NCHUNKS
    # Keys that are expected to be written to the chunk.json file
    CHUNK_KEYS = ('$chunk.alignmentset_id', )

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
        exe = "pbtools-chunker alignmentset"
        chunk_key = "alignmentset_id"
        mode = "alignmentset"
        _d = dict(e=exe,
                  i=input_files[1],
                  r=input_files[0],
                  o=output_files[0],
                  n=nchunks)
        return "{e} --debug --max-total-chunks {n} {i} {r} {o}".format(**_d)

class SubreadSetScatter(MetaScatterTaskBase):
    """
    Scatter a subreadset to create an Aligned DataSet by calling pbalign/blasr
    """
    TASK_ID = "pbsmrtpipe.tasks.subreadset_align_scatter"
    NAME = "Scatter Subreadset DataSet"
    VERSION = "0.1.0"

    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.DS_SUBREADS, "ds_subreads", "Subread DataSet"),
                   (FileTypes.DS_REF, "ds_reference", "Reference DataSet")]
    OUTPUT_TYPES = [(FileTypes.CHUNK, 'cdataset',
                     'Generic Chunked JSON SubreadSet')]
    OUTPUT_FILE_NAMES = [('subreadset_chunked', 'json'),]

    NPROC = 1
    SCHEMA_OPTIONS = {}
    RESOURCE_TYPES = None
    NCHUNKS = SymbolTypes.MAX_NCHUNKS
    CHUNK_KEYS = ('$chunk.subreadset_id', )

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
        exe = "pbtools-chunker"
        chunk_key = "subreadset_id"
        mode = "subreadset"
        _d = dict(e=exe,
                  m=mode,
                  i=input_files[1],
                  r=input_files[0],
                  o=output_files[0],
                  n=nchunks)
        return "{e} {m} --debug --max-total-chunks {n} {i} {r} {o}".format(**_d)


class TopVariantsReport(MetaTaskBase):
    """Consensus Reports to compute Top Variants"""
    TASK_ID = 'pbsmrtpipe.tasks.ds_top_variants_report'
    NAME = "Top Variants Report"
    VERSION = "0.1.0"

    TASK_TYPE =  TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.DS_REF, "ds_ref", "PacBio ReferenceSet XML"),
                   (FileTypes.GFF, 'gff', "GFF Alignment Summary")]
    OUTPUT_TYPES = [(FileTypes.REPORT, "rpt", "Pacbio JSON Report")]
    OUTPUT_FILE_NAMES = [('top_variants_report', 'json')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):

        reference_dir = os.path.dirname(os.path.dirname(input_files[0]))
        exe = "pbreport.py topvariants"
        json_report = os.path.basename(output_files[0])
        o = os.path.dirname(output_files[0])

        cmd = "{e} --debug {o} {j} {g} {rd}"
        _d = dict(e=exe, o=o, j=json_report, g=input_files[1], rd=reference_dir)
        return cmd.format(**_d)


class VariantsReport(MetaTaskBase):
    """Consensus Variants Reports"""
    TASK_ID = 'pbsmrtpipe.tasks.ds_variants_report'
    NAME = "Variants Report"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.DS_REF, "ds_ref", "PacBio ReferenceSet XML"),
                   (FileTypes.GFF, "gff", "Gff File"),
                   (FileTypes.GFF, "gff", "Gff File")]
    OUTPUT_TYPES = [(FileTypes.REPORT, 'rpt', "Pacbio JSON Report")]
    OUTPUT_FILE_NAMES = [('variants_report', 'json')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """
        Generates a table showing consensus stats and a report showing variants
        plots for the top 25 contigs of the supplied reference.

        Inputs types: Reference Entry dir, Alignment.gff, Variants.gff

        """
        exe = "pbreport.py variants --debug"
        json_report = os.path.basename(output_files[0])
        output_dir = os.path.dirname(output_files[0])
        reference_dir = os.path.dirname(os.path.dirname(input_files[0]))

        cmd = "{e} {o} {j} '{rd}' {g} {v}"
        d = dict(e=exe, o=output_dir, j=json_report,
                 g=input_files[1], rd=reference_dir,
                 v=input_files[2])

        return cmd.format(**d)
