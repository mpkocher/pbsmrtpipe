import logging
import os

from pbsmrtpipe.core import MetaTaskBase
from pbsmrtpipe.models import TaskTypes, FileTypes, SymbolTypes, ResourceTypes
import pbsmrtpipe.schema_opt_utils as OP


log = logging.getLogger(__name__)


class WriteReferenceContigChunk(MetaTaskBase):

    """
    Write Contig Headers to Chunk ids to Fofn and generate a CHUNK JSON File.

    The contigs are written using the ref00000X Reference.Info.XML spec of an 'id'.
    This is used in the cmp.h5 pipelines.

    """
    TASK_ID = "pbsmrtpipe.tasks.write_reference_contig_chunks"
    NAME = "Write Reference Contigs"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.LOCAL
    INPUT_TYPES = [(FileTypes.FASTA, "fasta", "Reference Fasta"),
                   (FileTypes.REPORT, "report", "Fasta Metadata JSON Report")]
    OUTPUT_TYPES = [(FileTypes.FOFN, "fofn", "Fofn Contig ids"),
                    (FileTypes.CHUNK, "chunk", "Contig Id Chunks")]
    OUTPUT_FILE_NAMES = [("reference_contig_ids", "fofn"),
                         ('reference_contig_chunks', 'json')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """Write a Chunk json file which points to contig id groups"""

        max_chunks = 10
        exe = "pbtools-chunker fofn-contig-ids"
        cmd = "{e} --debug --max-total-chunks {m} {f} {j}"

        _d = dict(e=exe,
                  f=input_files[0],
                  j=output_files[1],
                  m=max_chunks)
        c1 = cmd.format(**_d)

        _d2 = dict(i=input_files[0], o=output_files[0])
        c2 = "pbtools-converter fasta-to-contig-id-fofn --debug {i} {o}".format(**_d2)
        return [c1, c2]


class WriteReferenceIdxContigChunk(MetaTaskBase):

    """
    Write Contig Headers to Chunk ids to Fofn and generate a CHUNK JSON File.

    The contigs are written in the pbcore 'id' spec. This should be used in the
    BAM pipelines.

    """
    TASK_ID = "pbsmrtpipe.tasks.write_reference_contig_idx_chunks"
    NAME = "Write Reference (pbcore 'id') Contigs"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.LOCAL
    INPUT_TYPES = [(FileTypes.FASTA, "fasta", "Reference Fasta"),
                   (FileTypes.REPORT, "report", "Fasta Metadata JSON Report")]
    OUTPUT_TYPES = [(FileTypes.FOFN, "fofn", "Fofn Contig ids"),
                    (FileTypes.CHUNK, "chunk", "Contig Id Chunks")]
    OUTPUT_FILE_NAMES = [("reference_contig_idxs", "fofn"),
                         ('reference_contig_idx_chunks', 'json')]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """Write a Chunk json file which points to contig id groups"""

        max_chunks = 10
        exe = "pbtools-chunker fofn-contig-ids"
        cmd = "{e} --debug --max-total-chunks {m} {f} {j}"

        _d = dict(e=exe,
                  f=input_files[0],
                  j=output_files[1],
                  m=max_chunks)
        c1 = cmd.format(**_d)

        _d2 = dict(i=input_files[0], o=output_files[0])
        c2 = "pbtools-converter fasta-to-contig-idx-fofn --debug {i} {o}".format(**_d2)
        return [c1, c2]


def _to_call_variants_opts_schema():

    opts = []
    opts.append(OP.to_option_schema(OP.to_opt_id("consensus.algorithm"), "string", "Consensus Algorithm", "Consensus Algorithm Type", "quiver"))
    opts.append(OP.to_option_schema(OP.to_opt_id('consensus.enable_map_qv_filter'), "boolean", "Enable QV Filter", "Enable QV Filter for Consensus", True))
    opts.append(OP.to_option_schema(OP.to_opt_id("consensus.min_coverage"), ("integer", "null"), "Minimum Coverage", "Consensus Minimum Coverage Description", None))
    opts.append(OP.to_option_schema(OP.to_opt_id("consensus.min_confidence"), ("integer", "null"), "Minimum Confidence", "Consensus Minimum Confidence Level", None))
    opts.append(OP.to_option_schema(OP.to_opt_id("consensus.diploid_mode"), "boolean", "Diploid Mode", "Enable Diploid Mode", False))

    return {op['required'][0]: op for op in opts}


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
    _d = dict(c=input_files[2],
              d=diploid_str,
              vf=coverageFilter,
              cf=confidenceFilter,
              n=nproc,
              a=algorithm,
              h=input_files[1],
              r=input_files[0],
              g=output_files[0],
              f=output_files[1],
              q=output_files[2])

    c = "variantCaller --skipUnrecognizedContigs -W \"{c}\" {d} {vf} {cf} --verbose --numWorkers {n} --algorithm={a} {h} --reference '{r}' -o {g} -o {f} -o {q}"

    return c.format(**_d)


class CallVariants(MetaTaskBase):

    """Compute the Consensus"""
    TASK_ID = "pbsmrtpipe.tasks.call_variants_with_fastx"
    NAME = "Call Variants"
    VERSION = "1.1.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.FASTA, "fasta", "Reference Fasta file"),
                   (FileTypes.ALIGNMENT_CMP_H5, "cmp_h5", "CmpH5 Alignment"),
                   (FileTypes.FOFN, "contigs_fofn", "Contig FOFN Header (ref0000X) format")]
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


class BamCallVariants(MetaTaskBase):

    """BAM interface to quiver. The contig 'ids' (using the pbcore 'id' format)
    are passed in via a FOFN
    """
    # MK. Inheritance is specifically not allowed

    TASK_ID = "pbsmrtpipe.tasks.bam_call_variants_with_fastx"
    NAME = "BAM Call Variants"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.DS_REF, "ref_ds", "Reference DataSet file"),
                   (FileTypes.DS_BAM, "bam", "DataSet BAM Alignment"),
                   (FileTypes.TXT, "contigs_txt", "Contig Txt Header (pbcore Fasta id format)")]

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


class GffToVcf(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.gff_to_vcf"
    NAME = "GFF to VCF"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.GFF, "gff", "Gff File"),
                   (FileTypes.FASTA, "fasta", "Fasta Reference")]
    OUTPUT_TYPES = [(FileTypes.VCF, "vcf", "VCF File")]
    OUTPUT_FILE_NAMES = [("consensus", "vcf")]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        _d = dict(r=input_files[1], g=input_files[0], o=output_files[0])
        cmd = "gffToVcf.py --globalReference={r} {g} > {o}"
        return cmd.format(**_d)


class GffToBed(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.consensus_gff_to_bed"
    NAME = "GFF to Bed"
    VERSION = "1.0.0"

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
        exe = "gffToBed.py"
        cmd = "{e} --name={n} --description='{d}' '{p}' {i} > {o}"
        _d = dict(e=exe, n=name, p=purpose, d=trackDescription,
                  i=input_files[0], o=output_files[0])
        return cmd.format(**_d)


class SummarizeConsensus(MetaTaskBase):

    """ Enrich Alignment Summary"""
    TASK_ID = "pbsmrtpipe.tasks.enrich_summarize_consensus"
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

        exe = 'summarizeConsensus.py'
        cmd = "{e} --variantsGff {g} {f} --output {t}"
        _d = dict(e=exe, g=input_files[0], f=input_files[1], t=output_files[0])
        cmds.append(cmd.format(**_d))
        return cmds


class TopVariantsReport(MetaTaskBase):
    TASK_ID = 'pbsmrtpipe.tasks.top_variants_report'
    NAME = "Top Variants Report"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.FASTA, "fasta", "PacBio Reference File"),
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
    TASK_ID = 'pbsmrtpipe.tasks.variants_report'
    NAME = "Variants Report"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.FASTA, "fasta", "PacBio Reference Fasta"),
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
