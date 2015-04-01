"""All new-era Align BAM tasks """
import logging
# MK Notes. This should NOT be Allowed
import os
import re

from pbsmrtpipe.core import MetaTaskBase
from pbsmrtpipe.models import TaskTypes, FileTypes, SymbolTypes, ResourceTypes
import pbsmrtpipe.schema_opt_utils as OP
import pbsmrtpipe.pb_tasks._mapping_opts as AOP

from ._shared_options import GLOBAL_TASK_OPTIONS
from .mapping import _to_coverage_summary_opts, _to_summarize_coverage_cmd, Constants


class BamAlignCoverageSummary(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.bam_coverage_summary"
    NAME = "Align Coverage Summary"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.BAM, "bam", "BAM Alignment")]
    OUTPUT_TYPES = [(FileTypes.GFF, "gff", "GFF Coverage Alignment Summary")]
    OUTPUT_FILE_NAMES = [('alignment_summary', 'gff')]

    SCHEMA_OPTIONS = _to_coverage_summary_opts()
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        return _to_summarize_coverage_cmd(input_files, output_files, ropts, nproc, resources)


class BamAlign(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.bam_align"
    NAME = "BAM Align"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.MOVIE_FOFN, "mfofn", "Movie FOFN"),
                   (FileTypes.RGN_FOFN, "rfofn", "Region FOFN"),
                   (FileTypes.FASTA, "fasta", "Fasta Reference"),
                   (FileTypes.REPORT, "rpt", "Fasta Reference JSON metadata Report")]
    OUTPUT_TYPES = [(FileTypes.BAM, "bam", "BAM Alignment file")]
    OUTPUT_FILE_NAMES = [('aligned_reads', 'bam')]

    NPROC = SymbolTypes.MAX_NPROC
    SCHEMA_OPTIONS = AOP.to_bam_blasr_opts()
    RESOURCE_TYPES = None


    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """Blasr alignment tasks. Calls blasr directly rather than going through
        pbalign, for now.
        """

        # We pipe the output of blasr through samtools so the big SAM never
        # hits the disk
        blasr_cmd = "blasr {input_} {reference} {align_opts} | {samtools_exe} view -Shb - > {outfile}"

        # The basic align opts that are required for downstream BAM analysis to
        # work
        align_opts = "-printSAMQV -sam -clipping subread "

        # Check the settings that may have come from the settings xml file
        align_opts += " -regionTable {r}".format(r=input_files[1])

        def to_o(x):
            return ropts[OP.to_opt_id(x)]

        if to_o("place_repeats_randomly"):
            align_opts += " -placeRepeatsRandomly"

        if to_o("max_error") >= 0.0:
            align_opts += " -minPctIdentity {m}".format(m= 100.0 - to_o("max_error"))

        # if self.reference.info.hasIndexFile('sawriter'):
        #     align_opts += " -sa {sa}".format(
        #         sa=self.reference.info.indexFile('sawriter').replace(' ', r'\ '))

        if to_o("min_anchor_size") > 0:
            align_opts += ' -minMatch {m}'.format(m=to_o("min_anchor_size"))

        if to_o("max_hits") > 0:
            align_opts += ' -bestn {b}'.format(b=to_o("max_hits"))

        if to_o("concordant"):
            align_opts += ' -concordant'

        if to_o("use_quality"):
            align_opts += ' -useQuality'

        if nproc > 1:
            align_opts += " -nproc {n}".format(n=nproc - 1)

        if to_o("raw_blasr_opts") is not None:
            align_opts += " {b}".format(b=to_o("raw_blasr_opts"))

        return blasr_cmd.format(
            input_=input_files[0], reference=input_files[2],
            align_opts=align_opts, outfile=output_files[0],
            samtools_exe=Constants.EXE_SAMTOOLS)


class BamLoadChemistry(MetaTaskBase):
    """
    Add chemistry information required by Quiver to the BAM file.
    Happens in two steps, first a header-only file is created, then
    that header is copied into the BAM file using samtools reheader.
    Once all these tasks are complete, merge can begin. So it also writes
    a sentinel file.
    """
    TASK_ID = "pbsmrtpipe.tasks.bam_load_chemistry"
    NAME = "BAM load chemistry"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.MOVIE_FOFN, "movie_fofn", "Input Movie FOFN"),
                   (FileTypes.BAM, "align_bam", "BAM Alignment")]
    OUTPUT_TYPES = [(FileTypes.TXT, "sentinel_bam", "Sentinel BAM txt")]
    OUTPUT_FILE_NAMES = [(".sentinel_bam", "txt")]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = (ResourceTypes.TMP_DIR,)

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        cmds = []

        def _add(x):
            cmds.append(x)

        mapped_bam = input_files[1]

        tmp_header_basename = os.path.basename(input_files[1]) + '.chem_header.sam'

        create_header_cmd = "createChemistryHeader.py {in_bam} {t}/{out_header} --bas_files {fofn}".format(
            in_bam=mapped_bam, fofn=input_files[0],
            out_header=tmp_header_basename, t=resources[0])

        _add(create_header_cmd)

        tmp_bam_basename = os.path.basename(mapped_bam) + '.tmp.bam'
        reheader_cmd = "{exe} reheader {t}/{in_sam} {mod_bam} > {t}/{tmp_bam} && mv {t}/{tmp_bam} {mod_bam}".format(
            exe=Constants.EXE_SAMTOOLS, in_sam=tmp_header_basename,
            tmp_bam=tmp_bam_basename,
            mod_bam=mapped_bam,
            t=resources[0])

        _add(reheader_cmd)

        _add("rm {t}/{in_sam}".format(in_sam=tmp_header_basename, t=resources[0]))

        _add("echo \"Writing Load chemistry sentinel file\" > {o}".format(o=output_files[0]))
        return cmds


class BamToByMovieFofn(MetaTaskBase):
    """
    Create a fofn of the BAM files that are organized by movie. This
    fofn file will be used by bamtools merge to create BAM files
    organized by contig.
    """
    TASK_ID = "pbsmrtpipe.tasks.bam_to_by_movie_fofn"
    NAME = "BAM To By Movie FOFN"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.LOCAL
    INPUT_TYPES = [(FileTypes.TXT, "bam_sentinel", "BAM Sentinel")]
    OUTPUT_TYPES = [(FileTypes.FOFN, "bam_fofn", "BAM FOFN By Movie")]
    OUTPUT_FILE_NAMES = [("bam_by_movie", "fofn")]
    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        # MK. This needs to be rethought Non-chunked jobs will not have 'chunk' in the name
        # cmd = "for i in $(ls ../*/aligned_reads.chunk*.bam); do readlink -f $i >> {c}; done".format(c=output_files[0])
        cmd = "for i in $(ls ../*/aligned_reads*.bam); do readlink -f $i >> {c}; done".format(c=output_files[0])
        return cmd


class BamMergeSort(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.bam_merge_sort"
    NAME = "BAM fofn merge sort"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.FOFN, "bam fofn", "BAM Fofn"),
                   (FileTypes.TXT, "contig_headers", "Fasta Contig Header")]
    OUTPUT_TYPES = [(FileTypes.BAM, "bam_contig", "BAM Aligned by Contig")]
    OUTPUT_FILE_NAMES = [("aligned_reads_by_contig", "bam")]

    SCHEMA_OPTIONS = {}
    NPROC = 2

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """Merge the by-movie BAM files into by-contig BAM files. Then, sort those
        files. Once all these tasks are done, a new fofn of those files can be
        made.
        """

        cmds = []

        # Note that this is using a slightly modified version of bamtools. The
        # master branch, as of August 2014, does not support this kind of
        # region string
        piped_cmd = "{exe} merge -list {i} -region $(head -1 {c})..$(tail -1 {c})".format(
            exe=Constants.EXE_BAMTOOLS,
            i=input_files[0],
            c=input_files[1])

        piped_cmd += ' | '

        # samtools sort writes its output to <prefix>.bam, so we have to take
        # off the .bam ending to the file name
        # MK this is very fragile and not awesome
        by_contig_prefix = re.sub("\.bam$", "", output_files[0])

        piped_cmd += "{exe} sort - {out}".format(
            exe=Constants.EXE_SAMTOOLS,
            out=by_contig_prefix)

        cmds.append(piped_cmd)

        return cmds


class ToContigBamFofn(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.to_contig_bam_fofn"
    NAME = "To Contig BAM Fofn"
    VERSION = "1.0.0"
    TASK_TYPE = TaskTypes.LOCAL

    INPUT_TYPES = [(FileTypes.TXT, "bam_sentinel", "Sentinel from bam merge-sort")]
    OUTPUT_TYPES = [(FileTypes.FOFN, "bam_contig_fofn", "Bam By Contig FOFN")]
    OUTPUT_FILE_NAMES = [("by_contig_bam", "fofn")]

    SCHEMA_OPTIONS = {}
    NPROC = 1

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        # MK this is not a great idea
        cmd = "for i in $(ls ../*/aligned_reads_by_contig*.bam); do readlink -f $i >> {c}; done".format(c=output_files[0])
        return cmd


class CreateBamIndex(MetaTaskBase):
    """Create a companion BAI file

    This treats the BAM file as a mutable file
    """
    TASK_ID = "pbsmrtpipe.tasks.bam_create_index"
    NAME = "Create BAM index"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.BAM, "bam", "Aligned BAM"),
                   (FileTypes.TXT, "txt", "Sentinel")]
    OUTPUT_TYPES = [(FileTypes.BAM, "bam", "Aligned BAM with BAI")]

    SCHEMA_OPTIONS = {}
    NPROC = 1
    RESOURCE_TYPES = None

    MUTABLE_FILES = [('$inputs.0', '$outputs.0')]

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """Create the companion bam.bai index file. This is written as mutation
        of the input file because the name provided as the task output file name will
        not necessarily be the same as the input file"""
        _d = dict(e=Constants.EXE_SAMTOOLS, b=input_files[0])
        cmd = "{e} index {b}".format(**_d)
        return cmd