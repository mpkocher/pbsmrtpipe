
"""
Unit tests for the various scatter/gather tools in pbsmrtipipe.tools_dev.
"""

import unittest
import os.path as op
import re

from pbcommand.pb_io.common import load_pipeline_chunks_from_json
from pbcommand.pb_io.report import load_report_from_json
from pbcommand.models import PipelineChunk
import pbcommand.testkit.core
from pbcore.io import SubreadSet, ContigSet, FastaReader, FastqReader, \
    ConsensusReadSet, AlignmentSet, ConsensusAlignmentSet, HdfSubreadSet

from pbsmrtpipe.tools.chunk_utils import write_chunks_to_json

from base import get_temp_file

MNT_DATA = "/mnt/secondary-siv/testdata/SA3-DS"


def _write_fasta_or_contigset(file_name):
    fasta_file = re.sub(".contigset.xml", ".fasta", file_name)
    rec = [">chr%d\nacgtacgtacgt" % x for x in range(251)]
    with open(fasta_file, "w") as f:
        f.write("\n".join(rec))
    if file_name.endswith(".xml"):
        cs = ContigSet(fasta_file)
        cs.write(file_name)


class CompareScatteredRecordsBase(object):
    READER_CLASS = None
    READER_KWARGS = {}

    def run_after(self, rtc, output_dir):
        unchunked = self.INPUT_FILES[0]
        json_file = rtc.task.output_files[0]
        chunks = load_pipeline_chunks_from_json(json_file)
        n_rec = 0
        with self.READER_CLASS(unchunked, **self.READER_KWARGS) as f:
            n_rec = len([r for r in f])
        n_rec_chunked = 0
        for chunk in chunks:
            d = chunk.chunk_d
            chunked = d[self.CHUNK_KEYS[0]]
            with self.READER_CLASS(chunked, **self.READER_KWARGS) as cs:
                n_rec_chunked += len([r for r in cs])
        self.assertEqual(n_rec_chunked, n_rec)


class ScatterSequenceBase(CompareScatteredRecordsBase):

    @classmethod
    def setUpClass(cls):
        super(ScatterSequenceBase, cls).setUpClass()
        _write_fasta_or_contigset(cls.INPUT_FILES[0])


class TestScatterFilterFasta(ScatterSequenceBase,
                             pbcommand.testkit.core.PbTestScatterApp):

    """
    Test pbsmrtpipe.tools_dev.scatter_filter_fasta
    """
    READER_CLASS = FastaReader
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_filter_fasta"
    INPUT_FILES = [
        get_temp_file(suffix=".fasta")
    ]
    MAX_NCHUNKS = 12
    RESOLVED_MAX_NCHUNKS = 12
    CHUNK_KEYS = ("$chunk.fasta_id",)


class TestScatterContigSet(ScatterSequenceBase,
                           pbcommand.testkit.core.PbTestScatterApp):

    """
    Test pbsmrtpipe.tools_dev.scatter_contigset
    """
    READER_CLASS = ContigSet
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_contigset"
    INPUT_FILES = [
        get_temp_file(suffix=".contigset.xml")
    ]
    MAX_NCHUNKS = 12
    RESOLVED_MAX_NCHUNKS = 12
    CHUNK_KEYS = ("$chunk.contigset_id",)


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestScatterContigSetIndexed(CompareScatteredRecordsBase,
                                  pbcommand.testkit.core.PbTestScatterApp):

    """
    Test pbsmrtpipe.tools_dev.scatter_contigset
    Test ContigSet scatter when the underlying .fasta file is indexed
    (requires samtools and thus stored externally).
    """
    # XXX validates fix for bug ticket 27977
    READER_CLASS = ContigSet
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_contigset"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pbsmrtpipe-unittest/data/chunk/transcripts.contigset.xml"
    ]
    MAX_NCHUNKS = 2
    RESOLVED_MAX_NCHUNKS = 2
    CHUNK_KEYS = ("$chunk.contigset_id",)


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestScatterSubreadZMWs(CompareScatteredRecordsBase,
                             pbcommand.testkit.core.PbTestScatterApp):

    """
    Test pbsmrtpipe.tools_dev.scatter_subread_zmws
    """
    READER_CLASS = SubreadSet
    READER_KWARGS = {'strict': True}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_subread_zmws"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/SA3-DS/lambda/2372215/0007_micro/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml"
    ]
    MAX_NCHUNKS = 12
    RESOLVED_MAX_NCHUNKS = 12
    CHUNK_KEYS = ("$chunk.subreadset_id",)


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestScatterCCSZMWs(CompareScatteredRecordsBase,
                         pbcommand.testkit.core.PbTestScatterApp):

    """
    Test pbsmrtpipe.tools_dev.scatter_ccs_zmws
    """
    READER_CLASS = ConsensusReadSet
    READER_KWARGS = {'strict': True}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_ccs_zmws"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pbsmrtpipe-unittest/data/chunk/ccs.consensusreadset.xml"
    ]
    MAX_NCHUNKS = 8
    RESOLVED_MAX_NCHUNKS = 8
    CHUNK_KEYS = ("$chunk.ccsset_id",)


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestScatterHdfSubreads(CompareScatteredRecordsBase,
                             pbcommand.testkit.core.PbTestScatterApp):

    """
    Test pbsmrtpipe.tools_dev.scatter_hdfsubreads
    """
    READER_CLASS = HdfSubreadSet
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_hdfsubreads"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/SA3-DS/lambda/2372215/0007_tiny/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.hdfsubreadset.xml"
    ]
    MAX_NCHUNKS = 8
    RESOLVED_MAX_NCHUNKS = 8
    CHUNK_KEYS = ("$chunk.hdf5subreadset_id",)


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestScatterAlignmentsReference(pbcommand.testkit.core.PbTestScatterApp):
    READER_CLASS = AlignmentSet
    READER_KWARGS = {}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_alignments_reference"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/SA3-DS/lambda/2372215/0007_tiny/Alignment_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.1.alignmentset.xml",
        "/mnt/secondary-siv/testdata/SA3-DS/lambda.referenceset.xml",
    ]
    MAX_NCHUNKS = 2
    RESOLVED_MAX_NCHUNKS = 2
    CHUNK_KEYS = ("$chunk.alignmentset_id", "$chunk.reference_id")

    def run_after(self, rtc, output_dir):
        json_file = rtc.task.output_files[0]
        chunks = load_pipeline_chunks_from_json(json_file)
        windows = []
        for chunk in chunks:
            d = chunk.chunk_d
            chunked = d[self.CHUNK_KEYS[0]]
            with self.READER_CLASS(chunked, **self.READER_KWARGS) as ds:
                windows.append(ds.refWindows)
        self.assertEqual(windows, [
            [('lambda_NEB3011', 0, 24251)],
            [('lambda_NEB3011', 24251, 48502)]
        ])


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestScatterAlignmentsReferenceBasemods(TestScatterAlignmentsReference):
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_alignments_reference_basemods"


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestScatterSubreadReference(pbcommand.testkit.core.PbTestScatterApp):
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_subread_reference"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/SA3-DS/lambda/2372215/0007_tiny/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml",
        "/mnt/secondary-siv/testdata/SA3-DS/lambda.referenceset.xml",
    ]
    MAX_NCHUNKS = 3
    RESOLVED_MAX_NCHUNKS = 3
    CHUNK_KEYS = ("$chunk.subreadset_id", "$chunk.reference_id")


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestScatterCCSReference(pbcommand.testkit.core.PbTestScatterApp):
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_ccs_reference"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pbsmrtpipe-unittest/data/chunk/ccs.consensusreadset.xml",
        "/mnt/secondary-siv/testdata/SA3-DS/lambda.referenceset.xml",
    ]
    MAX_NCHUNKS = 8
    RESOLVED_MAX_NCHUNKS = 8
    CHUNK_KEYS = ("$chunk.ccsset_id", "$chunk.reference_id")


@unittest.skipUnless(op.isdir("/mnt/secondary-siv/testdata/pblaa-unittest"),
                     "Missing /mnt/secondary-siv/testdata/pblaa-unittest")
class TestScatterSubreadBarcodes(pbcommand.testkit.core.PbTestScatterApp):
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_subread_barcodes"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pblaa-unittest/P6-C4/HLA_ClassI/m150724_012016_sherri_c100820352550000001823172911031521_s1_p0.class_I.haploid.bam",
    ]
    MAX_NCHUNKS = 8
    RESOLVED_MAX_NCHUNKS = 8
    CHUNK_KEYS = ("$chunk.subreadset_id", )


########################################################################
# GATHER TASKS
########################################################################

class CompareGatheredRecordsBase(object):
    READER_CLASS = None
    READER_KWARGS = {}

    def run_after(self, rtc, output_dir):
        gathered_file = rtc.task.output_files[0]
        chunks = load_pipeline_chunks_from_json(self.INPUT_FILES[0])
        n_rec = 0
        with self.READER_CLASS(gathered_file, **self.READER_KWARGS) as f:
            n_rec = len([r for r in f])
        n_rec_chunked = 0
        for chunk in chunks:
            d = chunk.chunk_d
            chunked = d[self.CHUNK_KEY]
            with self.READER_CLASS(chunked, **self.READER_KWARGS) as cs:
                n_rec_chunked += len([r for r in cs])
        self.assertEqual(n_rec_chunked, n_rec)


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestGatherSubreads(CompareGatheredRecordsBase,
                         pbcommand.testkit.core.PbTestGatherApp):

    """
    Test pbsmrtpipe.tools_dev.gather_subreads
    """
    READER_CLASS = SubreadSet
    READER_KWARGS = {'strict': True}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_subreads"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pbsmrtpipe-unittest/data/chunk/subreads_gather.chunks.json"
    ]
    CHUNK_KEY = "$chunk.subreadset_id"


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestGatherAlignmentSet(CompareGatheredRecordsBase,
                             pbcommand.testkit.core.PbTestGatherApp):

    """
    Test pbsmrtpipe.tools_dev.gather_alignments
    """
    READER_CLASS = AlignmentSet
    READER_KWARGS = {'strict': True}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_alignments"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pbsmrtpipe-unittest/data/chunk/alignmentset_gather.chunks.json"
    ]
    CHUNK_KEY = "$chunk.alignmentset_id"

    def run_after(self, rtc, output_dir):
        super(TestGatherAlignmentSet, self).run_after(rtc,
                                                      output_dir)
        with AlignmentSet(rtc.task.output_files[0]) as ds:
            self.assertTrue(ds.isIndexed)
            self._check_bam_count(ds.toExternalFiles())

    def _check_bam_count(self, files):
        # should still be multiple .bam files
        self.assertNotEqual(len(files), 1)


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestGatherCCS(CompareGatheredRecordsBase,
                    pbcommand.testkit.core.PbTestGatherApp):

    """
    Test pbsmrtpipe.tools_dev.gather_ccs
    """
    READER_CLASS = ConsensusReadSet
    READER_KWARGS = {'strict': True}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_ccs"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pbsmrtpipe-unittest/data/chunk/ccs_gather.chunks.json"
    ]
    CHUNK_KEY = "$chunk.ccsset_id"


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestGatherCCSAlignmentSet(CompareGatheredRecordsBase,
                                pbcommand.testkit.core.PbTestGatherApp):

    """
    Test pbsmrtpipe.tools_dev.gather_ccs_alignments
    """
    READER_CLASS = ConsensusAlignmentSet
    READER_KWARGS = {'strict': True}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_ccs_alignments"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pbsmrtpipe-unittest/data/chunk/consensusalignmentset_gather.chunks.json"
    ]
    CHUNK_KEY = "$chunk.ccs_alignmentset_id"


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestGatherReport(pbcommand.testkit.core.PbTestGatherApp):

    """
    Test pbsmrtpipe.tools_dev.gather_report
    """
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_report"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pbsmrtpipe-unittest/data/chunk/ccs_gather.chunks.json"
    ]
    CHUNK_KEY = "$chunk.report_id"

    def run_after(self, rtc, output_dir):
        report_file = rtc.task.output_files[0]
        r = load_report_from_json(report_file)
        a = {a.name: a.value for a in r.attributes}
        self.assertEqual(a, {
            'num_below_min_accuracy': 0,
            'num_not_converged': 0,
            'num_insert_size_too_small': 0,
            'num_too_many_unusable_subreads': 3,
            'num_no_usable_subreads': 0,
            'num_below_snr_threshold': 27,
            'num_ccs_reads': 52,
            'num_not_enough_full_passes': 58})


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestGatherContigs(CompareGatheredRecordsBase,
                        pbcommand.testkit.core.PbTestGatherApp):

    """
    Test pbsmrtpipe.tools_dev.gather_contigs
    """
    READER_CLASS = ContigSet
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_contigs"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pbsmrtpipe-unittest/data/chunk/contig_gather.chunks.json"
    ]
    CHUNK_KEY = "$chunk.contigset_id"


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestGatherFasta(CompareGatheredRecordsBase,
                      pbcommand.testkit.core.PbTestGatherApp):

    """
    Test pbsmrtpipe.tools_dev.gather_fasta
    """
    READER_CLASS = FastaReader
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_fasta"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pbsmrtpipe-unittest/data/chunk/fasta_gather.chunks.json"
    ]
    CHUNK_KEY = "$chunk.fasta_id"


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestGatherFastq(CompareGatheredRecordsBase,
                      pbcommand.testkit.core.PbTestGatherApp):

    """
    Test pbsmrtpipe.tools_dev.gather_fastq
    """
    READER_CLASS = FastqReader
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_fastq"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pbsmrtpipe-unittest/data/chunk/fastq_gather.chunks.json"
    ]
    CHUNK_KEY = "$chunk.fastq_id"


class TextRecordsGatherBase(object):

    """
    Base class for testing gather of simple line-based formats (GFF, CSV).
    """
    RECORDS = []
    RECORD_HEADER = None
    EXTENSION = None

    @classmethod
    def setUpClass(cls):
        super(TextRecordsGatherBase, cls).setUpClass()
        json_file = cls.INPUT_FILES[0]
        base = ".".join(json_file.split(".")[:-2])
        chunks = []
        for i in range(2):
            file_name = "%s.%d.%s" % (base, i + 1, cls.EXTENSION)
            with open(file_name, 'w') as f:
                if cls.RECORD_HEADER is not None:
                    f.write(cls.RECORD_HEADER)
                f.write("\n".join(cls.RECORDS[i * 2:(i + 1) * 2]))
                f.write("\n")  # XXX we need this for CSV gather
            d = {cls.CHUNK_KEY: op.abspath(file_name)}
            c = PipelineChunk("%s_%i" % (cls.EXTENSION, i + 1), **d)
            chunks.append(c)
        write_chunks_to_json(chunks, json_file)

    def run_after(self, rtc, output_dir):
        gathered_file = rtc.task.output_files[0]
        base, ext = op.splitext(gathered_file)
        self.assertEqual(ext, ".%s" % self.EXTENSION)
        with open(gathered_file) as f:
            lines_ = f.readlines()
            lines = self._get_lines(lines_)
            self.assertEqual(lines, self.RECORDS)
            self.validate_content(lines_)

    def validate_content(self, lines):
        pass


class TestGatherGFF(TextRecordsGatherBase,
                    pbcommand.testkit.core.PbTestGatherApp):

    """
    Test pbsmrtpipe.tools_dev.gather_gff
    """
    RECORDS = [
        "contig1\tkinModCall\tmodified_base\t1\t1\t31\t+\t.\tcoverage=169",
        "contig1\tkinModCall\tmodified_base\t2\t2\t41\t-\t.\tcoverage=170",
        "contig1\tkinModCall\tmodified_base\t3\t3\t51\t+\t.\tcoverage=168",
        "contig1\tkinModCall\tmodified_base\t4\t4\t60\t-\t.\tcoverage=173",
    ]
    RECORD_HEADER = "##gff-version 3\n##source-id ipdSummary\n"
    EXTENSION = "gff"

    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_gff"
    INPUT_FILES = [
        get_temp_file(suffix=".chunks.json")
    ]
    CHUNK_KEY = "$chunk.gff_id"

    def _get_lines(self, lines):
        return [l.strip() for l in lines if l[0] != '#']

    def validate_content(self, lines):
        self.assertEqual(len(lines), 6)
        self.assertEqual(lines[1].strip(), "##source-id ipdSummary")


class TestGatherCSV(TextRecordsGatherBase,
                    pbcommand.testkit.core.PbTestGatherApp):

    """
    Test pbsmrtpipe.tools_dev.gather_csv
    """
    RECORDS = [
        "contig1,3000000,170",
        "contig2,90000,180",
        "contig3,58000,159",
        "contig4,20000,160",
    ]
    RECORD_HEADER = "contig_id,length,coverage\n"
    EXTENSION = "csv"

    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_csv"
    INPUT_FILES = [
        get_temp_file(suffix=".chunks.json")
    ]
    CHUNK_KEY = "$chunk.csv_id"

    def _get_lines(self, lines):
        return [l.strip() for l in lines[1:]]
