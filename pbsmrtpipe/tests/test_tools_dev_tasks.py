
"""
Unit tests for the various scatter/gather tools in pbsmrtipipe.tools_dev.
"""

import tempfile
import unittest
import random
import shutil
import os.path as op
import re

import pysam

from pbcommand.pb_io.common import load_pipeline_chunks_from_json
from pbcommand.pb_io.report import load_report_from_json
from pbcommand.models import PipelineChunk
import pbcommand.testkit.core
from pbcore.io import SubreadSet, ContigSet, FastaReader, FastqReader, \
    ConsensusReadSet, AlignmentSet, ConsensusAlignmentSet, HdfSubreadSet, \
    ReferenceSet
import pbcore.data

from pbsmrtpipe.tools.chunk_utils import write_chunks_to_json
from pbsmrtpipe.mock import write_random_report, \
    write_random_fasta_records, write_random_fastq_records

from base import get_temp_file

MNT_DATA = "/pbi/dept/secondary/siv/testdata"
skip_if_missing_testdata = unittest.skipUnless(op.isdir(MNT_DATA),
    "Missing {d}".format(d=MNT_DATA))


def _write_fasta_or_contigset(file_name, make_faidx=False):
    fasta_file = re.sub(".contigset.xml", ".fasta", file_name)
    rec = [">chr%d\nacgtacgtacgt" % x for x in range(251)]
    with open(fasta_file, "w") as f:
        f.write("\n".join(rec))
        f.flush()
    if make_faidx:
        pysam.faidx(fasta_file)
    if file_name.endswith(".xml"):
        cs = ContigSet(fasta_file, strict=make_faidx)
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
            n_rec = len([rec for rec in f])
        n_rec_chunked = 0
        for chunk in chunks:
            d = chunk.chunk_d
            chunked = d[self.CHUNK_KEYS[0]]
            with self.READER_CLASS(chunked, **self.READER_KWARGS) as cs:
                n_rec_chunked += len([rec for rec in cs])
        self.assertEqual(n_rec_chunked, n_rec)


class ScatterSequenceBase(CompareScatteredRecordsBase):

    def setUp(self):
        super(ScatterSequenceBase, self).setUp()
        _write_fasta_or_contigset(self.INPUT_FILES[0], make_faidx=True)


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


class TestScatterContigSet(TestScatterFilterFasta,
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


def make_tmp_dataset_xml(bam_file, ds_type):
    suffix = ".{t}.xml".format(t=ds_type.__name__.lower())
    tmp_file = tempfile.NamedTemporaryFile(suffix=suffix).name
    ds = ds_type(bam_file, strict=True)
    ds.write(tmp_file)
    return tmp_file


class TestScatterSubreadZMWs(CompareScatteredRecordsBase,
                             pbcommand.testkit.core.PbTestScatterApp):

    """
    Test pbsmrtpipe.tools_dev.scatter_subread_zmws
    """
    READER_CLASS = SubreadSet
    READER_KWARGS = {'strict': True}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_subread_zmws"
    INPUT_FILES = [
        make_tmp_dataset_xml(pbcore.data.getUnalignedBam(), READER_CLASS)
    ]
    MAX_NCHUNKS = 12
    RESOLVED_MAX_NCHUNKS = 12
    CHUNK_KEYS = ("$chunk.subreadset_id",)


class TestScatterCCSZMWs(CompareScatteredRecordsBase,
                         pbcommand.testkit.core.PbTestScatterApp):

    """
    Test pbsmrtpipe.tools_dev.scatter_ccs_zmws
    """
    READER_CLASS = ConsensusReadSet
    READER_KWARGS = {'strict': True}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_ccs_zmws"
    INPUT_FILES = [
        make_tmp_dataset_xml(pbcore.data.getCCSBAM(), READER_CLASS)
    ]
    MAX_NCHUNKS = 6
    RESOLVED_MAX_NCHUNKS = 6
    CHUNK_KEYS = ("$chunk.ccsset_id",)


# XXX it would be better to use local files for this but it's the least
# important test in this file
@skip_if_missing_testdata
class TestScatterHdfSubreads(CompareScatteredRecordsBase,
                             pbcommand.testkit.core.PbTestScatterApp):

    """
    Test pbsmrtpipe.tools_dev.scatter_hdfsubreads
    """
    READER_CLASS = HdfSubreadSet
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_hdfsubreads"
    INPUT_FILES = [
        "/pbi/dept/secondary/siv/testdata/SA3-DS/lambda/2372215/0007_tiny/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.hdfsubreadset.xml"
    ]
    MAX_NCHUNKS = 8
    RESOLVED_MAX_NCHUNKS = 8
    CHUNK_KEYS = ("$chunk.hdf5subreadset_id",)


class TestScatterAlignmentsReference(pbcommand.testkit.core.PbTestScatterApp):
    READER_CLASS = AlignmentSet
    READER_KWARGS = {}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_alignments_reference"
    INPUT_FILES = [
        pbcore.data.getBamAndCmpH5()[0],
        pbcore.data.getLambdaFasta()
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


class TestScatterAlignmentsReferenceBasemods(TestScatterAlignmentsReference):
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_alignments_reference_basemods"


class TestScatterSubreadReference(pbcommand.testkit.core.PbTestScatterApp):
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_subread_reference"
    INPUT_FILES = [
        pbcore.data.getUnalignedBam(),
        pbcore.data.getLambdaFasta()
    ]
    MAX_NCHUNKS = 3
    RESOLVED_MAX_NCHUNKS = 3
    CHUNK_KEYS = ("$chunk.subreadset_id", "$chunk.reference_id")


class TestScatterCCSReference(pbcommand.testkit.core.PbTestScatterApp):
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_ccs_reference"
    INPUT_FILES = [
        make_tmp_dataset_xml(pbcore.data.getCCSBAM(), ConsensusReadSet),
        make_tmp_dataset_xml(pbcore.data.getLambdaFasta(), ReferenceSet)
    ]
    MAX_NCHUNKS = 8
    RESOLVED_MAX_NCHUNKS = 8
    CHUNK_KEYS = ("$chunk.ccsset_id", "$chunk.reference_id")


# FIXME
@skip_if_missing_testdata
class TestScatterSubreadBarcodes(pbcommand.testkit.core.PbTestScatterApp):
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_subread_barcodes"
    INPUT_FILES = [
        "/pbi/dept/secondary/siv/testdata/pblaa-unittest/P6-C4/HLA_ClassI/m150724_012016_sherri_c100820352550000001823172911031521_s1_p0.class_I.haploid.bam",
    ]
    MAX_NCHUNKS = 8
    RESOLVED_MAX_NCHUNKS = 8
    CHUNK_KEYS = ("$chunk.subreadset_id", )


########################################################################
# GATHER TASKS
########################################################################

class CompareGatheredRecordsBase(object):
    """
    Base class for comparing record count in output to chunked inputs
    """
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


class _SetupGatherApp(CompareGatheredRecordsBase,
                      pbcommand.testkit.core.PbTestGatherApp):
    """
    Automates the setup of gather tests using dynamically generated inputs
    with canned or mock data.
    """
    NCHUNKS = 2
    INPUT_FILES = [
        tempfile.NamedTemporaryFile(suffix=".chunks.json").name
    ]

    def _generate_chunk_output_file(self):
        raise NotImplementedError()

    def _generate_chunk_json(self, data_files):
        chunks = [PipelineChunk(chunk_id="chunk_data_{i}".format(i=i),
                                **({self.CHUNK_KEY:fn}))
                  for i, fn in enumerate(data_files)]
        write_chunks_to_json(chunks, self.INPUT_FILES[0])

    def _copy_mock_output_file(self, file_name):
        base, ext = op.splitext(file_name)
        tmp_file = tempfile.NamedTemporaryFile(suffix=ext).name
        shutil.copy(file_name, tmp_file)
        for index in [".pbi", ".fai"]:
            if op.exists(file_name + index):
                shutil.copy(file_name + index, tmp_file + index)
        tmp_file = self._make_dataset_file(tmp_file)
        return tmp_file

    def _make_dataset_file(self, file_name):
        return make_tmp_dataset_xml(file_name, self.READER_CLASS)

    def setUp(self):
        data_files = [self._generate_chunk_output_file()
                      for i in range(self.NCHUNKS)]
        self._generate_chunk_json(data_files)


class TestGatherSubreads(_SetupGatherApp):
    """
    Test pbsmrtpipe.tools_dev.gather_subreads
    """
    READER_CLASS = SubreadSet
    READER_KWARGS = {'strict': True}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_subreads"
    CHUNK_KEY = "$chunk.subreadset_id"

    def _generate_chunk_output_file(self):
        return self._copy_mock_output_file(pbcore.data.getUnalignedBam())


class TestGatherWrongType(_SetupGatherApp):
    """
    This test checks that a gather task fails when the chunk files are of the
    wrong DataSet type.
    """
    READER_CLASS = ConsensusReadSet
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_ccs"
    CHUNK_KEY = "$chunk.consensusreadset_id"

    def _generate_chunk_output_file(self):
        return self._copy_mock_output_file(pbcore.data.getUnalignedBam())

    def _make_dataset_file(self, file_name):
        return make_tmp_dataset_xml(file_name, SubreadSet)

    def test_run_e2e(self):
        self.assertRaises(AssertionError,
                          super(TestGatherWrongType, self).test_run_e2e)


class TestGatherAlignmentSet(_SetupGatherApp):
    """
    Test pbsmrtpipe.tools_dev.gather_alignments
    """
    READER_CLASS = AlignmentSet
    READER_KWARGS = {'strict': True}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_alignments"
    CHUNK_KEY = "$chunk.alignmentset_id"

    def _generate_chunk_output_file(self):
        return self._copy_mock_output_file(pbcore.data.getBamAndCmpH5()[0])

    def run_after(self, rtc, output_dir):
        super(TestGatherAlignmentSet, self).run_after(rtc,
                                                      output_dir)
        with AlignmentSet(rtc.task.output_files[0]) as ds:
            self.assertTrue(ds.isIndexed)
            self._check_bam_count(ds.toExternalFiles())

    def _check_bam_count(self, files):
        # should still be multiple .bam files
        self.assertNotEqual(len(files), 1)


class TestGatherCCS(_SetupGatherApp):
    """
    Test pbsmrtpipe.tools_dev.gather_ccs
    """
    READER_CLASS = ConsensusReadSet
    READER_KWARGS = {'strict': True}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_ccs"
    CHUNK_KEY = "$chunk.ccsset_id"

    def _generate_chunk_output_file(self):
        return self._copy_mock_output_file(pbcore.data.getCCSBAM())


# FIXME
@skip_if_missing_testdata
class TestGatherCCSAlignmentSet(CompareGatheredRecordsBase,
                                pbcommand.testkit.core.PbTestGatherApp):

    """
    Test pbsmrtpipe.tools_dev.gather_ccs_alignments
    """
    READER_CLASS = ConsensusAlignmentSet
    READER_KWARGS = {'strict': True}
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_ccs_alignments"
    INPUT_FILES = [
        "/pbi/dept/secondary/siv/testdata/pbsmrtpipe-unittest/data/chunk/consensusalignmentset_gather.chunks.json"
    ]
    CHUNK_KEY = "$chunk.ccs_alignmentset_id"


class TestGatherReport(_SetupGatherApp):
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_report"
    CHUNK_KEY = "$chunk.report_id"

    def _generate_chunk_output_file(self):
        tmp_file = tempfile.NamedTemporaryFile(suffix=".json").name
        write_random_report(tmp_file, 5)
        return tmp_file

    def run_after(self, rtc, output_dir):
        report_file = rtc.task.output_files[0]
        r = load_report_from_json(report_file)
        a = {a.id: a.value for a in r.attributes}
        n = {a.id: a.name for a in r.attributes}
        self.assertEqual(a, {"mock_attr_2": 4, "mock_attr_3": 6,
            "mock_attr_0": 0, "mock_attr_1": 2, "mock_attr_4": 8})
        self.assertEqual(n, {"mock_attr_2": "Attr 2", "mock_attr_3": "Attr 3",
            "mock_attr_0": "Attr 0", "mock_attr_1": "Attr 1",
            "mock_attr_4": "Attr 4"})
        keys = [str(a.id) for a in r.attributes]
        # check attribute order
        self.assertEqual(keys, ["mock_attr_0", "mock_attr_1", "mock_attr_2",
                                "mock_attr_3", "mock_attr_4"])


class TestGatherJson(TestGatherReport):
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_json"


class TestGatherContigs(_SetupGatherApp):

    """
    Test pbsmrtpipe.tools_dev.gather_contigs
    """
    READER_CLASS = ContigSet
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_contigs"
    CHUNK_KEY = "$chunk.contigset_id"

    def _generate_chunk_output_file(self):
        fn = tempfile.NamedTemporaryFile(suffix=".fasta").name
        write_random_fasta_records(fn)
        pysam.faidx(fn)
        return self._make_dataset_file(fn)


class TestGatherFasta(_SetupGatherApp):
    """
    Test pbsmrtpipe.tools_dev.gather_fasta
    """
    READER_CLASS = FastaReader
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_fasta"
    CHUNK_KEY = "$chunk.fasta_id"

    def _generate_chunk_output_file(self):
        fn = tempfile.NamedTemporaryFile(suffix=".fasta").name
        write_random_fasta_records(fn,
            prefix="contig{n}".format(n=random.randint(1,10000)))
        return fn


class TestGatherFastq(_SetupGatherApp):

    """
    Test pbsmrtpipe.tools_dev.gather_fastq
    """
    READER_CLASS = FastqReader
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_fastq"
    CHUNK_KEY = "$chunk.fastq_id"

    def _generate_chunk_output_file(self):
        fn = tempfile.NamedTemporaryFile(suffix=".fastq").name
        write_random_fastq_records(fn,
            prefix="contig{n}".format(n=random.randint(1,10000)))
        return fn


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
