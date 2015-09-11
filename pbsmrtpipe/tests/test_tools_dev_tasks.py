
"""
Unit tests for the various scatter/gather tools in pbsmrtipipe.tools_dev.
"""

import unittest
import os.path as op
import re

from pbcommand.pb_io.common import load_pipeline_chunks_from_json
from pbcommand.pb_io.report import load_report_from_json
from pbcore.io import SubreadSet, ContigSet, FastaReader, ConsensusReadSet
import pbcommand.testkit.core

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

    def run_after(self, rtc, output_dir):
        json_file = rtc.task.output_files[0]
        chunks = load_pipeline_chunks_from_json(json_file)
        n_rec = 0
        with self.READER_CLASS(self.INPUT_FILES[0]) as f:
            n_rec = len([r for r in f])
        n_rec_chunked = 0
        for chunk in chunks:
            d = chunk.chunk_d
            with self.READER_CLASS(d[self.CHUNK_KEYS[0]]) as cs:
                n_rec_chunked += len([r for r in cs])
        self.assertEqual(n_rec_chunked, n_rec)


class CompareGatheredRecordsBase(object):
    READER_CLASS = None

    def run_after(self, rtc, output_dir):
        gathered_file = rtc.task.output_files[0]
        chunks = load_pipeline_chunks_from_json(self.INPUT_FILES[0])
        n_rec = 0
        with self.READER_CLASS(gathered_file) as f:
            n_rec = len([r for r in f])
        n_rec_chunked = 0
        for chunk in chunks:
            d = chunk.chunk_d
            with self.READER_CLASS(d[self.CHUNK_KEY]) as cs:
                n_rec_chunked += len([r for r in cs])
        self.assertEqual(n_rec_chunked, n_rec)


class ScatterSequenceBase(CompareScatteredRecordsBase):

    @classmethod
    def setUpClass(cls):
        super(ScatterSequenceBase, cls).setUpClass()
        _write_fasta_or_contigset(cls.INPUT_FILES[0])


class TestScatterFilterFasta(ScatterSequenceBase,
                             pbcommand.testkit.core.PbTestScatterApp):
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
    READER_CLASS = ContigSet
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_contigset"
    INPUT_FILES = [
        get_temp_file(suffix=".contigset.xml")
    ]
    MAX_NCHUNKS = 12
    RESOLVED_MAX_NCHUNKS = 12
    CHUNK_KEYS = ("$chunk.contigset_id",)


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestScatterSubreadZMWs(CompareScatteredRecordsBase,
                             pbcommand.testkit.core.PbTestScatterApp):
    READER_CLASS = SubreadSet
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
    READER_CLASS = ConsensusReadSet
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_ccs_zmws"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pbsmrtpipe-unittest/data/chunk/ccs.consensusreadset.xml"
    ]
    MAX_NCHUNKS = 8
    RESOLVED_MAX_NCHUNKS = 8
    CHUNK_KEYS = ("$chunk.ccsset_id",)


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestGatherCCS(CompareGatheredRecordsBase,
                    pbcommand.testkit.core.PbTestGatherApp):

    READER_CLASS = ConsensusReadSet
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.gather_ccs"
    INPUT_FILES = [
        "/mnt/secondary-siv/testdata/pbsmrtpipe-unittest/data/chunk/ccs_gather.chunks.json"
    ]
    CHUNK_KEY = "$chunk.ccsset_id"


@unittest.skipUnless(op.isdir(MNT_DATA), "Missing %s" % MNT_DATA)
class TestGatherReport(pbcommand.testkit.core.PbTestGatherApp):

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
