
import tempfile
import unittest
import logging
import os.path as op
import subprocess

from pbcore.io import (FastaReader, FastqReader, openDataSet, HdfSubreadSet,
    SubreadSet)
import pbcore.data
from pbcommand.testkit import PbTestApp
from pbcommand.utils import which

log = logging.getLogger(__name__)

DATA = op.join(op.dirname(__file__), "data")


class Constants(object):
    BAX2BAM = "bax2bam"
    BAM2FASTA = "bam2fasta"

# XXX hacks to make sure tools are actually available
HAVE_BAX2BAM = which(Constants.BAX2BAM) is not None
HAVE_BAM2FASTX = which(Constants.BAM2FASTA) is not None
DATA_DIR = "/pbi/dept/secondary/siv/testdata"
HAVE_DATA_DIR = op.isdir(DATA_DIR)

@unittest.skipUnless(HAVE_BAX2BAM and HAVE_DATA_DIR, "Missing bax2bam")
class TestBax2Bam(PbTestApp):
    TASK_ID = "pbsmrtpipe.tasks.h5_subreads_to_subread"
    DRIVER_EMIT = 'python -m pbsmrtpipe.pb_tasks.pacbio emit-tool-contract {i} '.format(i=TASK_ID)
    DRIVER_RESOLVE = 'python -m pbsmrtpipe.pb_tasks.pacbio run-rtc '

    # XXX we want to test that this behaves properly when multiple movies are
    # supplied as input, so we make an HdfSubreadSet on the fly from various
    # bax files in testdata
    INPUT_FILES = [
        tempfile.NamedTemporaryFile(suffix=".hdfsubreadset.xml").name,
    ]
    MAX_NPROC = 24

    RESOLVED_NPROC = 1
    RESOLVED_TASK_OPTIONS = {}
    RESOLVED_IS_DISTRIBUTED = True

    @classmethod
    def setUpClass(cls):
        FILES = [
            DATA_DIR + "/SA3-RS/lambda/2372215/0007_tiny/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.1.bax.h5",
            pbcore.data.getBaxH5_v23()[0]
            #DATA_DIR + "/SA3-RS/lambda/2590980/0008/Analysis_Results/m141115_075238_ethan_c100699872550000001823139203261572_s1_p0.1.bax.h5",
        ]
        ds = HdfSubreadSet(*FILES)
        assert len(set([f.movieName for f in ds.resourceReaders()])) == 2
        ds.write(cls.INPUT_FILES[0])


    def run_after(self, rtc, output_dir):
        with SubreadSet(rtc.task.output_files[0]) as ds_out:
            self.assertEqual(len(ds_out.toExternalFiles()), 2)


@unittest.skipUnless(HAVE_BAM2FASTX and HAVE_DATA_DIR, "Missing bam2fastx")
class TestBam2Fasta(PbTestApp):
    TASK_ID = "pbsmrtpipe.tasks.bam2fasta"
    DRIVER_EMIT = 'python -m pbsmrtpipe.pb_tasks.pacbio emit-tool-contract {i} '.format(i=TASK_ID)
    DRIVER_RESOLVE = 'python -m pbsmrtpipe.pb_tasks.pacbio run-rtc '
    INPUT_FILES = [ "/pbi/dept/secondary/siv/testdata/SA3-DS/lambda/2372215/0007_micro/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml" ]
    MAX_NPROC = 24
    RESOLVED_NPROC = 1
    RESOLVED_TASK_OPTIONS = {}
    RESOLVED_IS_DISTRIBUTED = True
    READER_CLASS = FastaReader

    def run_after(self, rtc, output_dir):
        n_actual = n_expected = 0
        with openDataSet(self.INPUT_FILES[0]) as ds:
            n_expected = len([ rec for rec in ds ])
        with self.READER_CLASS(rtc.task.output_files[0]) as f:
            n_actual = len([ rec for rec in f ])
        self.assertEqual(n_actual, n_expected)


@unittest.skipUnless(HAVE_BAM2FASTX and HAVE_DATA_DIR, "Missing bam2fastx")
class TestBam2Fastq(TestBam2Fasta):
    TASK_ID = "pbsmrtpipe.tasks.bam2fastq"
    DRIVER_EMIT = 'python -m pbsmrtpipe.pb_tasks.pacbio emit-tool-contract {i} '.format(i=TASK_ID)
    READER_CLASS = FastqReader


@unittest.skipUnless(HAVE_BAM2FASTX and HAVE_DATA_DIR, "Missing bam2fastx")
class TestBam2FastaCCS(TestBam2Fasta):
    TASK_ID = "pbsmrtpipe.tasks.bam2fasta_ccs"
    DRIVER_EMIT = 'python -m pbsmrtpipe.pb_tasks.pacbio emit-tool-contract {i} '.format(i=TASK_ID)
    INPUT_FILES = [ "/pbi/dept/secondary/siv/testdata/pbsmrtpipe-unittest/data/chunk/pbccs.tasks.ccs-1/ccs.consensusreadset.xml" ]
    READER_CLASS = FastaReader


@unittest.skipUnless(HAVE_BAM2FASTX and HAVE_DATA_DIR, "Missing bam2fastx")
class TestBam2FastqCCS(TestBam2FastaCCS):
    TASK_ID = "pbsmrtpipe.tasks.bam2fastq_ccs"
    DRIVER_EMIT = 'python -m pbsmrtpipe.pb_tasks.pacbio emit-tool-contract {i} '.format(i=TASK_ID)
    READER_CLASS = FastqReader
