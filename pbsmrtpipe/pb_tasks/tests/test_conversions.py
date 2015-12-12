
import tempfile
import unittest
import logging
import os.path as op

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


SIV_DATA_DIR = "/pbi/dept/secondary/siv/testdata"


def _to_skip_msg(exe):
    return "Missing {e} or {d}".format(d=SIV_DATA_DIR, e=exe)

# XXX hacks to make sure tools are actually available
HAVE_BAX2BAM = which(Constants.BAX2BAM) is not None
HAVE_BAM2FASTX = which(Constants.BAM2FASTA) is not None
HAVE_DATA_DIR = op.isdir(SIV_DATA_DIR)


HAVE_DATA_AND_BAX2BAM = HAVE_BAX2BAM and HAVE_DATA_DIR
SKIP_MSG_BAX2BAM = _to_skip_msg(Constants.BAX2BAM)

HAVE_DATA_AND_BAM2FASTX = HAVE_BAM2FASTX and HAVE_DATA_DIR
SKIP_MSG_BAM2FX = _to_skip_msg(Constants.BAM2FASTA)

skip_unless_bax2bam = unittest.skipUnless(HAVE_DATA_AND_BAX2BAM, SKIP_MSG_BAX2BAM)
skip_unless_bam2fastx = unittest.skipUnless(HAVE_DATA_AND_BAM2FASTX, SKIP_MSG_BAM2FX)


def _get_bax2bam_inputs():
    """Little hackery to get the setup class Inputs and to avoid calls to
    setupclass if skiptest is used

    Nat: we want to test that this behaves properly when multiple movies are
    supplied as input, so we make an HdfSubreadSet on the fly from various
    bax files in testdata
    """
    if HAVE_DATA_AND_BAX2BAM:
        hdf_subread_xml = tempfile.NamedTemporaryFile(suffix=".hdfsubreadset.xml").name

        bax_files = (SIV_DATA_DIR + "/SA3-RS/lambda/2372215/0007_tiny/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.1.bax.h5",
                     pbcore.data.getBaxH5_v23()[0])

        ds = HdfSubreadSet(*bax_files)
        assert len(set([f.movieName for f in ds.resourceReaders()])) == 2
        ds.write(hdf_subread_xml)
        return [hdf_subread_xml]
    else:
        # Assume the test data isn't found and the test won't be run
        return ["/path/to/this-test-should-be-skipped.txt"]


@skip_unless_bax2bam
class TestBax2Bam(PbTestApp):
    TASK_ID = "pbsmrtpipe.tasks.h5_subreads_to_subread"
    DRIVER_EMIT = 'python -m pbsmrtpipe.pb_tasks.pacbio emit-tool-contract {i} '.format(i=TASK_ID)
    DRIVER_RESOLVE = 'python -m pbsmrtpipe.pb_tasks.pacbio run-rtc '

    # See comments above
    INPUT_FILES = _get_bax2bam_inputs()
    MAX_NPROC = 24

    RESOLVED_NPROC = 1
    RESOLVED_TASK_OPTIONS = {}
    RESOLVED_IS_DISTRIBUTED = True

    def run_after(self, rtc, output_dir):
        with SubreadSet(rtc.task.output_files[0]) as ds_out:
            self.assertEqual(len(ds_out.toExternalFiles()), 2)


@skip_unless_bam2fastx
class TestBam2Fasta(PbTestApp):
    TASK_ID = "pbsmrtpipe.tasks.bam2fasta"
    DRIVER_EMIT = 'python -m pbsmrtpipe.pb_tasks.pacbio emit-tool-contract {i} '.format(i=TASK_ID)
    DRIVER_RESOLVE = 'python -m pbsmrtpipe.pb_tasks.pacbio run-rtc '
    INPUT_FILES = ["/pbi/dept/secondary/siv/testdata/SA3-DS/lambda/2372215/0007_micro/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml"]
    MAX_NPROC = 24
    RESOLVED_NPROC = 1
    RESOLVED_IS_DISTRIBUTED = True
    READER_CLASS = FastaReader

    def _get_counts(self, rtc):
        with openDataSet(self.INPUT_FILES[0]) as ds:
            n_expected = len([rec for rec in ds])
        with self.READER_CLASS(rtc.task.output_files[0]) as f:
            n_actual = len([rec for rec in f])
        return n_expected, n_actual

    def run_after(self, rtc, output_dir):
        n_expected, n_actual = self._get_counts(rtc)
        self.assertEqual(n_actual, n_expected)


@skip_unless_bam2fastx
class TestBam2Fastq(TestBam2Fasta):
    TASK_ID = "pbsmrtpipe.tasks.bam2fastq"
    DRIVER_EMIT = 'python -m pbsmrtpipe.pb_tasks.pacbio emit-tool-contract {i} '.format(i=TASK_ID)
    READER_CLASS = FastqReader


@skip_unless_bam2fastx
class TestBam2FastqFiltered(TestBam2Fastq):
    TASK_OPTIONS = {"pbsmrtpipe.task_options.min_subread_length": 3000}
    RESOLVED_TASK_OPTIONS = {"pbsmrtpipe.task_options.min_subread_length": 3000}

    def run_after(self, rtc, output_dir):
        n_expected, n_actual = self._get_counts(rtc)
        self.assertTrue(0 < n_actual < n_expected)


@skip_unless_bam2fastx
class TestBam2FastaCCS(TestBam2Fasta):
    TASK_ID = "pbsmrtpipe.tasks.bam2fasta_ccs"
    DRIVER_EMIT = 'python -m pbsmrtpipe.pb_tasks.pacbio emit-tool-contract {i} '.format(i=TASK_ID)
    INPUT_FILES = ["/pbi/dept/secondary/siv/testdata/pbsmrtpipe-unittest/data/chunk/pbccs.tasks.ccs-1/ccs.consensusreadset.xml"]
    READER_CLASS = FastaReader


@skip_unless_bam2fastx
class TestBam2FastqCCS(TestBam2FastaCCS):
    TASK_ID = "pbsmrtpipe.tasks.bam2fastq_ccs"
    DRIVER_EMIT = 'python -m pbsmrtpipe.pb_tasks.pacbio emit-tool-contract {i} '.format(i=TASK_ID)
    READER_CLASS = FastqReader
