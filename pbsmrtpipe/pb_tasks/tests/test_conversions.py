import os
import unittest
import logging
import os.path as op
import subprocess

from pbcommand.testkit import PbTestApp

log = logging.getLogger(__name__)

DATA = op.join(op.dirname(__file__), "data")

# XXX hacks to make sure tools are actually available
HAVE_BAX2BAM = False
try:
    rc = subprocess.call(["bax2bam", "--version"])
except OSError as e:
    if e.errno != os.errno.ENOENT:
        raise
else:
    HAVE_BAX2BAM = True

HAVE_BAM2FASTX = False
try:
    rc = subprocess.call(["bam2fasta", "--help"])
except OSError as e:
    if e.errno != os.errno.ENOENT:
        raise
else:
    HAVE_BAM2FASTX = True


@unittest.skipUnless(HAVE_BAX2BAM, "Missing bax2bam")
class TestBax2Bam(PbTestApp):
    TASK_ID = "pbsmrtpipe.tasks.h5_subreads_to_subread"
    DRIVER_EMIT = 'python -m pbsmrtpipe.pb_tasks.pacbio emit-tool-contract {i} '.format(i=TASK_ID)
    DRIVER_RESOLVE = 'python -m pbsmrtpipe.pb_tasks.pacbio run-rtc '

    # User Provided values
    # Abspath'ed temp files will be automatically generated
    INPUT_FILES = [
        "m140905_042212_sidney_c100564852550000001823085912221377_s1_X0.1.hdfsubreadset.xml"
    ]
    MAX_NPROC = 24

    RESOLVED_NPROC = 1
    RESOLVED_TASK_OPTIONS = {}
    RESOLVED_IS_DISTRIBUTED = True

    # FIXME bax2bam doesn't like relative paths in dataset XML
    def setUp(self):
        xml_file = self.INPUT_FILES[0]
        subprocess.call(["dataset.py", "create", "--type=HdfSubreadSet",
            self.INPUT_FILES[0], op.join(DATA, "m140905_042212_sidney_c100564852550000001823085912221377_s1_X0.1.bax.h5")])


@unittest.skipUnless(HAVE_BAM2FASTX, "Missing bam2fastx")
class TestBam2Fasta(PbTestApp):
    TASK_ID = "pbsmrtpipe.tasks.bam2fasta"
    DRIVER_EMIT = 'python -m pbsmrtpipe.pb_tasks.pacbio emit-tool-contract {i} '.format(i=TASK_ID)
    DRIVER_RESOLVE = 'python -m pbsmrtpipe.pb_tasks.pacbio run-rtc '
    INPUT_FILES = [ "test.subreadset.xml" ]
    MAX_NPROC = 24
    RESOLVED_NPROC = 1
    RESOLVED_TASK_OPTIONS = {}
    RESOLVED_IS_DISTRIBUTED = True

    # FIXME see above...
    def setUp(self):
        xml_file = self.INPUT_FILES[0]
        subprocess.call(["dataset.py", "create", "--type=SubreadSet",
            self.INPUT_FILES[0], op.join(DATA, "test.bam") ])


@unittest.skipUnless(HAVE_BAM2FASTX, "Missing bam2fastx")
class TestBam2Fastq(TestBam2Fasta):
    TASK_ID = "pbsmrtpipe.tasks.bam2fastq"
