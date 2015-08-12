import logging
import os.path as op
import subprocess

from pbcommand.models import TaskTypes
from pbsmrtpipe.schema_opt_utils import to_opt_id

from task_test_base import _TaskTestBase, TEST_DATA_DIR, get_data_file
from pbcommand.testkit import PbTestApp

log = logging.getLogger(__name__)

DATA = op.join(op.dirname(__file__), "data")

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
