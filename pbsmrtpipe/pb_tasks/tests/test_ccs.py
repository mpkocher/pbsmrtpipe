import logging

from task_test_base import _TaskTestBase

log = logging.getLogger(__name__)


class TestReadsOfInsert(_TaskTestBase):
    TASK_ID = "pbsmrtpipe.tasks.reads_of_insert"
    INPUT_FILE_NAMES = ['input.fofn']

    NCOMMANDS = 6


class TestReadsOfInsertReport(_TaskTestBase):
    TASK_ID = "pbsmrtpipe.tasks.roi_report"
    INPUT_FILE_NAMES = ["ccs.fofn"]
