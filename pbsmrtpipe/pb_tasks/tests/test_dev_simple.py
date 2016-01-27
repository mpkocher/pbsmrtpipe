import logging
import os

from pbcommand.models import TaskTypes
from pbsmrtpipe.schema_opt_utils import to_opt_id

from task_test_base import _TaskTestBase, TEST_DATA_DIR, get_data_file
from pbcommand.testkit import PbTestApp

log = logging.getLogger(__name__)


def _get_test_data_file(file_name):
    return os.path.join(TEST_DATA_DIR, file_name)


class TestDevSimpleTaskDefaults(PbTestApp):
    TASK_ID = "pbsmrtpipe.tasks.dev_simple_hello_world"
    DRIVER_EMIT = 'python -m pbsmrtpipe.pb_tasks.dev emit-tool-contract {i} '.format(i=TASK_ID)
    DRIVER_RESOLVE = 'python -m pbsmrtpipe.pb_tasks.dev run-rtc '

    # User Provided values
    # Abspath'ed temp files will be automatically generated
    INPUT_FILES = [get_data_file('file.txt')]
    MAX_NPROC = 24

    RESOLVED_NPROC = 1
    RESOLVED_TASK_OPTIONS = {}
    IS_DISTRIBUTED = True
    RESOLVED_IS_DISTRIBUTED = False


class TestTestDevSimpleTask(TestDevSimpleTaskDefaults):
    # Test different set of options
    TASK_OPTIONS = {to_opt_id('dev.hello_message'): "Custom Message"}

    #RESOLVED_TASK_OPTIONS = {to_opt_id('dev.hello_message'): "Custom Message"}
