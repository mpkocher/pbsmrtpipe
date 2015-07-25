import logging
import os

from pbsmrtpipe.models import TaskTypes
from pbsmrtpipe.schema_opt_utils import to_opt_id

from task_test_base import _TaskTestBase, TEST_DATA_DIR

log = logging.getLogger(__name__)


def _get_test_data_file(file_name):
    return os.path.join(TEST_DATA_DIR, file_name)


class TestDevSimpleTaskDefaults(_TaskTestBase):
    TASK_ID = "pbsmrtpipe.tasks.dev_simple_hello_world"
    # User Provided values
    # Abspath'ed temp files will be automatically generated
    INPUT_FILE_NAMES = ["file_01.txt"]
    # Use the default values defined in the jsonschema
    TASK_OPTIONS = {}

    MAX_NPROC = 24

    # Resolved Values
    NCOMMANDS = 2
    RESOLVED_NPROC = 1
    RESOLVED_TASK_OPTIONS = {}
    RESOLVED_TASK_TYPE = TaskTypes.LOCAL


class TestTestDevSimpleTask(TestDevSimpleTaskDefaults):
    # Test different set of options
    TASK_OPTIONS = {to_opt_id('dev.hello_message'): "Custom Message"}

    RESOLVED_TASK_OPTIONS = {to_opt_id('dev.hello_message'): "Custom Message"}


class TestDependencyInjectionDevExampleTask(_TaskTestBase):
    TASK_ID = 'pbsmrtpipe.tasks.dev_di_example'
    INPUT_FILE_NAMES = [_get_test_data_file("dev_di_example_report.json")]
    TASK_OPTIONS = {}

    MAX_NPROC = 24

    NCOMMANDS = 1
    # The DI will $max_nproc / 2 + 2
    RESOLVED_NPROC = 14
    RESOLVED_TASK_TYPE = TaskTypes.LOCAL
    RESOLVED_TASK_OPTIONS = {}
