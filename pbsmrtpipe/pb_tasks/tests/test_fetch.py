import logging

from pbsmrtpipe.models import TaskTypes

from task_test_base import _TaskTestBase
from pbsmrtpipe.schema_opt_utils import to_opt_id

log = logging.getLogger(__name__)


class TestFastaReferenceInfoConverter(_TaskTestBase):
    TASK_ID = 'pbsmrtpipe.tasks.reference_converter'
    INPUT_FILE_NAMES = ['my.fasta']

    NCOMMANDS = 4
    RESOLVED_TASK_TYPE = TaskTypes.DISTRIBUTED
