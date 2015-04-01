import logging

from pbsmrtpipe.models import TaskTypes

from task_test_base import _TaskTestBase
from pbsmrtpipe.schema_opt_utils import to_opt_id

log = logging.getLogger(__name__)


class TestFilterTask(_TaskTestBase):
    TASK_ID = "pbsmrtpipe.tasks.filter"
    INPUT_FILE_NAMES = ["movie.fofn", 'movie_report.json']
    TASK_OPTIONS = {to_opt_id('filter_trim'): False,
                    to_opt_id('filter_artifact_score'): -1,
                    to_opt_id("use_subreads"): True}

    MAX_NPROC = 24

    NCOMMANDS = 2
    RESOLVED_NPROC = 1
    RESOLVED_TASK_OPTIONS = {}
    RESOLVED_TASK_TYPE = TaskTypes.DISTRIBUTED


class BasicFilterTaskWithFilters(TestFilterTask):
    NCOMMANDS = 1
    TASK_OPTIONS = {to_opt_id('filter_read_score'): 80,
                    to_opt_id('filter_min_subread_length'): 2000}


class TaskAllOptsTestFilterTask(TestFilterTask):
    NCOMMANDS = 2
    TASK_OPTIONS = {to_opt_id('filter_artifact_score'): -7,
                    to_opt_id('filter_max_read_length'): 10000,
                    to_opt_id('filter_min_read_length'): 1000}


class TestFilterSubread(_TaskTestBase):
    TASK_ID = 'pbsmrtpipe.tasks.filter_subreads'
    TASK_OPTIONS = {to_opt_id("use_subreads"): True}
    INPUT_FILE_NAMES = ['movie.fofn', 'movie_report.json', 'region.fofn', 'region_report.json']

    NCOMMANDS = 2
    RESOLVED_TASK_TYPE = TaskTypes.DISTRIBUTED
    RESOLVED_TASK_OPTIONS = {to_opt_id('use_subreads'): True}


class BasicSubreadFilterTask(_TaskTestBase):
    TASK_ID = "pbsmrtpipe.tasks.filter_subread_summary"
    INPUT_FILE_NAMES = ["region.fofn"]

    NCOMMANDS = 1
    RESOLVED_NPROC = 1
    RESOLVED_TASK_TYPE = TaskTypes.DISTRIBUTED
