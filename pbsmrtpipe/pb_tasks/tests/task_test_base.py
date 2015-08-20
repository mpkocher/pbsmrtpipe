import os
import tempfile
import unittest
import logging
import functools
import copy

import pbsmrtpipe.loader
import pbsmrtpipe.opts_graph as OG
import pbsmrtpipe.graph.bgraph as B
from pbsmrtpipe.core import REGISTERED_FILE_TYPES
from pbcommand.models import TaskTypes


log = logging.getLogger(__name__)

TEST_DIR = os.path.dirname(__file__)
TEST_DATA_DIR = os.path.join(TEST_DIR, 'data')


def get_data_file(file_name):
    return os.path.join(TEST_DATA_DIR, file_name)


def _write_dummy_file(file_name):
    with open(file_name, "w") as f:
        f.write("dummy test file\n")


def _write_dummy_file_from_name(task_dir, name):
    if '/' in name:
        # there's an assumption that the last item is a file
        names = name.split('/')
        dir_name = copy.copy(task_dir)
        for x in names[:-1]:
            p = os.path.join(dir_name, x)
            if not os.path.exists(p):
                os.mkdir(p)
            dir_name = p

        file_name = os.path.join(dir_name, names[-1])
    else:
        file_name = os.path.join(task_dir, name)

    _write_dummy_file(file_name)
    return file_name


class _TaskTestBase(unittest.TestCase):

    """
    The name must be prepended with _ so that nose doesn't run it
    """
    TASK_ID = None
    # If the file name is given as abspath, then it's used, otherwise a dummy
    # file will be created
    INPUT_FILE_NAMES = None
    TASK_OPTIONS = {}
    MAX_NPROC = 1
    MAX_NCHUNKS = 1

    # Values that will be validated
    NCOMMANDS = 1
    RESOLVED_TASK_OPTIONS = {}
    RESOLVED_NPROC = 1
    RESOLVED_TASK_TYPE = True

    def setUp(self):
        import pbsmrtpipe.loader

        self.RTASKS = pbsmrtpipe.loader.load_all_tool_contracts()
        self.temp_dir = tempfile.mkdtemp(suffix="_pbtest_job_dir")

    # def tearDown(self):
    #     if os.path.exists(self.temp_dir):
    #         shutil.rmtree(self.temp_dir)

    def _to_meta_tasks(self):
        if self.TASK_ID not in self.RTASKS:
            raise KeyError("Unable to find task id {i} ({n} tasks). Test {x}".format(i=self.TASK_ID, n=len(self.RTASKS), x=self.__class__.__name__))

        return self.RTASKS[self.TASK_ID]

    def _to_task(self):
        meta_task = self._to_meta_tasks()
        log.debug(meta_task)
        log.debug(meta_task.summary())

        tasks_dir = os.path.join(self.temp_dir, 'tasks')
        os.mkdir(tasks_dir)
        task_dir = os.path.join(tasks_dir, '{i}-0'.format(i=meta_task.task_id))
        os.mkdir(task_dir)

        input_files = []
        for x in self.INPUT_FILE_NAMES:
            if os.path.isabs(x):
                input_files.append(x)
            else:
                f = _write_dummy_file_from_name(task_dir, x)
                input_files.append(f)

        file_type_id_to_count = {file_type.file_type_id: 0 for _, file_type in REGISTERED_FILE_TYPES.iteritems()}

        to_resolve_files = functools.partial(B.resolve_io_files, file_type_id_to_count)

        resolve_resources_func = functools.partial(B.resolve_di_resources, task_dir)

        task = OG.meta_task_to_task(meta_task, input_files, self.TASK_OPTIONS, task_dir, self.MAX_NPROC, self.MAX_NCHUNKS, resolve_resources_func, to_resolve_files)

        log.debug(task)
        for cmd in task.cmds:
            log.debug("\n" + cmd)
        log.debug(task.resolved_options)
        return task

    def test_task_id(self):
        t = self._to_task()
        self.assertEqual(t.task_id, self.TASK_ID)

    def test_resolved_nproc(self):
        t = self._to_task()
        self.assertEqual(t.nproc, self.RESOLVED_NPROC)

    def test_task_options(self):
        t = self._to_task()
        for k, v in self.TASK_OPTIONS.iteritems():
            self.assertEqual(t.resolved_options[k], v)

    def test_ncommands(self):
        t = self._to_task()
        self.assertEqual(len(t.cmds), self.NCOMMANDS)

    def test_ncommands_type(self):
        t = self._to_task()
        self.assertIsInstance(t.cmds, (list, tuple))

    def test_resolved_options(self):

        t = self._to_task()

        for opt, value in self.RESOLVED_TASK_OPTIONS.iteritems():
            self.assertIn(opt, t.resolved_options.keys())
            self.assertEquals(t.resolved_options[opt], value)

    @unittest.skip
    def test_task_type(self):
        t = self._to_task()
        self.assertEqual(t.task_type, self.RESOLVED_TASK_TYPE)
