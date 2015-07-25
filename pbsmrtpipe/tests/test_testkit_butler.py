import os
import unittest
import logging

from base import TEST_DATA_DIR

import pbsmrtpipe.testkit.butler as B
from pbsmrtpipe.testkit.butler import ButlerTask, ButlerWorkflow

log = logging.getLogger(__name__)


class _TestSanity(unittest.TestCase):
    FILE_NAME = 'example_butler_workflow.cfg'
    BUTLER_KLASS = ButlerWorkflow

    def setUp(self):
        self.path = os.path.join(TEST_DATA_DIR, self.FILE_NAME)

    def _to_butler(self):
        return B.config_parser_to_butler(self.path)

    def test_parsing_cfg_to_butler(self):
        b = self._to_butler()
        self.assertIsInstance(b, self.BUTLER_KLASS)


class TestParsingButlerWorkflowCfg(_TestSanity):
    pass


class TestParsingButlerWorkflowCfgWithPrefix(_TestSanity):
    FILE_NAME = 'example_butler_workflow_with_prefix.cfg'

    def test_parsing_preset_xml(self):
        b = self._to_butler()
        log.debug(b)
        self.assertIsInstance(b.preset_xml, str)


class TestParsingButlerTaskCfg(_TestSanity):
    FILE_NAME = 'example_butler_task.cfg'
    BUTLER_KLASS = ButlerTask
