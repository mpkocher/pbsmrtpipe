
"""
Test that is_distributed is correct in resolved tool contracts.
"""

import os
import json
import logging

from pbcommand.pb_io.tool_contract_io import (load_tool_contract_from,
    load_resolved_tool_contract_from)
from .base import TestBase

log = logging.getLogger(__name__)


class TestResolvedToolContracts(TestBase):

    def test_resolved_tool_contract_is_distributed(self):
        workflow_options_json = os.path.join(self.job_dir, "workflow",
            "options-workflow.json")
        workflow_options = json.load(open(workflow_options_json))
        distributed_mode = workflow_options["pbsmrtpipe.options.distributed_mode"]
        tasks_dir = os.path.join(self.job_dir, "tasks")
        for task_dir in os.listdir(tasks_dir):
            tc_file = os.path.join(tasks_dir, task_dir, "tool-contract.json")
            tc = load_tool_contract_from(tc_file)
            rtc_file = os.path.join(tasks_dir, task_dir,
                "resolved-tool-contract.json")
            rtc = load_resolved_tool_contract_from(rtc_file)
            if distributed_mode and tc.task.is_distributed:
                self.assertTrue(rtc.task.is_distributed,
                    "Resolved tool contract {f} has unexpected is_distributed=False".format(f=rtc_file))
            else:
                self.assertFalse(rtc.task.is_distributed,
                    "Resolved tool contract {f} has unexpected is_distributed=True".format(f=rtc_file))
