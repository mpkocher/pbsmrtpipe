import os
import logging
import json
import re

from .base import TestBase
from pbsmrtpipe.testkit.base import monkey_patch
from pbsmrtpipe.testkit.validators2 import (ValidateFasta, ValidateJsonReport)

log = logging.getLogger(__name__)


@monkey_patch
class TestDataStore(TestBase):

    """
    Test to see of the core job directory structure and files exist
    """
    DIRS = ('html', 'workflow', 'tasks')

    def test_load_datastore_from_file(self):
        """
        Can load Datastore from Json
        :return:
        """
        p = os.path.join(self.job_dir, "workflow", "datastore.json")
        with open(p, 'r') as r:
            d = json.loads(r.read())
        self.assertIsInstance(d, dict)


@monkey_patch
class TestDataStoreFiles(TestBase):

    """
    Load Files by type in datastore and validate them using pbvalidate
    """

    DATASTORE_FILE_VALIDATORS = (ValidateFasta, ValidateJsonReport)


@monkey_patch
class TestDataStoreFileLabels(TestBase):

    def test_datastore_file_name_and_description(self):
        """
        Make sure output files have non-blank name and description.
        """
        p = os.path.join(self.job_dir, "workflow", "datastore.json")
        with open(p, 'r') as r:
            d = json.loads(r.read())
            for fd in d["files"]:
                self.assertTrue(re.search("[a-zA-Z0-9]{1,}", fd["name"]))
                self.assertTrue(re.search("[a-zA-Z0-9]{1,}", fd["description"]))
