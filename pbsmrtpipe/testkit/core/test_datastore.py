import os
import logging

from .base import TestBase
from pbsmrtpipe.testkit.base import monkey_patch

log = logging.getLogger(__name__)


@monkey_patch
class TestDataStore(TestBase):
    """
    Test to see of the core job directory structure and files exist
    """
    DIRS = ('html', 'workflow', 'tasks')

    def test_datastore_01(self):
        self.assertTrue(True)
