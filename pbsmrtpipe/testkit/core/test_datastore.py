
import unittest
import os
import logging
import json
import re

from pbcommand.pb_io.report import load_report_from_json
from pbcommand.validators import validate_report
from pbcommand.models import FileTypes
from pbcore.io import getDataSetUuid

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


class TestDataStoreUuids(TestBase):
    """
    Verify that report and DataSet UUIDs are propagated to the datastore.
    """

    def test_datastore_report_file_uuid(self):
        p = os.path.join(self.job_dir, "workflow", "datastore.json")
        with open(p, 'r') as r:
            d = json.loads(r.read())
            n_tested = 0
            for file_info in d['files']:
                if file_info['fileTypeId'] == FileTypes.REPORT.file_type_id:
                    rpt = load_report_from_json(file_info['path'])
                    self.assertEqual(rpt.uuid, file_info['uniqueId'],
                                     "{p}: {u1} != {u2}".format(
                                     p=file_info['path'],
                                     u1=rpt.uuid,
                                     u2=file_info['uniqueId']))
                    n_tested += 1
            if n_tested == 0:
                raise unittest.SkipTest("No Report JSON files in datastore.")

    def test_datastore_dataset_file_uuid(self):
        FILE_TYPE_IDS = ["DS_SUBREADS_H5", "DS_SUBREADS", "DS_CCS", "DS_REF",
                         "DS_ALIGN", "DS_CONTIG", "DS_BARCODE", "DS_ALIGN_CCS"]
        DATASET_FILE_TYPES = set([getattr(FileTypes, t).file_type_id
                                  for t in FILE_TYPE_IDS])
        p = os.path.join(self.job_dir, "workflow", "datastore.json")
        with open(p, 'r') as r:
            d = json.loads(r.read())
            n_tested = 0
            for file_info in d['files']:
                if file_info['fileTypeId'] in DATASET_FILE_TYPES:
                    uuid = getDataSetUuid(file_info['path'])
                    self.assertEqual(uuid, file_info['uniqueId'],
                                     "{p}: {u1} != {u2}".format(
                                     p=file_info['path'],
                                     u1=uuid,
                                     u2=file_info['uniqueId']))
                    n_tested += 1
            if n_tested == 0:
                raise unittest.SkipTest("No DataSet XML files in datastore.")


class TestReports(TestBase):
    def test_datastore_report_validation(self):
        p = os.path.join(self.job_dir, "workflow", "datastore.json")
        with open(p, 'r') as r:
            d = json.loads(r.read())
            n_tested = 0
            for file_info in d['files']:
                if file_info['fileTypeId'] == FileTypes.REPORT.file_type_id:
                    errors = validate_report(file_info['path'], False)
                    if len(errors) > 0:
                        self.fail("Report validation failed:\n{e}".format(
                                  e="\n".join([str(e) for e in errors])))
