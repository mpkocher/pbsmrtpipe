import logging
import os
import unittest

import pbsmrtpipe.mock as M
from pbsmrtpipe.models import FileTypes
from pbsmrtpipe.dataset_io import dispatch_metadata_resolver, DatasetMetadata

from base import get_data_file, get_temp_file

log = logging.getLogger(__name__)


class DatasetFastaTest(unittest.TestCase):

    FILE_NAME = 'small.fasta'
    NRECORDS = 1000
    FILE_TYPE = FileTypes.FASTA

    def test_01(self):
        p = get_data_file(self.FILE_NAME)
        ds_metadata = dispatch_metadata_resolver(self.FILE_TYPE, p)
        self.assertIsInstance(ds_metadata, DatasetMetadata)
        self.assertEquals(ds_metadata.nrecords, self.NRECORDS)


class DatasetFastqTest(DatasetFastaTest):
    FILE_NAME = 'small.fastq'
    NRECORDS = 999
    FILE_TYPE = FileTypes.FASTQ


class DatasetFofnTest(unittest.TestCase):

    FILE_TYPE = FileTypes.FOFN

    def test_01(self):
        nrecords = 15
        name = "example_fofn"
        f = get_temp_file(name)
        _ = M.write_random_fofn(f, nrecords)
        ds_metadata = dispatch_metadata_resolver(self.FILE_TYPE, f)
        self.assertEquals(ds_metadata.nrecords, nrecords)
        os.remove(f)


class DatasetRegionFofnTest(DatasetFofnTest):
    FILE_TYPE = FileTypes.RGN_FOFN


class DatasetRegionMovieFofnTest(DatasetFofnTest):
    FILE_TYPE = FileTypes.MOVIE_FOFN
