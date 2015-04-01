import unittest
import logging

from pbsmrtpipe.core import MetaTaskBase
from pbsmrtpipe.models import FileTypes, TaskTypes, SymbolTypes, ResourceTypes

log = logging.getLogger(__name__)


class _MyTask(MetaTaskBase):
    """My Task Does A, then B and is awesome"""
    TASK_ID = "pbsmrtpipe.tasks.my_task_01"
    NAME = "My Awesome Task Display Name"
    VERSION = "1.2.4"

    TASK_TYPE = TaskTypes.LOCAL

    # input types are given my a tuple of (File Type, binding label, description)
    INPUT_TYPES = ((FileTypes.FASTA, "x_fasta", "A Description of the Fasta"),
                   (FileTypes.CSV, "a_csv", "A description of the CSV file."),
                   (FileTypes.BAM, "a_bam", "An awesome BAM that must have Metrics X, Y, Z"))
    OUTPUT_TYPES = ((FileTypes.FOFN, "a_fofn", "A Fofn of Bams"),)
    OUTPUT_FILE_NAMES = (("my_file", "fofn"),)
    SCHEMA_OPTIONS = {}
    NPROC = SymbolTypes.MAX_NPROC
    RESOURCE_TYPES = (ResourceTypes.TMP_FILE, ResourceTypes.LOG_FILE)
    # Could be extended to have project or tool dependencies.
    # DEPENDENCIES = (("pbcore", "0.8.9", ("pbalign", "0.3.8"))

    @staticmethod
    def to_cmd(input_files, output_files, opts, nproc, resources):
        log.info("Task {i}".format(i=_MyTask.TASK_ID))
        log.info(input_files)
        log.info(output_files)
        log.info(opts)
        log.info(nproc)
        log.info(resources)
        return "Do stuff"


class MyTest(unittest.TestCase):
    def test_sanity(self):
        log.info(_MyTask)
        self.assertTrue(True)