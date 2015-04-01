import logging

from pbsmrtpipe.models import TaskTypes

from task_test_base import _TaskTestBase
import pbsmrtpipe.schema_opt_utils as OP

log = logging.getLogger(__name__)


class TestLabelZmwsTaskDefault(_TaskTestBase):
    TASK_ID = "pbsmrtpipe.tasks.label_zmws"
    INPUT_FILE_NAMES = ["movie.fofn", "barcodes.fasta"]
    TASK_OPTIONS = {OP.to_opt_id("barcode.score_mode"): "symmetric", OP.to_opt_id("barcode.adapter_side_pad"): 5}

    MAX_NPROC = 24

    NCOMMANDS = 1
    RESOLVED_NPROC = 24
    RESOLVED_TASK_OPTIONS = {}
    RESOLVED_TASK_TYPE = TaskTypes.DISTRIBUTED


class TestLabelZmwsTaskCustomOpts(TestLabelZmwsTaskDefault):
    TASK_OPTIONS = {OP.to_opt_id("barcode.score_mode"): "symmetric", OP.to_opt_id("barcode.adapter_side_pad"): 5}
    RESOLVED_TASK_OPTIONS = {OP.to_opt_id("barcode.score_mode"): "symmetric", OP.to_opt_id("barcode.adapter_side_pad"): 5}


class TestEmitFastqTask(_TaskTestBase):
    TASK_ID = "pbsmrtpipe.tasks.generate_barcode_fastqs"
    INPUT_FILE_NAMES = ["movie.fofn", "barcode.fofn"]
    TASK_OPTIONS = {}

    MAX_NPROC = 24

    NCOMMANDS = 4
    RESOLVED_NPROC = 1
    RESOLVED_TASK_OPTIONS = {}
    RESOLVED_TASK_TYPE = TaskTypes.DISTRIBUTED
