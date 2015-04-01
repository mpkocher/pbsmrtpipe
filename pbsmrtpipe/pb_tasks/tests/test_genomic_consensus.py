import logging

from pbsmrtpipe.models import TaskTypes
from task_test_base import _TaskTestBase
from pbsmrtpipe.schema_opt_utils import to_opt_id

log = logging.getLogger(__name__)


def _to_i(s):
    return to_opt_id("consensus.{s}".format(s=s))


class TestCallVariantsWithFastxDefaults(_TaskTestBase):
    TASK_ID = "pbsmrtpipe.tasks.call_variants_with_fastx"
    INPUT_FILE_NAMES = ["file.fasta", "file.cmp.h5", 'contig_ids.fofn']
    TASK_OPTIONS = {}

    MAX_NPROC = 24

    NCOMMANDS = 1
    RESOLVED_NPROC = 24
    RESOLVED_TASK_OPTIONS = {_to_i("diploid_mode"): False, _to_i("algorithm"): "quiver"}
    RESOLVED_TASK_TYPE = TaskTypes.DISTRIBUTED


class TestCallVariantsWithFastxWithOpts(TestCallVariantsWithFastxDefaults):
    TASK_OPTIONS = {_to_i("diploid_mode"): True, _to_i("algorithm"): "quiver", _to_i('min_coverage'): 5}

    NCOMMANDS = 1
    RESOLVED_NPROC = 24
    RESOLVED_TASK_OPTIONS = {_to_i("diploid_mode"): True, _to_i("algorithm"): "quiver", _to_i('min_coverage'): 5}
    RESOLVED_TASK_TYPE = TaskTypes.DISTRIBUTED


class TestSummarizeConsensus(_TaskTestBase):
    TASK_ID = 'pbsmrtpipe.tasks.enrich_summarize_consensus'
    INPUT_FILE_NAMES = ['alignments.gff', 'variants.gff']


class TestTopVariantsReport(_TaskTestBase):
    TASK_ID = 'pbsmrtpipe.tasks.top_variants_report'
    INPUT_FILE_NAMES = ['lambda.fasta', 'variants.gff.gz']


class TestVariantsReport(_TaskTestBase):
    TASK_ID = 'pbsmrtpipe.tasks.variants_report'
    INPUT_FILE_NAMES = ['lambda.fasta', 'alignment.gff', 'variants.gff.gz']
