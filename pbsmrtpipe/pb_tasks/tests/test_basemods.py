import logging

from task_test_base import _TaskTestBase
import pbsmrtpipe.schema_opt_utils as OP

log = logging.getLogger(__name__)


class TestComputeModificationsDefaults(_TaskTestBase):
    TASK_ID = 'pbsmrtpipe.tasks.compute_modifications'

    INPUT_FILE_NAMES = ('reference_dir_id/sequence/reference.fasta', 'report.json', 'my.cmp.h5', 'contig_id.fofn')


class TestComputeModifications(TestComputeModificationsDefaults):
    TASK_OPTIONS = {OP.to_opt_id('basemods.max_length'): 10,
                    OP.to_opt_id('basemods.identify_modifications'): True,
                    OP.to_opt_id('basemods.tet_treaded'): True}

    RESOLVED_TASK_OPTIONS = {OP.to_opt_id('basemods.max_length'): 10,
                             OP.to_opt_id('basemods.identify_modifications'): True,
                             OP.to_opt_id("basemods.tet_treaded"): True}


class TestModificationsReport(_TaskTestBase):
    TASK_ID = "pbsmrtpipe.tasks.modification_report"
    INPUT_FILE_NAMES = ['modifications.csv']

    NCOMMANDS = 2


class TestFindMotifs(_TaskTestBase):
    TASK_ID = 'pbsmrtpipe.tasks.find_motifs'
    INPUT_FILE_NAMES = ['modifications.gff', 'reference_dir_id/sequence/reference.fasta']