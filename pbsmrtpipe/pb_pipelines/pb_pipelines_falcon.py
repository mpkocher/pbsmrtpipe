import logging

from pbsmrtpipe.core import register_pipeline
from pbsmrtpipe.constants import to_pipeline_ns

from .pb_pipelines_sa3 import Constants

log = logging.getLogger(__name__)


def dev_register(relative_id, display_name, tags=()):
    pipeline_id = to_pipeline_ns(relative_id)
    ptags = list(set(tags + ('dev', )))
    return register_pipeline(pipeline_id, display_name, "0.1.0", tags=ptags)

@dev_register("pipe_falcon", "Falcon Pipeline", tags=("local", "chunking"))
def get_task_falcon_local_pipeline():
    """Simple example pipeline"""
    b0 = [
          ('$entry:e_01', 'falcon_ns.tasks.task_falcon_config:0'),
          ('falcon_ns.tasks.task_falcon_config:0', 'falcon_ns.tasks.task_falcon_make_fofn_abs:0'),
    ]
    br0 = [
          ('falcon_ns.tasks.task_falcon_config:0',        'falcon_ns.tasks.task_falcon0_build_rdb:0'),
          ('falcon_ns.tasks.task_falcon_make_fofn_abs:0', 'falcon_ns.tasks.task_falcon0_build_rdb:1'),
         ]

    br1 = [
          ('falcon_ns.tasks.task_falcon_config:0',     'falcon_ns.tasks.task_falcon0_run_daligner_jobs:0'),
          ('falcon_ns.tasks.task_falcon0_build_rdb:0', 'falcon_ns.tasks.task_falcon0_run_daligner_jobs:1'),
         ]
    br2 = [
          ('falcon_ns.tasks.task_falcon_config:0',             'falcon_ns.tasks.task_falcon0_run_merge_consensus_jobs:0'),
          ('falcon_ns.tasks.task_falcon0_build_rdb:0',         'falcon_ns.tasks.task_falcon0_run_merge_consensus_jobs:1'),
          ('falcon_ns.tasks.task_falcon0_run_daligner_jobs:0', 'falcon_ns.tasks.task_falcon0_run_merge_consensus_jobs:2'),
         ]
    bp0 = [
          ('falcon_ns.tasks.task_falcon_config:0',                    'falcon_ns.tasks.task_falcon1_build_pdb:0'),
          ('falcon_ns.tasks.task_falcon0_run_merge_consensus_jobs:0', 'falcon_ns.tasks.task_falcon1_build_pdb:1'),
         ]
    bp1 = [
          ('falcon_ns.tasks.task_falcon_config:0',     'falcon_ns.tasks.task_falcon1_run_daligner_jobs:0'),
          ('falcon_ns.tasks.task_falcon1_build_pdb:0', 'falcon_ns.tasks.task_falcon1_run_daligner_jobs:1'),
         ]
    bp2 = [
          ('falcon_ns.tasks.task_falcon_config:0',             'falcon_ns.tasks.task_falcon1_run_merge_consensus_jobs:0'),
          ('falcon_ns.tasks.task_falcon1_build_pdb:0',         'falcon_ns.tasks.task_falcon1_run_merge_consensus_jobs:1'),
          ('falcon_ns.tasks.task_falcon1_run_daligner_jobs:0', 'falcon_ns.tasks.task_falcon1_run_merge_consensus_jobs:2'),
         ]
    bf = [
            ('falcon_ns.tasks.task_falcon_config:0',                    'falcon_ns.tasks.task_falcon2_run_asm:0'),
            ('falcon_ns.tasks.task_falcon1_run_merge_consensus_jobs:0', 'falcon_ns.tasks.task_falcon2_run_asm:1'),
         ]
    return b0 + br0 + br1 + br2 + bp0 + bp1 + bp2 + bf
