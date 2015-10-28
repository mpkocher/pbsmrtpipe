import logging

from pbsmrtpipe.core import register_pipeline
from pbsmrtpipe.constants import to_pipeline_ns

from .pb_pipelines_sa3 import Constants, Tags, _core_align, _core_gc

log = logging.getLogger(__name__)


def dev_register(relative_id, display_name, tags=()):
    pipeline_id = to_pipeline_ns(relative_id)
    ptags = list(set(tags + (Tags.DENOVO, )))
    return register_pipeline(pipeline_id, display_name, "0.1.0", tags=ptags)

def _get_falcon_pipeline(i_cfg, i_fasta_fofn):
    """Basic falcon pipeline components.
    """
    b0 = [
          (i_cfg,        'falcon_ns.tasks.task_falcon_config:0'),
          (i_fasta_fofn, 'falcon_ns.tasks.task_falcon_config:1'),
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

@dev_register("pipe_falcon_with_fofn", "Falcon FOFN Pipeline", tags=("local", "chunking"))
def get_task_falcon_local_pipeline():
    """Simple falcon local pipeline.
    Use an entry-point for FASTA input.
    """
    return _get_falcon_pipeline('$entry:e_01', '$entry:e_02')

@dev_register("pipe_falcon", "Falcon Pipeline", tags=("local", "chunking"))
def get_task_falcon_local_pipeline():
    """Simple falcon local pipeline.
    FASTA input comes from config file.
    """
    i_cfg = '$entry:e_01'
    init = [
          (i_cfg, 'falcon_ns.tasks.task_falcon_config_get_fasta:0'),
           ]
    i_fasta_fofn = 'falcon_ns.tasks.task_falcon_config_get_fasta:0' # output from init
    return init + _get_falcon_pipeline(i_cfg, i_fasta_fofn)

@dev_register("polished_falcon", "Polished Falcon Pipeline")
def get_task_falcon_local_pipeline():
    """Simple polished falcon local pipeline.
    FASTA input comes from the SubreadSet.
    """
    i_cfg = '$entry:e_01'
    subreadset = Constants.ENTRY_DS_SUBREAD

    btf = [(subreadset, 'pbsmrtpipe.tasks.bam2fasta:0')]
    ftfofn = [('pbsmrtpipe.tasks.bam2fasta:0', 'pbsmrtpipe.tasks.fasta2fofn:0')]

    i_fasta_fofn = 'pbsmrtpipe.tasks.fasta2fofn:0'

    falcon = _get_falcon_pipeline(i_cfg, i_fasta_fofn)

    ref = 'falcon_ns.tasks.task_falcon2_run_asm:0'

    faidx = [(ref, 'pbsmrtpipe.tasks.fasta2referenceset:0')]

    ref = 'pbsmrtpipe.tasks.fasta2referenceset:0'

    polish = _core_align(subreadset, ref) + _core_gc('pbalign.tasks.pbalign:0',
                                                     ref)

    return btf + ftfofn + falcon + faidx + polish


@dev_register("polished_falcon_lean", "Assembly (HGAP 4)")
def get_task_falcon_local_pipeline():
    """Simple lean polished falcon local pipeline.
    FASTA input comes from the SubreadSet.
    Cfg input is built from preset.xml
    """
    subreadset = Constants.ENTRY_DS_SUBREAD

    btf = [(subreadset, 'pbsmrtpipe.tasks.bam2fasta:0')]
    ftfofn = [('pbsmrtpipe.tasks.bam2fasta:0', 'pbsmrtpipe.tasks.fasta2fofn:0')]

    i_fasta_fofn = 'pbsmrtpipe.tasks.fasta2fofn:0'

    gen_cfg = [(i_fasta_fofn, 'falcon_ns.tasks.task_falcon_get_config:0')]

    i_cfg = 'falcon_ns.tasks.task_falcon_get_config:0'

    falcon = _get_falcon_pipeline(i_cfg, i_fasta_fofn)

    ref = 'falcon_ns.tasks.task_falcon2_run_asm:0'

    faidx = [(ref, 'pbsmrtpipe.tasks.fasta2referenceset:0')]

    ref = 'pbsmrtpipe.tasks.fasta2referenceset:0'

    polish = _core_align(subreadset, ref) + _core_gc('pbalign.tasks.pbalign:0',
                                                     ref)

    return btf + ftfofn + gen_cfg + falcon + faidx + polish
