###################
# FALCON TASKS
from pbcommand.cli import registry_builder, registry_runner
from pbcommand.models import (FileTypes, OutputFileType)
import logging
import sys

log = logging.getLogger(__name__)

TOOL_NAMESPACE = 'falcon_ns'
DRIVER_BASE = "python -m pbsmrtpipe.pb_tasks.tasks_falcon "

#from . import pbcommand_quick as pbquick
#registry = pbquick.registry_builder(TOOL_NAMESPACE, DRIVER_BASE)
registry = registry_builder(TOOL_NAMESPACE, DRIVER_BASE)

# FOFN = FileType(to_file_ns("generic_fofn"), "generic", "fofn", 'text/plain')
FC_FOFN = FileTypes.FOFN
FC_JSON = FileTypes.JSON
FC_CONFIG = FileTypes.TXT
FC_BASH = FileTypes.TXT
FC_DUMMY = FileTypes.TXT

def FT(file_type, basename):
    return OutputFileType(file_type.file_type_id,
                          "Label " + file_type.file_type_id,
                          repr(file_type),
                          "description for {f}".format(f=file_type),
                          basename)
RDJ = FT(FC_BASH, 'run_daligner_jobs.sh')

@registry('task_falcon_config', '0.0.0', [FC_CONFIG], [FC_JSON], is_distributed=False)
def run_rtc(rtc):
    from pbfalcon import tasks as pbfalcon
    return pbfalcon.run_falcon_config(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon_make_fofn_abs', '0.0.0', [FC_FOFN], [FC_FOFN], is_distributed=False)
def run_rtc(rtc):
    from pbfalcon import tasks as pbfalcon
    return pbfalcon.run_falcon_make_fofn_abs(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon0_build_rdb', '0.0.0', [FC_JSON, FC_FOFN], [RDJ, FT(FC_DUMMY, 'job.done')], is_distributed=False)
def run_rtc(rtc):
    from pbfalcon import tasks as pbfalcon
    return pbfalcon.run_falcon_build_rdb(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon0_run_daligner_jobs', '0.0.0', [FC_JSON, RDJ], [FC_FOFN], is_distributed=False, nproc=4)
def run_rtc(rtc):
    from pbfalcon import tasks as pbfalcon
    return pbfalcon.run_daligner_jobs(rtc.task.input_files, rtc.task.output_files, db_prefix='raw_reads')

@registry('task_falcon0_run_merge_consensus_jobs', '0.0.0', [FC_JSON, RDJ, FC_FOFN], [FC_FOFN], is_distributed=False, nproc=4)
def run_rtc(rtc):
    from pbfalcon import tasks as pbfalcon
    return pbfalcon.run_merge_consensus_jobs(rtc.task.input_files, rtc.task.output_files, db_prefix='raw_reads')

# Run similar steps for preads.
@registry('task_falcon1_build_pdb', '0.0.0', [FC_JSON, FC_FOFN], [RDJ], is_distributed=False)
def run_rtc(rtc):
    from pbfalcon import tasks as pbfalcon
    return pbfalcon.run_falcon_build_pdb(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon1_run_daligner_jobs', '0.0.0', [FC_JSON, RDJ], [FC_FOFN], is_distributed=False, nproc=4)
def run_rtc(rtc):
    from pbfalcon import tasks as pbfalcon
    return pbfalcon.run_daligner_jobs(rtc.task.input_files, rtc.task.output_files, db_prefix='preads')

@registry('task_falcon1_run_merge_consensus_jobs', '0.0.0', [FC_JSON, RDJ, FC_FOFN], [FC_FOFN], is_distributed=False, nproc=4)
def run_rtc(rtc):
    from pbfalcon import tasks as pbfalcon
    return pbfalcon.run_merge_consensus_jobs(rtc.task.input_files, rtc.task.output_files, db_prefix='preads')

@registry('task_falcon2_run_asm', '0.0.0', [FC_JSON, FC_FOFN], [FileTypes.FASTA], is_distributed=False, nproc=4)
def run_rtc(rtc):
    from pbfalcon import tasks as pbfalcon
    return pbfalcon.run_falcon_asm(rtc.task.input_files, rtc.task.output_files)


if __name__ == '__main__':
    sys.exit(registry_runner(registry, sys.argv[1:]))
