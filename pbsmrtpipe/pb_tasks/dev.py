import logging
import os
import shutil

from pbcommand.cli import registry_builder, registry_runner
from pbcommand.models import FileTypes, TaskTypes, SymbolTypes, ResourceTypes
import sys

from pbcore.io import readFofn, ReferenceSet

from pbsmrtpipe.legacy.input_xml import fofn_to_report
from pbsmrtpipe.mock import write_random_fasta_records
import pbsmrtpipe.schema_opt_utils as OP
from pbsmrtpipe.tools.converter import write_report_and_log
from pbsmrtpipe.tools.dev import (subread_dataset_report,
                                  run_random_fofn,
                                  run_fasta_filter,
                                  run_reference_dataset_report)

log = logging.getLogger(__name__)

TOOL_NAMESPACE = 'pbsmrtpipe'
DRIVER_BASE = "python -m pbsmrtpipe.pb_tasks.dev "

registry = registry_builder(TOOL_NAMESPACE, DRIVER_BASE)


def _get_opts():
    oid = OP.to_opt_id('dev.hello_message')
    return {oid: OP.to_option_schema(oid, "string", 'Hello Message', "Hello Message for dev Task.", "Default Message")}


def run_main_dev_hello_world(input_file, output_file):
    with open(input_file, 'r') as f:
        with open(output_file, 'w') as w:
            w.write(f.readline())
    return 0


@registry("dev_hello_world", '0.1.0', FileTypes.TXT, FileTypes.TXT, is_distributed=False)
def run_rtc(rtc):
    return run_main_dev_hello_world(rtc.task.input_files[0], rtc.task.output_files[0])


@registry("dev_subread_report", "0.1.0", FileTypes.DS_SUBREADS, FileTypes.REPORT, is_distributed=False)
def run_rtc(rtc):
    return subread_dataset_report(rtc.task.input_files[0], rtc.task.output_files[0])


@registry('dev_hello_worlder', '0.2.1', FileTypes.TXT, FileTypes.TXT, is_distributed=False, nproc=2)
def run_rtc(rtc):
    return run_main_dev_hello_world(rtc.task.input_files[0], rtc.task.output_files[0])


@registry('dev_hello_garfield', '0.2.1', FileTypes.TXT, (FileTypes.TXT, FileTypes.TXT), is_distributed=False, nproc=3)
def run_rtc(rtc):
    for output_file in rtc.task.output_files:
        run_main_dev_hello_world(rtc.task.input_files[0], output_file)
    return 0


@registry('dev_hello_lasagna', '0.1.0', FileTypes.TXT, FileTypes.TXT, is_distributed=False, nproc=SymbolTypes.MAX_NPROC)
def run_rtc(rtc):
    return run_main_dev_hello_world(rtc.task.input_files[0], rtc.task.output_files[0])


@registry('dev_txt_to_fofn', '1.0.0', FileTypes.TXT, FileTypes.FOFN, is_distributed=False, nproc=1)
def run_rtc(rtc):
    out = rtc.task.output_files[0]
    output_dir = os.path.dirname(out)
    return run_random_fofn(out, output_dir, 10)


@registry('dev_txt_to_fofn_report', '0.1.0', FileTypes.FOFN, (FileTypes.FOFN, FileTypes.REPORT), is_distributed=False)
def run_rtc(rtc):
    files = list(readFofn(rtc.task.input_files[0]))
    report = fofn_to_report(len(files))
    shutil.copy(rtc.task.input_files[0], rtc.task.output_files[0])
    write_report_and_log(report, rtc.task.output_files[1])
    log.info("Completed running {i}".format(i=rtc.task))
    return 0


@registry('dev_fofn_example', '0.1.0', (FileTypes.FOFN, FileTypes.REPORT), FileTypes.TXT, is_distributed=False, nproc=2)
def run_rtc(rtc):
    return run_main_dev_hello_world(rtc.task.input_files[0], rtc.task.output_files[0])


@registry('dev_txt_to_fasta', '0.1.0', FileTypes.TXT, FileTypes.FASTA, is_distributed=False)
def run_rtc(rtc):
    nrecords = 1000
    write_random_fasta_records(rtc.task.output_files[0], nrecords)
    return 0


@registry('dev_py_filter_fasta', '0.1.0', FileTypes.FASTA, FileTypes.FASTA, is_distributed=False)
def run_rtc(rtc):
    min_length = 25
    return run_fasta_filter(rtc.task.input_files[0], rtc.task.output_files[0], min_length)


@registry('dev_hello_distributed', '0.1.0', (FileTypes.TXT, FileTypes.TXT), FileTypes.TXT, is_distributed=False, nproc=3)
def run_rtc(rtc):
    with open(rtc.task.output_files[0], 'w') as w:
        for input_file in rtc.task.input_files:
            with open(input_file, 'r') as f:
                w.write(f.readline())
    return 0


@registry('dev_reference_ds_report', '0.1.0', FileTypes.DS_REF, FileTypes.REPORT, is_distributed=False, nproc=3)
def run_rtc(rtc):
    reference_ds = ReferenceSet(rtc.task.input_files[0])
    return run_reference_dataset_report(reference_ds, rtc.task.output_files[0])


def _get_simple_opts():
    # Util func to create an task option id 'pbsmrtpipe.task_options.dev.hello_message'
    oid = OP.to_opt_id('dev.hello_message')
    return {oid: OP.to_option_schema(oid, "string", 'Hello Message', "Hello Message for dev Task.", "Default Message")}


@registry('dev_simple_hello_world', '0.1.0', FileTypes.TXT, FileTypes.TXT, is_distributed=False)
def run_rtc(rtc):
    return run_main_dev_hello_world(rtc.task.input_files[0], rtc.task.output_files[0])


if __name__ == '__main__':
    sys.exit(registry_runner(registry, sys.argv[1:]))
