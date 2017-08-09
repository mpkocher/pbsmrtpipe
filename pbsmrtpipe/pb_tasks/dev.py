import logging
import os
import shutil
import time
import xml.dom.minidom

from pbcommand.cli import registry_builder, registry_runner
from pbcommand.pb_io.report import fofn_to_report
from pbcommand.models import FileTypes, TaskTypes, SymbolTypes, ResourceTypes
import sys

from pbcore.io import (readFofn, ReferenceSet, FastqReader, FastaWriter,
                       FastaRecord, FastaReader)
import random

from pbsmrtpipe.mock import write_random_fasta_records
import pbsmrtpipe.schema_opt_utils as OP
from pbsmrtpipe.tools.dev import (subread_dataset_report,
                                  run_random_fofn,
                                  run_reference_dataset_report,
                                  run_fasta_report)

# pylint: disable=E0102

log = logging.getLogger(__name__)

TOOL_NAMESPACE = 'pbsmrtpipe'
DRIVER_BASE = "python -m pbsmrtpipe.pb_tasks.dev "

registry = registry_builder(TOOL_NAMESPACE, DRIVER_BASE)


def _to_opt_id(name):
    return ".".join([TOOL_NAMESPACE, 'task_options', name])


def write_report_and_log(report, output_file):
    report.write_json(output_file)
    log.debug("Wrote report {r} to {o}".format(r=report, o=output_file))
    return True


def _fastq_to_fasta(fastq_path, fasta_path):
    """Convert a fastq file to  fasta file"""
    with FastqReader(fastq_path) as r:
        with FastaWriter(fasta_path) as w:
            for fastq_record in r:
                fasta_record = FastaRecord(fastq_record.name, fastq_record.sequence)
                w.writeRecord(fasta_record)

    log.info("Completed converting {q} to {f}".format(q=fastq_path, f=fasta_path))
    return 0


def run_fasta_filter(fasta_in, fasta_out, min_seq_length):
    with FastaWriter(fasta_out) as w:
        with FastaReader(fasta_in) as r:
            for record in r:
                if len(record.sequence) > min_seq_length:
                    w.writeRecord(record)

    return 0


def _get_opts():
    oid = OP.to_opt_id('dev.hello_message')
    return {oid: OP.to_option_schema(oid, "string", 'Hello Message', "Hello Message for dev Task.", "Default Message")}


def run_main_dev_hello_world(input_file, output_file):
    with open(input_file, 'r') as f:
        with open(output_file, 'w') as w:
            w.write(f.readline())
    return 0


@registry("dev_hello_world", '0.1.0', FileTypes.TXT, FileTypes.TXT, is_distributed=False, options=dict(dev_message="Hello Dev message"))
def run_rtc(rtc):
    return run_main_dev_hello_world(rtc.task.input_files[0], rtc.task.output_files[0])


@registry("dev_raises_exception", "0.1.0", FileTypes.TXT, FileTypes.TXT,
          is_distributed=False)
def run_rtc(rtc):
    """Task that deliberately raises an exception, for testing failure mode"""
    assert 0


@registry("dev_optional_failure", "0.1.0", FileTypes.DS_REF, FileTypes.TXT,
          is_distributed=False,
          options={"raise_exception":False})
def run_rtc_optional_failure(rtc):
    if rtc.task.options["pbsmrtpipe.task_options.raise_exception"]:
        raise ValueError("raise_exception=True, failing task!")
    with open(rtc.task.output_files[0], 'w') as w:
        w.write("Hello, world!")
    return 0


@registry("dev_optional_failure_subreads", "0.1.0", FileTypes.DS_SUBREADS,
          FileTypes.TXT,
          is_distributed=False,
          options={"raise_exception":False})
def run_rtc_optional_failure_subreads(rtc):
    return run_rtc_optional_failure(rtc)


dev_diagnostic_options = dict(
    dev_diagnostic_strict=False,
    test_str="asdf",
    test_int=1,
    test_float=3.14,
    test_choice_str=["A", "B", "C"],
    test_choice_int=[1, 2, 3],
    test_choice_float=[0.01, 0.1, 1.0])

@registry("dev_subread_report", "0.2.0", FileTypes.DS_SUBREADS, FileTypes.REPORT, is_distributed=False, options=dev_diagnostic_options)
def run_rtc(rtc):
    return subread_dataset_report(rtc.task.input_files[0], rtc.task.output_files[0])


@registry('dev_hello_worlder', '0.2.2', FileTypes.TXT, FileTypes.TXT, is_distributed=False, nproc=2,
          options=dict(dev_sleep_time_sec=0, test_msg=("alpha", "beta", "gamma")))
def run_rtc(rtc):
    # Add test_msg option to test choice support in integration/e2e tests
    ropt = rtc.task.options
    max_sleep_time = ropt[_to_opt_id('dev_sleep_time_sec')]
    test_msg = ropt[_to_opt_id('test_msg')]
    sleep_time = random.randint(0, max_sleep_time)
    # Longer running jobs
    log.info("Test message choice value '{m}'".format(m=test_msg))
    log.info("Sleeping for {} sec form max sleep time {}".format(sleep_time, max_sleep_time))
    time.sleep(sleep_time)
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


@registry('dev_hello_distributed', '0.1.0', (FileTypes.TXT, FileTypes.TXT), FileTypes.TXT, is_distributed=True, nproc=3)
def run_rtc(rtc):
    with open(rtc.task.output_files[0], 'w') as w:
        for input_file in rtc.task.input_files:
            with open(input_file, 'r') as f:
                w.write(f.readline())
    return 0


@registry('dev_reference_ds_report', '0.1.0', FileTypes.DS_REF, FileTypes.REPORT, is_distributed=False, nproc=3, options=dev_diagnostic_options)
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


@registry("dev_fastq_to_fasta", "0.1.0", FileTypes.FASTQ, FileTypes.FASTA, is_distributed=False)
def run_rtc(rtc):
    return _fastq_to_fasta(rtc.task.input_files[0], rtc.task.output_files[0])


@registry("dev_fasta", "0.2.1", FileTypes.TXT, FileTypes.FASTA, is_distributed=False,
          options=dict(max_nrecords=1000))
def run_rtc(rtc):
    max_records = rtc.tasks.options[_to_opt_id("alpha")]
    return write_random_fasta_records(rtc.task.output_files[0], max_records)


@registry("dev_tc_fasta_report", "0.1.1", FileTypes.FASTA, FileTypes.REPORT, is_distributed=False)
def run_rtc(rtc):
    return run_fasta_report(rtc.task.input_files[0], rtc.task.output_files[0])


@registry("dev_filter_fasta", "0.1.1", FileTypes.FASTA, FileTypes.FASTA, is_distributed=False,
          options=dict(dev_fasta_min_length=50))
def run_rtc(rtc):
    min_seq_length = rtc.task.options[_to_opt_id("dev_fasta_min_length")]
    # this is for testing failures
    if min_seq_length < 0:
        raise ValueError("Invalid min seq length {m}".format(m=min_seq_length))
    return run_fasta_filter(rtc.task.input_files[0], rtc.task.output_files[0], min_seq_length)


@registry("rset_to_txt", "0.1.0", FileTypes.DS_REF, FileTypes.TXT, is_distributed=False)
def run_rtc(rtc):
    """Dev Task for testing pipelines. Generates a Txt file"""

    with open(rtc.task.output_files[0], 'w') as f:
        f.write("Dev Mock converting ReferenceSet {}\n".format(rtc.task.input_files[0]))
        f.write("to text file {}".format(rtc.task.output_files[0]))

    return 0


@registry("dev_verify_chemistry", "0.1.0", FileTypes.DS_SUBREADS, FileTypes.TXT, is_distributed=False, options=dict(chemistry_version="unknown"))
def run_chem_bundle_check(rtc):
    """Dev task for verifying chemistry bundle propagation"""
    CHEM_DIR = os.environ["SMRT_CHEMISTRY_BUNDLE_DIR"]
    manifest = os.path.join(CHEM_DIR, "manifest.xml")
    dom = xml.dom.minidom.parse(manifest)
    version = dom.getElementsByTagName("Version")[0].lastChild.data
    expected = rtc.task.options["pbsmrtpipe.task_options.chemistry_version"]
    msg = "Expected version {e}, got {v}".format(e=expected, v=version)
    assert version == expected, msg
    with open(rtc.task.output_files[0], 'w') as f:
        f.write("SMRT_CHEMISTRY_BUNDLE_DIR={f}".format(f=rtc.task.input_files[0]))
        f.write("VERSION={v}".format(v=version))
    return 0


if __name__ == '__main__':
    sys.exit(registry_runner(registry, sys.argv[1:]))
