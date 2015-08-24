
"""
Tool contract wrappers for miscellaneous quick functions.
"""

import functools
import logging
import shutil
import gzip
import os
import sys

from pbcommand.engine import run_cmd
from pbcommand.cli import registry_builder, registry_runner
from pbcommand.models import FileTypes

log = logging.getLogger(__name__)

TOOL_NAMESPACE = 'pbsmrtpipe'
DRIVER_BASE = "python -m pbsmrtpipe.pb_tasks.pacbio "

registry = registry_builder(TOOL_NAMESPACE, DRIVER_BASE)


def run_bax_to_bam(input_file_name, output_file_name):
    base_name = os.path.splitext(output_file_name)[0]
    args = [
        "bax2bam",
        "--subread",
        "-o", base_name,
        "--output-xml", output_file_name,
        "--xml", input_file_name
    ]
    logging.info(" ".join(args))
    result = run_cmd(" ".join(args),
                     stdout_fh=sys.stdout,
                     stderr_fh=sys.stderr)
    return result.exit_code


def run_bam_to_fastx(program_name, input_file_name, output_file_name):
    def _splitext(path):
        base, ext = os.path.splitext(path)
        if ext == ".gz":
            base, ext2 = os.path.splitext(base)
            ext = ext2 + ext
        return base, ext
    args = [
        program_name,
        "-o", _splitext(output_file_name)[0],
        input_file_name,
    ]
    logging.info(" ".join(args))
    result = run_cmd(" ".join(args),
                     stdout_fh=sys.stdout,
                     stderr_fh=sys.stderr)
    if result.exit_code != 0:
        return result.exit_code
    else:
        if not output_file_name.endswith(".gz"):
            output_file_name_ = output_file_name + ".gz"
            with gzip.open(output_file_name_) as f_in:
                with open(output_file_name, "w") as f_out:
                    f_out.write(f_in.read())
    return 0


run_bam_to_fasta = functools.partial(run_bam_to_fastx, "bam2fasta")
run_bam_to_fastq = functools.partial(run_bam_to_fastx, "bam2fastq")


@registry("h5_subreads_to_subread", "0.1.0",
          FileTypes.DS_SUBREADS_H5,
          FileTypes.DS_SUBREADS, is_distributed=True, nproc=1)
def run_bax2bam(rtc):
    return run_bax_to_bam(rtc.task.input_files[0], rtc.task.output_files[0])


@registry("bam2fastq", "0.1.0",
          FileTypes.DS_SUBREADS,
          FileTypes.FASTQ, is_distributed=True, nproc=1)
def run_bam2fastq(rtc):
    return run_bam_to_fastq(rtc.task.input_files[0], rtc.task.output_files[0])


@registry("bam2fasta", "0.1.0",
          FileTypes.DS_SUBREADS,
          FileTypes.FASTA, is_distributed=True, nproc=1)
def run_bam2fasta(rtc):
    return run_bam_to_fasta(rtc.task.input_files[0], rtc.task.output_files[0])


if __name__ == '__main__':
    sys.exit(registry_runner(registry, sys.argv[1:]))
