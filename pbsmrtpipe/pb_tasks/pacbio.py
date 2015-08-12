
"""
Tool contract wrappers for miscellaneous quick functions.
"""

import logging
import shutil
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
    output_dir = os.path.dirname(output_file_name)
    args = [
        "bax2bam",
        "--subread",
        "-o", base_name,
        "--xml", input_file_name
    ]
    logging.warn(" ".join(args))
    result = run_cmd(" ".join(args),
                     stdout_fh=sys.stdout,
                     stderr_fh=sys.stderr)
    if result.exit_code != 0:
        return result.exit_code

    # FIXME bax2bam won't let us choose the output XML file name :(
    for file_name in os.listdir(output_dir):
        if file_name.endswith(".dataset.xml") and file_name != output_file_name:
            shutil.move(file_name, output_file_name)
    return 0


@registry("h5_subreads_to_subread", "0.1.0",
          FileTypes.DS_SUBREADS_H5,
          FileTypes.DS_SUBREADS, is_distributed=True, nproc=1)
def run_bax2bam(rtc):
    return run_bax_to_bam(rtc.task.input_files[0], rtc.task.output_files[0])


if __name__ == '__main__':
    sys.exit(registry_runner(registry, sys.argv[1:]))
