"""Native python Static Driver Proof of concept for testing purposes"""
import os
import sys
import logging
import json
import functools
from pbcommand.cli import pacbio_args_runner, get_default_argparser
from pbcommand.utils import setup_log

from pbsmrtpipe.cli_utils import main_runner_default, validate_file

import pbsmrtpipe.pb_io as IO
import pbsmrtpipe.tools.utils as U
import pbsmrtpipe.mock as M

log = logging.getLogger(__name__)
slog = logging.getLogger('status.' + __name__)

__version__ = '1.0'


def run_driver(dm):
    """
    :type dm: pbsmrtpipe.models.DriverManifest
    :param dm:
    :return:
    """
    log.info("Python Running MOCK driver {x}".format(x=dm))
    for i, path in enumerate(dm.task['input_files']):
        if not os.path.exists(path):
            log.warn("Unable to find input {i} {p}".format(i=i, p=path))

    nrecords = 10
    # write mock files
    for path in dm.task['output_files']:
        M.write_mock_file_by_type(path, nrecords)

    return 0


def _args_run_driver(args):
    path = args.driver_manifest_json

    dm = IO.load_driver_manifest_from_file(path)

    return run_driver(dm)


def get_parser():
    p = get_default_argparser(__version__, "Dispatch Driver for running Static Tasks")
    U.add_debug_option(p)
    p.add_argument("driver_manifest_json", type=validate_file, help="Path to driver-manifest.json")
    return p


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_parser()
    return pacbio_args_runner(argv_[1:], parser, _args_run_driver, log, setup_log)


if __name__ == '__main__':
    sys.exit(main())