"""Dispatch Driver from running Static Tasks"""
import sys
import logging
from pbcommand.cli import pacbio_args_runner, get_default_argparser
from pbcommand.utils import setup_log

from pbsmrtpipe.cli_utils import main_runner_default, validate_file

import pbsmrtpipe.pb_io as IO
import pbsmrtpipe.tools.utils as U
from pbsmrtpipe.engine import backticks

log = logging.getLogger(__name__)
slog = logging.getLogger('status.' + __name__)

__version__ = '1.0'


def run_external_driver(driver_manifest, path):
    log.info("Running driver manifest {d}".format(d=driver_manifest))

    # print driver_manifest.task
    # print driver_manifest.driver

    cmd = "{e} {p}".format(e=driver_manifest.driver.driver_exe, p=path)
    log.info("Running command '{c}'".format(c=cmd))
    rcode, _, _, _ = backticks(cmd)
    return rcode


def _args_run_driver(args):
    # load manifest -> DriverManifest instance
    path = args.driver_manifest_json
    dm = IO.load_driver_manifest_from_file(path)
    return run_external_driver(dm, path)


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
