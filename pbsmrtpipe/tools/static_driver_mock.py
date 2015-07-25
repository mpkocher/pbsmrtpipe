"""Native python Static Driver Proof of concept for testing purposes"""
import os
import sys
import logging

from pbcommand.cli import pacbio_args_runner, get_default_argparser
from pbcommand.utils import setup_log
from pbcommand.pb_io import load_resolved_tool_contract_from

from pbsmrtpipe.cli_utils import validate_file

import pbsmrtpipe.tools.utils as U
import pbsmrtpipe.mock as M

log = logging.getLogger(__name__)
slog = logging.getLogger('status.' + __name__)

__version__ = '1.0'


def run_driver(rtc):
    """
    :type rtc: pbcommand.models.ResolvedToolContract
    :param rtc:
    :return: Int return code
    """
    log.info("Python Running MOCK driver {x}".format(x=rtc))
    for i, path in enumerate(rtc.task.input_files):
        if not os.path.exists(path):
            log.warn("Unable to find input {i} {p}".format(i=i, p=path))

    nrecords = 10
    # write mock files
    for path in rtc.task.output_files:
        M.write_mock_file_by_type(path, nrecords)

    return 0


def _args_run_driver(args):
    path = args.resolved_tool_contract
    rtc = load_resolved_tool_contract_from(path)
    return run_driver(rtc)


def get_parser():
    p = get_default_argparser(__version__, "MOCK Dispatch Driver for running Running Resolved Tool contract")
    U.add_debug_option(p)
    p.add_argument("resolved_tool_contract", type=validate_file, help="Path to resolved-tool-contract.json")
    return p


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_parser()
    return pacbio_args_runner(argv_[1:], parser, _args_run_driver, log, setup_log)


if __name__ == '__main__':
    sys.exit(main())
