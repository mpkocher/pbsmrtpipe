"""Native python Static Driver Proof of concept for testing purposes"""
import os
import sys
import logging
import json
import functools

from pbsmrtpipe.cli_utils import main_runner_default, validate_file

import pbsmrtpipe.pb_io as IO
import pbsmrtpipe.tools.utils as U

log = logging.getLogger(__name__)
slog = logging.getLogger('status.' + __name__)

__version__ = '1.0'


def __write_mock_file(s, p):
    with open(p, 'w') as f:
        f.write(s)
    log.debug("Wrote mock file {p}".format(p=p))


def _to_json():
    d = {"key": "value", "_comment": "MOCK DATA"}
    return json.dumps(d)

_write_mock_file = functools.partial(__write_mock_file, "MOCK\n")
_write_mock_json_file = functools.partial(__write_mock_file, _to_json())


def write_mock_file(p):
    if not os.path.exists(p):
        if p.endswith(".json"):
            _write_mock_json_file(p)
        else:
            _write_mock_file(p)


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

    # write mock files
    for path in dm.task['output_files']:
        write_mock_file(path)

    return 0


def _args_run_driver(args):
    path = args.driver_manifest_json

    dm = IO.load_driver_manifest_from_file(path)

    return run_driver(dm)


def get_parser():
    p = U.get_base_pacbio_parser(__version__, "Dispatch Driver for running Static Tasks")
    U.add_debug_option(p)
    p.add_argument("driver_manifest_json", type=validate_file, help="Path to driver-manifest.json")
    return p


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_parser()
    parser.set_defaults(func=_args_run_driver)

    return main_runner_default(argv_[1:], parser, log)


if __name__ == '__main__':
    sys.exit(main())