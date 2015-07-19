import logging
import argparse
import os
import pprint
import random
import sys
import time
import unittest

from pbcommand.cli import pacbio_args_runner, get_default_argparser
# the pbcommand version raise OSError for some reason
from pbsmrtpipe.cli_utils import validate_file

from pbsmrtpipe.engine import run_command, run_command_async
from pbsmrtpipe.cli import (add_log_file_options, add_log_level_option,
                            LOG_LEVELS)
from pbsmrtpipe.constants import SLOG_PREFIX
from pbsmrtpipe.utils import compose
import pbsmrtpipe.testkit.butler as B
import pbsmrtpipe.testkit.loader as L
import pbsmrtpipe.testkit.xunit as X
import pbsmrtpipe.tools.utils as TU

from pbsmrtpipe.utils import setup_log, StdOutStatusLogFilter

log = logging.getLogger()
slog = logging.getLogger(SLOG_PREFIX + __name__)

__version__ = '0.3.0'


def _patch_test_cases_with_job_dir(test_cases, job_dir):
    """This is dirrrttay. This must be called before the test cases are run"""
    for test_case in test_cases:
        test_case.__class__.job_dir = job_dir
        for t in test_case:
            t.__class__.job_dir = job_dir
            # log.debug("Setting job dir on {c}".format(c=t))


def _write_xunit_output(test_cases, result, output_xml, job_id):
    xml = X.convert_suite_and_result_to_xunit(test_cases, result)

    log.info("Writing Xunit XML output to {f}".format(f=output_xml))
    with open(output_xml, 'w+') as f:
        f.write(str(xml))

    # give the user some feedback that tests were run.
    xsuite = X.XunitTestSuite.from_xml(output_xml)
    slog.info(str(xsuite))
    slog.info(xsuite)
    nfailed_tests = xsuite.nfailure

    jenkins_xml = X.xunit_file_to_jenkins(output_xml, job_name=job_id)

    output_dir = os.path.dirname(output_xml)
    jenkins_name = "_".join(['jenkins', os.path.basename(output_xml)])
    jenkins_xml_file = os.path.join(output_dir, jenkins_name)

    log.info("Writing Jenkins XML output to {f}".format(f=jenkins_xml_file))
    with open(jenkins_xml_file, 'w') as f:
        f.write(str(jenkins_xml))

    log.info("Completed running {t} tests. {n} failed tests.".format(n=nfailed_tests, t=len(xsuite)))


def run_butler_tests(test_cases, output_dir, output_xml, job_id):
    """

    :return:
    """

    # This is really hacky
    _patch_test_cases_with_job_dir(test_cases, output_dir)

    # This is the API to run directly from Unittest
    slog.info("Running test cases")
    slog.info(test_cases)
    result = unittest.TestResult()
    test_suite = unittest.TestSuite(test_cases)
    test_suite.run(result)
    log.debug(result)

    _write_xunit_output(test_cases, result, output_xml, job_id)

    return 0 if result.wasSuccessful() else 1


def run_butler(butler, test_cases, output_xml, log_file=None, log_level=logging.DEBUG, force_distribute=False):
    """
    Run a Butler instance.

    :param butler: Butler instance
    :return: exit code


    :rtype: int
    """
    started_at = time.time()

    if force_distribute:
        butler.force_distribute = force_distribute

    cmd = butler.to_cmd()

    if not os.path.exists(butler.output_dir):
        os.mkdir(butler.output_dir)

    b_log = os.path.join(butler.output_dir, 'butler.log')
    # b_err = os.path.join(butler.output_dir, 'butler.stderr')
    b_out = os.path.join(butler.output_dir, 'butler.stdout')

    setup_log(log, level=log_level, file_name=b_log)

    # Butler stdout log
    str_formatter = '[%(levelname)s] %(message)s'
    setup_log(slog, level=log_level,
              file_name=b_out,
              log_filter=StdOutStatusLogFilter(),
              str_formatter=str_formatter)

    if log_file is not None:
        if isinstance(log_level, str):
            log_levels_d = {attr: getattr(logging, attr) for attr in LOG_LEVELS}
            level = log_levels_d[log_level]
        else:
            level = logging.DEBUG
        setup_log(log, level=level, file_name=log_file)

    # log.debug(pprint.pformat(butler.__dict__))
    slog.info("Running butler with id {i}".format(i=butler.job_id))
    slog.info("Running cmd '{c}'".format(c=cmd))

    def _to_p(file_name):
        return os.path.join(butler.output_dir, file_name)

    # pbsmrtpipe stdout and stderr
    stdout = _to_p('job.stdout')
    stderr = _to_p('job.stderr')

    with open(stdout, 'w') as stdout_fh:
        with open(stderr, 'w') as stderr_fh:

            rcode, stdout, stderr, run_time = run_command_async(cmd, stdout_fh, stderr_fh)

            rmessage = "was successful" if rcode == 0 else " failed"
            msg = "pbsmrtpipe command {m} ({s:.2f} sec) exit code {e}.'".format(e=rcode, s=run_time, m=rmessage)
            if rcode != 0:
                slog.error(msg)
                log.error(str(stderr))
            else:
                slog.info(msg)

        if test_cases is not None:
            trcode = run_butler_tests(test_cases, butler.output_dir, output_xml, butler.job_id)
        else:
            trcode = 0

    run_time = time.time() - started_at
    slog.info("Exiting testkit runner in {s:.2f} sec.".format(s=run_time))

    # Was butler successful + all the tests pass
    return rcode | trcode


def _args_run_butler(args):
    butler = B.config_parser_to_butler(args.testkit_cfg)

    test_cases = L.parse_cfg_file(args.testkit_cfg)

    if not os.path.exists(butler.output_dir):
        os.mkdir(butler.output_dir)

    output_xml = os.path.join(butler.output_dir, 'testkit_xunit.xml')

    if args.only_tests:
        return run_butler_tests(test_cases, butler.output_dir, output_xml, butler.job_id)
    else:
        rcode = run_butler(butler, test_cases, output_xml, log_file=args.log_file, log_level=args.log_level, force_distribute=args.force_distribute)
        return rcode


def add_tests_only_option(p):
    p.add_argument('--only-tests', action='store_true', help="Only run the tests.")
    return p


def _add_config_file_option(p):
    p.add_argument('testkit_cfg', type=validate_file, help="Path to testkit.cfg file.")
    return p


def get_parser():
    desc = "Testkit Tool to run pbsmrtpipe jobs."
    p = get_default_argparser(__version__, desc)

    funcs = [TU.add_debug_option,
             _add_config_file_option,
             add_tests_only_option,
             add_log_level_option,
             add_log_file_options,
             TU.add_force_distribute_option]

    f = compose(*funcs)
    p = f(p)

    return p


def main(argv=sys.argv):
    parser = get_parser()
    return pacbio_args_runner(argv[1:], parser, _args_run_butler, log, setup_log)



