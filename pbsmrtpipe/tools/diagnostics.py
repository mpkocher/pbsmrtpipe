"""CLI tool for helping getting metrics about the current configuration and setup"""

import sys
import logging
import argparse

import pbsmrtpipe

from pbsmrtpipe.cli_utils import main_runner_default, validate_file
import pbsmrtpipe.tools.utils as U

log = logging.getLogger(__name__)


def _add_preset_xml_option(p):
    p.add_argument('--preset', type=validate_file, help="Path to Preset XML file.")
    return p


def _add_full_option(p):
    # Run the Full diagnostics suite
    p.add_argument('--full', action='store_true', help="Perform full diagnostics tests (e.g., submit test job to cluster).")
    return p


def _test_valid_cluster_templates():
    """Can load installed cluster templates"""
    return True


def _test_valid_cluster_submission():
    """Submit hello world to cluster configuration"""
    return True


def run_full_diagnostics(rc_preset, preset):

    return True


def run_diagnostics(rc_preset, preset):
    """

    :param rc_preset:
    :param preset:
    :return:
    """

    return 0


def _args_run_diagnostics(args):
    return 0


def get_main_parser():
    desc = "Tool for testing current workflow configuration."
    p = U.get_base_pacbio_parser(pbsmrtpipe.get_version(), desc)
    _add_preset_xml_option(p)
    _add_full_option(p)

    p.set_defaults(func=_args_run_diagnostics)
    return p


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_main_parser()

    return main_runner_default(argv_[1:], parser, log)
