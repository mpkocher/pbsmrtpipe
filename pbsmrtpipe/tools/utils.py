import os
import argparse
import logging
import platform

from pbsmrtpipe.utils import validate_file, validate_fofn

log = logging.getLogger(__name__)


def subparser_builder(subparser, subparser_id, description, options_func, exe_func):
    """
    Util to add subparser options

    :param subparser:
    :param subparser_id:
    :param description:
    :param options_func: Function that will add args and options to Parser instance F(subparser) -> None
    :param exe_func: Function to run F(args) -> Int
    :return:
    """
    p = subparser.add_parser(subparser_id, help=description)
    options_func(p)
    p.set_defaults(func=exe_func)
    return p


def add_debug_option(p):
    p.add_argument('--debug', action='store_true',
                   help="Send logging info to stdout.")
    return p


def _validate_output_dir_or_get_default(value):
    if value is None:
        return os.getcwd()
    else:
        if os.path.exists(value):
            return os.path.abspath(value)
        else:
            os.mkdir(value)
            return os.path.abspath(value)


def add_output_dir_option(p):
    p.add_argument('-o', '--output-dir', type=_validate_output_dir_or_get_default, default=os.getcwd(), help="Output directory.")
    return p


def _add_input_file(args_label, type_, help_):
    def _wrapper(p):
        p.add_argument(args_label, type=type_, help=help_)
        return p
    return _wrapper


add_fasta_output = _add_input_file("fasta_out", str, "Path to output Fasta File")
add_fasta_input = _add_input_file("fasta_in", validate_file, "Path to Input FASTA File")

add_fastq_output = _add_input_file("fastq_out", str, "Path to output Fastq File")
add_fastq_input = _add_input_file("fastq_in", validate_file, "Path to Input FASTQ File")

add_fofn_input = _add_input_file("fofn_in", validate_fofn, "Path to Input FOFN (File of file names) File")
add_fofn_output = _add_input_file("fofn_out", str, "Path to output FOFN.")

add_report_output = _add_input_file("json_report", str, "Path to PacBio JSON Report")

add_subread_input = _add_input_file("subread_ds", validate_file, "Path to PacBio Subread DataSet XML")

add_ds_reference_input = _add_input_file("reference_ds", validate_file, "Path to PacBio Subread DataSet XML")


def add_override_distribute_option(p):
    g = p.add_mutually_exclusive_group()
    g.add_argument('--force-distributed', action='store_const', const=True, default=None,
                   help="Override XML settings to enable distributed mode (if cluster manager is provided)")
    g.add_argument('--local-only', action='store_const', const=True, default=None,
                   help="Override XML settings to disable distributed mode. All Task will be submitted to {n}".format(n=platform.node()))
    return p


def add_override_chunked_mode(p):
    g = p.add_mutually_exclusive_group()
    g.add_argument('--force-chunk-mode', action='store_const', const=True, default=None, help="Override to enable Chunk mode")
    g.add_argument('--disable-chunk-mode', action='store_const', const=True, default=None, help="Override to disable Chunk mode")
    return p
