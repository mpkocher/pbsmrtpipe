import os
import argparse
import logging

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


def get_base_pacbio_parser(version, description):
    """Get an argparse Parser instance

    :param version: version of your tool
    :param description: Description of your tool
    :return: Parser instance
    """
    p = argparse.ArgumentParser(version=version,
                                description=description,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
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


def add_force_distribute_option(p):
    p.add_argument('--force-distribute', action="store_true",
                   help="Override distribute mode in preset.xml and enabling distributed mode. If pbsmrtpipe.options.cluster_manager is not defined, distribute mode will still be disabled.")
    return p
