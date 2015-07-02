"""General commandline conversions tools that are called from pbsmrtpipe"""
import os
import sys
import logging
import argparse
import functools

from pbcore.io import FastaWriter, FastqReader, FastaRecord, readFofn
from pbsmrtpipe.cli_utils import main_runner_default, validate_file

from pbsmrtpipe.legacy.input_xml import InputXml, input_xml_to_report, fofn_to_report
import pbsmrtpipe.legacy.reference_utils as RU

import pbsmrtpipe.tools.utils as U
from pbsmrtpipe.utils import validate_fofn


__version__ = '1.0'

log = logging.getLogger()


def _add_debug_to_parser(p, message="Emit logging output to stdout."):
    p.add_argument('--debug', action='store_true', required=False, help=message)
    return p


def write_report_and_log(report, output_file):
    report.write_json(output_file)
    log.debug("Wrote report {r} to {o}".format(r=report, o=output_file))
    return True


def write_movie_fofn_report(input_xml_file, movie_fofn, report_json):
    """Converts an input.xml file to a movie fofn and json report"""

    input_xml = InputXml.from_file(input_xml_file)
    movies = input_xml.movies

    with open(movie_fofn, 'w+') as f:
        f.write("\n".join(sorted(movies)) + "\n")
    log.debug("Write {n} movies to FOFN {x}".format(n=len(movies), x=movie_fofn))

    if report_json is not None:
        report = input_xml_to_report(input_xml)
        write_report_and_log(report, report_json)

    return True


def _add_input_xml_to_fofn_options(p):
    _add_debug_to_parser(p)
    p.add_argument('input_xml', type=validate_file, help="PacBio input.xml file")
    p.add_argument('movie_fofn', type=str, help="output movie.fofn path",
                   default="movie.fofn")
    p.add_argument('--report', type=str, help="Path to output JSON Report.")
    return p


def _args_input_xml_to_movie_fofn(args):
    _ = write_movie_fofn_report(args.input_xml, args.movie_fofn, args.report)
    return 0


def _fastq_to_fasta(fastq_path, fasta_path):
    """Convert a fastq file to  fasta file"""
    with FastqReader(fastq_path) as r:
        with FastaWriter(fasta_path) as w:
            for fastq_record in r:
                fasta_record = FastaRecord(fastq_record.name, fastq_record.sequence)
                w.writeRecord(fasta_record)

    log.info("Completed converting {q} to {f}".format(q=fastq_path, f=fasta_path))
    return True


def _add_fastx_option(fastx_name, description):

    def f(p):
        p.add_argument(fastx_name, type=validate_file, help=description)
        return p

    return f


_add_fasta_file_option = _add_fastx_option("fasta", "Path to Fasta file.")
_add_fastq_file_option = _add_fastx_option("fastq", "Path to Fastq file.")


def _add_fastq_to_fastq_options(p):
    p = _add_debug_to_parser(p)
    p = _add_fasta_file_option(p)
    p = _add_fastq_file_option(p)
    return p


def _args_run_fastq_to_fasta(args):
    _ = _fastq_to_fasta(args.fastq, args.fasta)
    return 0


def _validate_file_or_dir(p):
    if os.path.exists(p):
        return os.path.abspath(p)
    raise IOError("Unable to find resource '{p}'".format(p=p))


def _add_ref_to_report_options(p):
    _add_debug_to_parser(p)
    p.add_argument('ref', type=_validate_file_or_dir, help='Reference Entry Dir or Path to reference.info.xml')
    p.add_argument('output_json', type=str, help='Path to output JSON report.')
    return p


def _args_run_ref_to_report(args):
    return RU.run_ref_to_report(args.ref, args.output_json)


def _add_fofn_to_report_options(p):
    p = _add_debug_to_parser(p)
    p.add_argument('input_fofn', type=validate_fofn, help="Path to FOFN")
    p.add_argument('report_json', type=str, help="Path to output json report.")
    return p


def _args_fofn_to_report(args):
    files = list(readFofn(args.input_fofn))
    report = fofn_to_report(len(files))
    write_report_and_log(report, args.report_json)
    return 0


def _add_fasta_to_contig_id_fofn_options(p):
    p = _add_debug_to_parser(p)
    p.add_argument("fasta", type=_validate_file_or_dir, help="Path to Pacbio Reference entry XML or FASTA file.")
    p.add_argument('output_fofn', type=str, help="Output List of contig Ids")
    return p


def _run_reference_entry_to_contig_attr_fofn(ru_func, reference_entry, contig_fofn):
    contig_ids = ru_func(reference_entry)
    with open(contig_fofn, 'w') as f:
        f.write("\n".join(contig_ids))
    return 0


def run_reference_entry_to_contig_id_fofn(reference_entry, contig_fofn):
    return _run_reference_entry_to_contig_attr_fofn(RU.reference_entry_to_contig_ids, reference_entry, contig_fofn)


def run_reference_entry_to_contig_idx_fofn(reference_entry, contig_fofn):
    # See the comments on contig_idx for why
    return _run_reference_entry_to_contig_attr_fofn(RU.reference_entry_to_contig_idx, reference_entry, contig_fofn)


def _args_fasta_to_contig_id_fofn(args):
    reference_entry = RU.load_reference_entry(args.fasta)
    return run_reference_entry_to_contig_id_fofn(reference_entry, args.output_fofn)


def _args_fasta_to_contig_idx_fofn(args):
    reference_entry = RU.load_reference_entry(args.fasta)
    # See the comments on contig_idx for why
    return run_reference_entry_to_contig_idx_fofn(reference_entry, args.output_fofn)


def get_main_parser():
    """
    Returns an argparse Parser with all the commandline utils as
    subparsers
    """
    desc = "General tool used by pbsmrtpipe to convert files and generate pbreport JSON report files."
    p = U.get_base_pacbio_parser(__version__, desc)

    sp = p.add_subparsers(help='Subparser Commands')

    def builder(sid_, help_, opt_func_, exe_func_):
        return U.subparser_builder(sp, sid_, help_, opt_func_, exe_func_)

    builder("fofn-to-report",
            "Create a pbreports json metadata Report from FOFN (file of file names)",
            _add_fofn_to_report_options, _args_fofn_to_report)

    builder('ref-to-report', "Convert a Reference Entry director to a JSON metadata report.",
            _add_ref_to_report_options, _args_run_ref_to_report)

    # InputXML to MovieFofn
    builder('input-xml-to-fofn', "Convert a Pacbio Input.xml file to Movie FOFN",
            _add_input_xml_to_fofn_options, _args_input_xml_to_movie_fofn)

    builder("fastq-to-fasta", "Convert fastq file to fasta file.",
            _add_fastq_to_fastq_options, _args_run_fastq_to_fasta)

    builder("fasta-to-contig-id-fofn",
            "Convert a Pacbio Fasta Reference Entry to a List of contig ids (ref0000X) format",
            _add_fasta_to_contig_id_fofn_options, _args_fasta_to_contig_id_fofn)

    builder("fasta-to-contig-idx-fofn",
            "Convert a Pacbio Fasta Reference Entry to a List of contig header (Using the pbcore 'id' definition)",
            _add_fasta_to_contig_id_fofn_options, _args_fasta_to_contig_idx_fofn)

    return p


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_main_parser()

    return main_runner_default(argv_[1:], parser, log)
