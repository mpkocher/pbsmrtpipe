"""CLI Tools to facilitate development testing."""
import logging
import os
import random
import tempfile
import sys
import string

from pbcore.io import (FastaWriter, FastaReader)

import pbcore.io.dataset as DIO

from pbsmrtpipe.cli_utils import main_runner_default
from pbsmrtpipe.report_model import Report, Attribute
import pbsmrtpipe.tools.utils as U
import pbsmrtpipe.mock as M

__version__ = '0.1.0'

log = logging.getLogger(__name__)


def _add_run_random_fasta_file(p):
    U.add_debug_option(p)
    p.add_argument('--max-records', type=int, default=1000, help="Max number of Fasta record to write.")
    U.add_fasta_output(p)
    return p


def _args_run_to_random_fasta_file(args):
    M.write_random_fasta_records(args.fasta_out, args.max_records)
    return 0


def _add_run_fasta_filter_options(p):
    U.add_debug_option(p)
    p.add_argument('--min-length', type=int, default=150, help='Min Length of Sequence to filter')
    U.add_fasta_input(p)
    U.add_fasta_output(p)
    return p


def run_fasta_filter(fasta_in, fasta_out, min_seq_length):
    with FastaWriter(fasta_out) as w:
        with FastaReader(fasta_in) as r:
            for record in r:
                if len(record.sequence) > min_seq_length:
                    w.writeRecord(record)

    return 0


def _args_run_fasta_filter(args):
    return run_fasta_filter(args.fasta_in, args.fasta_out, args.min_length)


def _add_run_random_fastq_options(p):
    U.add_debug_option(p)
    p.add_argument('--max-records', type=int, default=1000, help="Max number of Fasta record to write.")
    U.add_fastq_output(p)
    return p


def _args_run_random_fastq_file(args):
    M.write_random_fastq_records(args.fastq_out, nrecords=args.max_records)
    return 0


def _add_run_random_fofn_options(p):
    U.add_debug_option(p)
    U.add_output_dir_option(p)
    p.add_argument('--nfofns', type=int, default=10, help="Number of mock/random Fofns to write.")
    U.add_fofn_output(p)
    return p


def __to_random_fofn(contents, path):
    with open(path, 'w+') as w:
        w.write(contents)


def run_random_fofn(output_fofn, output_dir, nfofns):

    fofns = []
    for i in xrange(nfofns):
        name = "random_{i}".format(i=i)
        file_name = ".".join([name, 'fofn'])
        p = os.path.join(output_dir, file_name)
        with open(p, 'w+') as w:
            w.write(name)
        fofns.append(p)

    M.write_fofn(output_fofn, fofns)
    return 0

def _args_run_random_fofn(args):
    return run_random_fofn(args.fofn_out, args.output_dir, args.nfofns)


def dataset_to_report(ds):
    """
    :type ds: DataSet
    :param ds:
    :return:
    """
    is_valid = all(os.path.exists(p) for p in ds.toExternalFiles())
    datum = [("uuid", ds.uuid, "Unique Id"),
             ("total_records", ds.numRecords, "num Records"),
             ("valid_files", is_valid, "External files exist")]
    attributes = [Attribute(x, y, name=z) for x, y, z in datum]
    return Report("ds_report", attributes=attributes, dataset_uuids=[ds.uuid])


def subread_dataset_report(subread_path, report_path):
    ds = DIO.DataSet(subread_path)
    report = dataset_to_report(ds)
    report.write_json(report_path)
    return 0

def _add_run_dataset_report(p):
    U.add_debug_option(p)
    U.add_subread_input(p)
    U.add_report_output(p)
    return p


def _args_run_dataset_report(args):
    return subread_dataset_report(args.subread_ds, args.json_report)


def get_main_parser():
    p = U.get_base_pacbio_parser(__version__, "Tool For generating MOCK data for development testing.")

    sp = p.add_subparsers(help="Subparser Commands")

    def _builder(subparser_id, desc, options_func, exe_func):
        U.subparser_builder(sp, subparser_id, desc, options_func, exe_func)

    _builder('fasta', "Generate a random Fasta file", _add_run_random_fasta_file, _args_run_to_random_fasta_file)

    _builder('fastq', "Generate a random Fastq File", _add_run_random_fastq_options, _args_run_random_fastq_file)

    _builder('fofn', "Generate a random FOFN file", _add_run_random_fofn_options, _args_run_random_fofn)

    _builder('filter-fasta', "Filter a Fasta file by sequence length", _add_run_fasta_filter_options, _args_run_fasta_filter)

    _builder("dataset-report", "DataSet Report Generator", _add_run_dataset_report, _args_run_dataset_report)

    return p


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_main_parser()

    return main_runner_default(argv_[1:], parser, log)


if __name__ == '__main__':
    sys.exit(main())