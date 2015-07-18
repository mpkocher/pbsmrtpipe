import logging
import sys

from pbcore.io import FastaWriter, FastaReader
from pbcommand.utils import setup_log

import pbsmrtpipe.mock as M

from pbcommand.cli import pacbio_args_or_contract_runner_emit
from pbcommand.models import get_default_contract_parser, FileTypes, TaskTypes

log = logging.getLogger(__name__)

TOOL_ID = "pbsmrtpipe.tasks.dev_filter_fasta"


def get_contract_parser():
    driver = "python -m pbsmrtpipe.tools_dev.filter_fasta --resolved-tool-contract "
    p = get_default_contract_parser(TOOL_ID,
                                    "0.1.0",
                                    "Filter Fasta file by Sequence Length",
                                    driver,
                                    TaskTypes.LOCAL,
                                    1, [])

    p.add_input_file_type(FileTypes.FASTA, "fasta_in", "Fasta In", "Pac Bio Fasta format")
    p.add_output_file_type(FileTypes.FASTA, "fasta_out", "Filtered Fasta", "Filtered Fasta by sequence length Fasta", "filtered.fasta")
    p.add_int("pbsmrtpipe.tool_options.dev_fasta.min_length", "min_length", 50, "Min Sequence Length", "Minimum Sequence Length to filter")
    return p


def run_fasta_filter(fasta_in, fasta_out, min_seq_length):
    with FastaWriter(fasta_out) as w:
        with FastaReader(fasta_in) as r:
            for record in r:
                if len(record.sequence) > min_seq_length:
                    w.writeRecord(record)

    return 0


def _args_run_to_random_fasta_file(args):
    run_fasta_filter(args.fasta_in, args.fasta_out, args.min_length)
    return 0


def _rtc_runner(rtc):
    """
    :type rtc: pbcommand.models.ResolvedToolContract
    :return:
    """
    # the input file is just a sentinel file
    M.write_random_fasta_records(rtc.task.output_files[0])
    return 0


def main(argv=sys.argv):
    mp = get_contract_parser()
    return pacbio_args_or_contract_runner_emit(argv[1:],
                                               mp,
                                               _args_run_to_random_fasta_file,
                                               _rtc_runner,
                                               log,
                                               setup_log)


if __name__ == '__main__':
    sys.exit(main())
