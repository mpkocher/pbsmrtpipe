import logging
import sys
from pbcommand.utils import setup_log

import pbsmrtpipe.mock as M

from pbcommand.cli import pbparser_runner
from pbcommand.models import get_pbparser, FileTypes

log = logging.getLogger(__name__)

TOOL_ID = "pbsmrtpipe.tasks.dev_fasta"


def get_contract_parser():
    driver = "python -m pbsmrtpipe.tools_dev.fasta --emit-tool-contract "
    p = get_pbparser(TOOL_ID, "0.1.0", "Dev Fasta", "Generate a random fasta file", driver,
                     is_distributed=False)
    p.add_input_file_type(FileTypes.TXT, "txt", "Sentinel", "Sentinel file to trigger start")
    p.add_output_file_type(FileTypes.FASTA, "fasta_out", "Filtered Fasta", "Filtered Fasta by sequence length Fasta", "filtered.fasta")
    p.add_int("pbsmrtpipe.task_options.dev_fasta.max_records", "max_records", 1000, "Max Number of records to generate", "Max Records")
    return p


def _args_run_to_random_fasta_file(args):
    M.write_random_fasta_records(args.fasta_out, args.max_records)
    return 0


def _rtc_runner(rtc):
    """
    :type rtc: pbcommand.models.ResolvedToolContract
    :return:
    """
    M.write_random_fasta_records(rtc.task.input_files[0], rtc.task.output_files[0])
    return 0


def main(argv=sys.argv):
    mp = get_contract_parser()
    return pbparser_runner(argv[1:],
                                               mp,
                                               _args_run_to_random_fasta_file,
                                               _rtc_runner,
                                               log,
                                               setup_log)


if __name__ == '__main__':
    sys.exit(main())
