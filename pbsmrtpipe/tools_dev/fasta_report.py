import logging
import sys

from pbcore.io import FastaWriter, FastaReader
from pbcommand.utils import setup_log

import pbsmrtpipe.mock as M

from pbcommand.cli import pacbio_args_or_contract_runner_emit
from pbcommand.models import get_default_contract_parser, FileTypes, TaskTypes
from pbsmrtpipe.tools.dev import run_fasta_report

log = logging.getLogger(__name__)

TOOL_ID = "pbsmrtpipe.tasks.dev_tc_fasta_report"


def get_contract_parser():
    driver = "python -m pbsmrtpipe.tools_dev.fasta_report --resolved-tool-contract "
    desc = "Generate a PacBio report from Fasta file"

    p = get_default_contract_parser(TOOL_ID, "0.1.0", desc, driver, TaskTypes.LOCAL, 1, [])

    p.add_input_file_type(FileTypes.FASTA, "fasta_in", "Fasta In", "Pac Bio Fasta format")
    p.add_output_file_type(FileTypes.REPORT, "rpt_out", "Report", "PacBio ReportFiltered Fasta by sequence length Fasta", "fasta_report.json")

    return p


def args_runner(args):
    return run_fasta_report(args.fasta_in, args.rpt_out)


def rtc_runner(rtc):
    """
    :type rtc: pbcommand.models.ResolvedToolContract
    :return:
    """
    return run_fasta_report(rtc.task.input_files[0], rtc.task.output_files[0])


def main(argv=sys.argv):
    mp = get_contract_parser()
    return pacbio_args_or_contract_runner_emit(argv[1:],
                                               mp,
                                               args_runner,
                                               rtc_runner,
                                               log,
                                               setup_log)


if __name__ == '__main__':
    sys.exit(main())
