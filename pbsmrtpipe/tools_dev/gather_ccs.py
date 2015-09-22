
"""
Gather chunked CCS reads, equivalent to gather_alignments.
"""

import logging
from pbcommand.cli import pbparser_runner
from pbcommand.models import get_gather_pbparser, FileTypes
import sys
from pbcommand.utils import setup_log
from pbsmrtpipe.tools.gather import run_main_gather_ccsset

log = logging.getLogger(__name__)


class Constants(object):
    TOOL_ID = "pbsmrtpipe.tasks.gather_ccsset"
    CHUNK_KEY = "$chunk:ccsset_id"
    VERSION = "0.1.0"
    DRIVER = "python -m pbsmrtpipe.tools_dev.gather_ccs --resolved-tool-contract "


def get_parser():
    p = get_gather_pbparser(Constants.TOOL_ID,
                            Constants.VERSION,
                            "Dev CCS Gather",
                            __doc__,
                            Constants.DRIVER,
                            is_distributed=False)

    p.add_input_file_type(FileTypes.CHUNK, "cjson_in", "GCHUNK Json",
                          "Gathered CHUNK Json with ConsensusReadSet chunk key")

    p.add_output_file_type(FileTypes.DS_CCS,
                           "ds_out",
                           "ConsensusReadSet",
                           "Gathered ConsensusReadSet",
                           "gathered.consensusreadset.xml")
    return p


def args_runner(args):
    return run_main_gather_ccsset(args.cjson_in,
                                  args.ds_out,
                                  args.chunk_key)


def rtc_runner(rtc):
    return run_main_gather_ccsset(rtc.task.input_files[0],
                                  rtc.task.output_files[0],
                                  rtc.task.chunk_key)


def main(argv=sys.argv):
    return pbparser_runner(argv[1:],
                           get_parser(),
                           args_runner,
                           rtc_runner,
                           log,
                           setup_log)


if __name__ == '__main__':
    sys.exit(main())
