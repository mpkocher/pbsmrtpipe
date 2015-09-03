
import logging
import sys

from pbcommand.cli import pbparser_runner
from pbcommand.models import get_gather_pbparser, FileTypes
from pbcommand.utils import setup_log

from pbsmrtpipe.tools.gather import run_main_gather_json_stats

log = logging.getLogger(__name__)

class Constants(object):
    TOOL_ID = "pbsmrtpipe.tasks.gather_json_stats"
    CHUNK_KEY = "$chunk.json_id"
    VERSION = "0.1.0"
    DRIVER = "python -m pbsmrtpipe.tools_dev.gather_json_stats --resolved-tool-contract "
    OPT_CHUNK_KEY = 'pbsmrtpipe.task_options.gather_json_stats_chunk_key'


def get_parser():
    p = get_gather_pbparser(Constants.TOOL_ID,
                            Constants.VERSION,
                            "Dev JSON Gather",
                            "General Chunk JSON Statistics Gather",
                            Constants.DRIVER,
                            is_distributed=False)
    p.add_input_file_type(FileTypes.CHUNK, "cjson_in", "GCHUNK Json",
                          "Gathered CHUNK Json with Json chunk key")

    p.add_output_file_type(FileTypes.JSON, "json_out",
                           "JSON",
                           "Gathered JSON", "gathered.json")

    # Only need to add to argparse layer for the commandline
    p.arg_parser.add_str(Constants.OPT_CHUNK_KEY,
                         "chunk_key",
                         "$chunk.json_id",
                         "Chunk key",
                         "Chunk key to use (format $chunk.{chunk-key}")

    return p


def args_runner(args):
    return run_main_gather_json_stats(args.cjson_in, args.json_out, args.chunk_key)

def rtc_runner(rtc):
    return run_main_gather_json_stats(rtc.task.input_files[0], rtc.task.output_files[0], Constants.CHUNK_KEY)


def main(argv=sys.argv):
    return pbparser_runner(argv[1:],
                           get_parser(),
                           args_runner,
                           rtc_runner,
                           log,
                           setup_log)


if __name__ == '__main__':
    sys.exit(main())
