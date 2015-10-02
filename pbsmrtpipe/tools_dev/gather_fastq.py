import logging
import sys

from pbcommand.utils import setup_log
from pbcommand.cli import pbparser_runner
from pbcommand.models import FileTypes, get_gather_pbparser

from pbsmrtpipe.tools.gather import run_main_gather_fastq_contigset

log = logging.getLogger(__name__)

TOOL_ID = "pbsmrtpipe.tasks.gather_fastq"
CHUNK_KEY = "$chunk.fastq_id"


def get_contract_parser():
    driver = "python -m pbsmrtpipe.tools_dev.gather_fastq --resolved-tool-contract "

    p = get_gather_pbparser(TOOL_ID, "0.1.3", "Gather Fastq",
                            "Gather Fastq", driver, is_distributed=False)

    p.add_input_file_type(FileTypes.CHUNK, "cjson_in", "Gather ChunkJson",
                          "Fastq Gather Chunk JSON")

    p.add_output_file_type(FileTypes.FASTQ, "fastq_out", "Fastq Gathered",
                           "Fastq Gathered",
                           "file_gathered.fastq")

    p.add_str("pbsmrtpipe.task_options.dev_scatter_chunk_key", "chunk_key",
              "$chunk:fastq_id", "Chunk key", "Chunk key to use (format $chunk:{chunk-key}")
    return p


def args_runner(args):
    return run_main_gather_fastq_contigset(args.cjson_in, args.fastq_out, CHUNK_KEY)


def rtc_runner(rtc):
    """
    :type rtc: pbcommand.models.ResolvedToolContract
    :return:
    """
    # the input file is just a sentinel file
    return run_main_gather_fastq_contigset(rtc.task.input_files[0], rtc.task.output_files[0], CHUNK_KEY)


def main(argv=sys.argv):
    mp = get_contract_parser()
    return pbparser_runner(argv[1:],
                           mp, args_runner,
                           rtc_runner,
                           log,
                           setup_log)


if __name__ == '__main__':
    sys.exit(main())
