import logging
import os
import sys

from pbcore.io import FastaWriter, FastaReader
from pbcommand.utils import setup_log
from pbcommand.cli import pbparser_runner
from pbcommand.models import get_scatter_pbparser, FileTypes

import pbsmrtpipe.mock as M
import pbsmrtpipe.tools.chunk_utils as CU

log = logging.getLogger(__name__)

TOOL_ID = "pbsmrtpipe.tasks.dev_scatter_filter_fasta"


def get_contract_parser():
    driver = "python -m pbsmrtpipe.tools_dev.scatter_filter_fasta --resolved-tool-contract "

    chunk_keys = ("$chunk.fasta_id", )
    p = get_scatter_pbparser(TOOL_ID, "0.1.3", "Scatter Filter Fasta",
                             "Scatter Filter Fasta", driver, chunk_keys,
                             is_distributed=False)

    p.add_input_file_type(FileTypes.FASTA, "fasta_in", "Fasta In",
                          "Pac Bio Fasta format")
    p.add_output_file_type(FileTypes.CHUNK, "cjson_out", "Chunk JSON Filtered Fasta",
                           "Chunked JSON Filtered Fasta",
                           "fasta.chunked.json")
    # max nchunks for this specific task
    p.add_int("pbsmrtpipe.task_options.dev_scatter_max_nchunks", "max_nchunks", 7,
              "Max NChunks", "Maximum number of Chunks")
    p.add_str("pbsmrtpipe.task_options.dev_scatter_chunk_key", "chunk_key",
              "$chunk:fasta_id", "Chunk key", "Chunk key to use (format $chunk:{chunk-key}")
    return p


def run_main(fasta_file, output_json, max_nchunks, chunk_key):
    output_dir = os.path.dirname(output_json)
    CU.write_fasta_chunks_to_file(output_json, fasta_file, max_nchunks, output_dir, "scattered-fasta", "fasta")
    return 0


def _args_run_to_random_fasta_file(args):
    return run_main(args.fasta_in, args.cjson_out, args.max_nchunks, args.chunk_key)


def _rtc_runner(rtc):
    """
    :type rtc: pbcommand.models.ResolvedToolContract
    :return:
    """
    # the input file is just a sentinel file
    max_nchunks = rtc.task.options['pbsmrtpipe.task_options.dev_scatter_max_nchunks']
    chunk_key = '$chunk:fasta_id'
    return run_main(rtc.task.input_files[0], rtc.task.output_files[0], max_nchunks, chunk_key)


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
