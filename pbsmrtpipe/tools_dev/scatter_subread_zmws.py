
"""
Scatter subreads by ZMW range, used for input to ConsensusRead processing.
"""

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

TOOL_ID = "pbsmrtpipe.tasks.subreadset_zmw_scatter"


class Constants(object):
    DEFAULT_NCHUNKS = 5


def get_contract_parser():
    driver = "python -m pbsmrtpipe.tools_dev.scatter_subread_zmws --resolved-tool-contract "

    chunk_keys = ("$chunk.subreadset_id", )
    p = get_scatter_pbparser(TOOL_ID, "0.1.3", "SubreadSet ZMW scatter",
                             "Scatter Subread DataSet by ZMWs", driver,
                             chunk_keys, is_distributed=False)

    p.add_input_file_type(FileTypes.DS_SUBREADS,
                          "subreadset",
                          "SubreadSet",
                          "Pac Bio Fasta format")

    p.add_output_file_type(FileTypes.CHUNK,
                           "chunk_report_json",
                           "Chunk SubreadSet",
                           "PacBio Chunked JSON SubreadSet",
                           "subreadset_chunked.json")

    # max nchunks for this specific task
    p.add_int("pbsmrtpipe.task_options.scatter_subread_max_nchunks",
              "max_nchunks", Constants.DEFAULT_NCHUNKS,
              "Max NChunks", "Maximum number of Chunks")

    p.add_str("pbsmrtpipe.task_options.scatter_subreadset_chunk_key",
              "chunk_key", "$chunk:subreadset_id", "Chunk key",
              "Chunk key to use (format $chunk:{chunk-key}")
    return p


def run_main(chunk_output_json, subread_xml, max_nchunks, output_dir):
    return CU.write_subreadset_zmw_chunks_to_file(
        chunk_file=chunk_output_json,
        subreadset_path=subread_xml,
        max_total_chunks=max_nchunks,
        dir_name=output_dir,
        chunk_base_name="chunk_subreadset",
        chunk_ext='xml')


def _args_run_to_random_fasta_file(args):
    return run_main(args.chunk_report_json, args.subreadset,
                    args.max_nchunks, os.path.dirname(args.chunk_report_json))


def _rtc_runner(rtc):
    output_dir = os.path.dirname(rtc.task.output_files[0])
    max_nchunks = rtc.task.max_nchunks
    return run_main(rtc.task.output_files[0], rtc.task.input_files[0],
                    max_nchunks, output_dir)


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
