import sys
import argparse
import logging
import json

from functools import partial as P


from pbcommand.cli import get_default_argparser

from pbcore.io.FastaIO import FastaReader, FastaWriter
from pbcore.io.FastqIO import FastqReader, FastqWriter
from pbcore.io.GffIO import GffReader, GffWriter
from pbcore.io import (SubreadSet, ContigSet, AlignmentSet, ConsensusReadSet,
                       ConsensusAlignmentSet)

from pbsmrtpipe.cli_utils import main_runner_default, validate_file

import pbsmrtpipe.tools.utils as U
import pbsmrtpipe.pb_io as IO
from pbsmrtpipe.utils import fofn_to_files

log = logging.getLogger(__name__)

__version__ = '1.0.1'


def _validate_chunk_json_file(path):
    chunks = IO.load_pipeline_chunks_from_json(path)
    return path


def __gather_fastx(fastx_reader, fastx_writer, fastx_files, output_file):

    n = 0
    with fastx_writer(output_file) as writer:
        for fastx_file in fastx_files:
            with fastx_reader(fastx_file) as reader:
                for record in reader:
                    n += 1
                    writer.writeRecord(record)

    log.info("Completed gathering {n} files (with {x} records) to {f}".format(n=len(fastx_files), f=output_file, x=n))


gather_fasta = P(__gather_fastx, FastaReader, FastaWriter)
gather_fastq = P(__gather_fastx, FastqReader, FastqWriter)
gather_gff = P(__gather_fastx, GffReader, GffWriter)


def _read_header(csv_file):
    with open(csv_file, 'r') as f:
        header = f.readline()

    if ',' in header:
        return header.rstrip().split(',')
    else:
        return None


def __has_header(handle):
    handle.next()


def __has_header_and_one_record(handle):
    # has header
    __has_header(handle)
    # has at least one record
    handle.next()


def __csv_inspector(func, csv_file):

    is_empty = False
    with open(csv_file, 'r') as f:
        try:
            func(f)
        except StopIteration:
            is_empty = True

    return is_empty


_csv_is_empty = P(__csv_inspector, __has_header_and_one_record)
_csv_has_header = P(_csv_is_empty, __has_header)


def get_datum_from_chunks_by_chunk_key(chunks, chunk_key):
    log.info("extracting datum from chunks using chunk-key '{c}'".format(c=chunk_key))
    datum = []
    for chunk in chunks:
        if chunk_key in chunk.chunk_keys:
            value = chunk.chunk_d[chunk_key]
            datum.append(value)
        else:
            raise KeyError("Unable to find chunk key '{i}' in {p}".format(i=chunk_key, p=chunk))

    return datum


def gather_csv(csv_files, output_file, skip_empty=True):
    """

    :param csv_files:
    :param output_file:
    :param skip_empty: Emtpy files with or without the header will be skipped

    :type skip_empty: bool
    :return:
    """

    header = _read_header(csv_files[0])
    #nfields = 0 if header is None else len(header)

    with open(output_file, 'w') as writer:
        if header is not None:
            writer.write(",".join(header) + "\n")
        for csv_file in csv_files:
            if not _csv_is_empty(csv_file):
                header = _read_header(csv_file)
                # should do a comparison of the headers to make sure they
                # have the same number of fields
                with open(csv_file, 'r') as f:
                    # skip header
                    _ = f.readline()
                    for record in f:
                        writer.write(record)

    log.info("successfully merged {n} files to {f}".format(n=len(csv_files), f=output_file))

    return output_file


def gather_fofn(input_files, output_file, skip_empty=True):
    """
    This should be better spec'ed and impose a tighter constraint on the FOFN

    :param input_files: List of file paths
    :param output_file: File Path
    :param skip_empty: Ignore empty files

    :return: Output file

    :rtype: str
    """

    all_files = []
    for input_file in input_files:
        file_names = fofn_to_files(input_file)
        all_files.extend(file_names)

    with open(output_file, 'w') as f:
        f.write("\n".join(all_files))

    return output_file


def gather_contigset(input_files, output_file, new_resource_file=None,
                     skip_empty=True):
    """
    :param input_files: List of file paths
    :param output_file: File Path
    :param new_resource_file: the path of the file to which the other contig
                              files are consolidated
    :param skip_empty: Ignore empty files (doesn't do much yet)

    :return: Output file

    :rtype: str
    """
    tbr = ContigSet(*input_files)
    if not new_resource_file:
        if output_file.endswith('xml'):
            new_resource_file = output_file[:-3] + 'fasta'
    tbr.consolidate(new_resource_file)
    tbr.write(output_file)
    return output_file


def __gather_readset(dataset_type, input_files, output_file, skip_empty=True):
    """
    :param input_files: List of file paths
    :param output_file: File Path
    :param skip_empty: Ignore empty files (doesn't do much yet)

    :return: Output file

    :rtype: str
    """
    tbr = dataset_type(*input_files)
    tbr.write(output_file)
    return output_file


gather_subreadset = P(__gather_readset, SubreadSet)
gather_alignmentset = P(__gather_readset, AlignmentSet)
gather_ccsset = P(__gather_readset, ConsensusReadSet)
gather_ccs_alignmentset = P(__gather_readset, ConsensusAlignmentSet)


def __add_chunk_key_option(default_chunk_key):
    def _add_chunk_key_option(p):
        p.add_argument('--chunk-key', type=str, default=default_chunk_key,
                       help="Chunk key (e.g, $chunk.my_chunk_key_id) to gather over")
        return p
    return _add_chunk_key_option

add_chunk_key_csv = __add_chunk_key_option('$chunk.csv_id')
add_chunk_key_fasta = __add_chunk_key_option('$chunk.fasta_id')
add_chunk_key_fastq = __add_chunk_key_option('$chunk.fastq_id')
add_chunk_key_gff = __add_chunk_key_option('$chunk.gff_id')
add_chunk_key_fofn = __add_chunk_key_option('$chunk.fofn_id')
add_chunk_key_subreadset = __add_chunk_key_option('$chunk.subreadset_id')
add_chunk_key_alignmentset = __add_chunk_key_option('$chunk.alignmentset_id')
add_chunk_key_ccsset = __add_chunk_key_option('$chunk.ccsset_id')
add_chunk_key_ccs_alignmentset = __add_chunk_key_option(
    '$chunk.ccs_alignmentset_id')
# TODO: change this to contigset_id once quiver emits contigsets
add_chunk_key_contigset = __add_chunk_key_option('$chunk.fasta_id')


def __gather_options(output_file_message, input_files_message, input_validate_func, add_chunk_key_func_):
    def _f(p):
        p.add_argument('chunk_json',
                       type=input_validate_func,
                       help=input_files_message)
        p.add_argument('-o', '--output', type=str, help=output_file_message)
        return add_chunk_key_func_(p)
    return _f


def __add_gather_options(output_file_msg, input_file_msg, chunk_key_func):
    def _f(p):
        U.add_debug_option(p)
        f = __gather_options(output_file_msg, input_file_msg, validate_file, chunk_key_func)
        return f(p)
    return _f


_gather_csv_options = __add_gather_options("Output CSV file", "input CSV file", add_chunk_key_csv)
_gather_fastq_options = __add_gather_options("Output Fastq file", "Chunk input JSON file", add_chunk_key_fastq)
_gather_fasta_options = __add_gather_options("Output Fasta file", "Chunk input JSON file", add_chunk_key_fasta)
_gather_gff_options = __add_gather_options("Output Fasta file",
                                           "Chunk input JSON file",
                                           add_chunk_key_gff)
_gather_fofn_options = __add_gather_options(
    "Output Fofn file", "Chunk input JSON file", add_chunk_key_fofn)
_gather_subreadset_options = __add_gather_options("Output SubreadSet XML file",
                                                  "Chunk input JSON file",
                                                  add_chunk_key_subreadset)
_gather_ccsset_options = __add_gather_options("Output ConsensusReadSet XML file",
                                              "Chunk input JSON file",
                                              add_chunk_key_ccsset)
_gather_alignmentset_options = __add_gather_options("Output AlignmentSet XML file",
                                                    "Chunk input JSON file",
                                                    add_chunk_key_alignmentset)
_gather_ccs_alignmentset_options = __add_gather_options("Output ConsensusAlignmentSet XML file",
                                                        "Chunk input JSON file",
                                                        add_chunk_key_ccs_alignmentset)
_gather_contigset_options = __add_gather_options("Output ContigSet XML file",
                                                 "Chunk input JSON file",
                                                 add_chunk_key_contigset)


def __gather_runner(func, chunk_input_json, output_file, chunk_key):
    chunks = IO.load_pipeline_chunks_from_json(chunk_input_json)

    # Allow looseness
    if not chunk_key.startswith('$chunk.'):
        chunk_key = '$chunk.' + chunk_key
        log.warn("Prepending chunk key with '$chunk.' to '{c}'".format(c=chunk_key))

    chunked_files = get_datum_from_chunks_by_chunk_key(chunks, chunk_key)
    _ = func(chunked_files, output_file)
    return 0


def __rtc_gather_runner(func, rtc):
    # Gather Resolved ToolContracts will have a chunk
    return func(rtc.task.input_files[0], rtc.task.output_files[0], rtc.task.chunk_key)


def __args_gather_runner(func, args):
    return __gather_runner(func, args.chunk_json, args.output, args.chunk_key)

# These make assumptions about the CLI argparser args labels (e.g.,
# args.chunk_key)
_args_runner_gather_fasta = P(__args_gather_runner, gather_fasta)
_args_runner_gather_gff = P(__args_gather_runner, gather_gff)
_args_runner_gather_fastq = P(__args_gather_runner, gather_fastq)
_args_runner_gather_fofn = P(__args_gather_runner, gather_fofn)
_args_runner_gather_subreadset = P(__args_gather_runner, gather_subreadset)
_args_runner_gather_alignmentset = P(__args_gather_runner, gather_alignmentset)
_args_runner_gather_ccsset = P(__args_gather_runner, gather_ccsset)
_args_runner_gather_ccs_alignmentset = P(
    __args_gather_runner, gather_ccs_alignmentset)
_args_runner_gather_contigset = P(__args_gather_runner, gather_contigset)
_args_runner_gather_csv = P(__args_gather_runner, gather_csv)

# (chunk.json, output_file, chunk_key)
run_main_gather_fasta = P(__gather_runner, gather_fasta)
run_main_gather_fastq = P(__gather_runner, gather_fastq)
run_main_gather_csv = P(__gather_runner, gather_csv)
run_main_gather_gff = P(__gather_runner, gather_gff)
run_main_gather_alignmentset = P(__gather_runner, gather_alignmentset)
run_main_gather_subreadset = P(__gather_runner, gather_subreadset)
run_main_gather_contigset = P(__gather_runner, gather_contigset)
run_main_gather_ccsset = P(__gather_runner, gather_ccsset)
run_main_gather_ccs_alignmentset = P(__gather_runner, gather_ccs_alignmentset)


def get_parser():

    desc = "Gathering File Tool used within pbsmrtpipe on chunk.json files."
    p = get_default_argparser(__version__, desc)

    sp = p.add_subparsers(help="Commands")

    def builder(sid_, help_, opt_func_, exe_func_):
        return U.subparser_builder(sp, sid_, help_, opt_func_, exe_func_)

    # CSV
    builder('csv', "Merge CSV files into a single file.",
            _gather_csv_options, _args_runner_gather_csv)

    # Fastq
    builder('fastq', "Merge Fastq files into a single file.",
            _gather_fastq_options, _args_runner_gather_fastq)

    # Fasta
    builder('fasta', "Merge Fasta files into a single file.",
            _gather_fasta_options, _args_runner_gather_fasta)

    builder('fofn', "Merge FOFNs into a single file.",
            _gather_fofn_options, _args_runner_gather_fofn)

    # Gff
    builder('gff', "Merge Fasta files into a single file.",
            _gather_gff_options, _args_runner_gather_gff)

    # SubreadSet
    builder('subreadset', "Merge SubreadSet XMLs into a single file.",
            _gather_subreadset_options, _args_runner_gather_subreadset)

    # AlignmentSet
    builder('alignmentset', "Merge AlignmentSet XMLs into a single file.",
            _gather_alignmentset_options, _args_runner_gather_alignmentset)

    # ConsensusReadSet
    builder('ccsset', "Merge ConsensusReadSet XMLs into a single file.",
            _gather_ccsset_options, _args_runner_gather_ccsset)

    # ConsensusAlignmentSet
    builder('ccs_alignmentset',
            "Merge ConsensusAlignmentSet XMLs into a single file.",
            _gather_ccs_alignmentset_options, _args_runner_gather_ccs_alignmentset)

    # ContigSet
    builder('contigset', "Merge ContigSet XMLs into a single file.",
            _gather_contigset_options, _args_runner_gather_contigset)

    return p


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_parser()

    return main_runner_default(argv_[1:], parser, log)
