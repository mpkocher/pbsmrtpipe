import os
import sys
import logging
import math

from pbcore.io.FastaIO import FastaReader
from pbsmrtpipe.cli_utils import main_runner_default, validate_file

from pbsmrtpipe.legacy.reference_utils import (load_reference_entry,
                                               ReferenceEntry)

from pbsmrtpipe.models import PipelineChunk

from pbcommand.models.report import Report, Attribute
import pbsmrtpipe.tools.chunk_utils as CU
import pbsmrtpipe.tools.utils as U
import pbsmrtpipe.pb_io as IO
from pbsmrtpipe.utils import fofn_to_files, validate_fofn


log = logging.getLogger(__name__)

__version__ = "0.1.0"


def chunker_by_max_nchunks(alist, chunk_size):
    """Limit the individual size of each chunk"""
    if chunk_size > len(alist):
        n = len(alist)
    else:
        n = int(len(alist) / chunk_size + 1)
    return [alist[i:i + n] for i in range(0, len(alist), n)]


def chunker_by_max_chunksize(alist, max_nchunks):
    """Limit the max total number of chunks"""
    x = int(math.ceil(len(alist) / max_nchunks + 1))
    return chunker_by_max_nchunks(alist, x)


class Constants(object):
    FOFN_REPORT_ID = "fofn_chunk_report"
    FOFN_ATTRIBUTE_ID = "fofn_nchunks"

    CHUNK_KEY_ALNSET = "$chunk.alignmentset_id"
    CHUNK_KEY_FOFN = "$chunk.fofn_id"
    CHUNK_KEY_MOVIE_FOFN = "$chunk.movie_id"
    CHUNK_KEY_RGN_FOFN = "$chunk.rgn_id"
    CHUNK_KEY_FASTA = "$chunk.fasta_id"
    CHUNK_KEY_FASTQ = "$chunk.fastq_id"
    CHUNK_KEY_CSV = "$chunk.csv_id"

    CHUNK_KEY_CONTIG_ID = "$chunk.contig_id"
    CHUNK_KEY_CONTIG_NAME = "$chunk.contig_name_id"


def to_report(report_id, attribute_id, value):
    a = Attribute(attribute_id, value)
    r = Report(report_id, attributes=[a])
    return r


def write_report(report_id, attribute_id, value, report_file):
    r = to_report(report_id, attribute_id, value)
    r.write_json(report_file)
    return True


def nchunk_fofn(input_file, max_chunks):
    input_files = fofn_to_files(input_file)
    nchunks = min(len(input_files), max_chunks)
    return nchunks


def fofn_to_chunks(fofn):
    files = fofn_to_files(fofn)
    chunks = []
    for i, f in enumerate(files):
        chunk_id = "chunk-{i}".format(i=i)
        _d = {Constants.CHUNK_KEY_FOFN: f}
        p = PipelineChunk(chunk_id, **_d)
        chunks.append(p)
    return chunks


def run_chunk_fofn_file(input_fofn_file, max_chunks, chunk_json_report_file):
    """Chunk an FOFN into a Chunked JSON format"""
    n = nchunk_fofn(input_fofn_file, max_chunks)
    chunks = fofn_to_chunks(input_fofn_file)
    log.debug(chunks)
    IO.write_pipeline_chunks(chunks, chunk_json_report_file, "Generic FOFN Chunks")
    return 0


def add_max_nchunks_option(p):
    p.add_argument('--max-total-chunks', type=int, default=16, help="Chunk into X chunks.")
    return p


def _add_chunk_output_dir_option(p):
    p.add_argument('--output-dir', type=str, required=False, default=os.getcwd(),
                   help="Root directory to write chunked files to")
    return p


def _add_input_file_option(file_id, type_, help):
    def _f(p):
        p.add_argument(file_id, type=type_, help=help)
        return p
    return _f

# These are really 'options', but keeping the naming convention consistent
add_input_fofn_option = _add_input_file_option('input_fofn', validate_fofn, "Path to input.fofn (File of File names)")
add_input_movie_fofn_option = _add_input_file_option('movie_fofn', validate_fofn, "Path to movie files (.bas/.bax) FOFN.")
add_input_rgn_fofn_option = _add_input_file_option('region_fofn', validate_fofn, "Path to matching Region files (.rgn.h5) FOFN.")
add_input_fasta_option = _add_input_file_option('fasta', validate_file, "Path to Fasta file.")
add_input_fasta_reference_option = _add_input_file_option('fasta', validate_file, "Path to PacBio Reference Entry Fasta file.")
add_input_fastq_option = _add_input_file_option('fastq', validate_file, "Path to Fastq file")
add_input_alignmentset_option = _add_input_file_option(
    'alignmentset', validate_file, "Path to AlignmentSet XML file")
add_input_csv_option = _add_input_file_option('csv', validate_file, help="Path to CSV")
add_output_chunk_json_report_option = _add_input_file_option('chunk_report_json', str, help="Path to chunked JSON output")


def _add_common_chunk_options(p):
    # Order matters!
    U.add_debug_option(p)
    add_max_nchunks_option(p)
    p = _add_chunk_output_dir_option(p)
    p = add_output_chunk_json_report_option(p)
    return p


def _add_chunk_fofn_options(p):
    p = add_input_fofn_option(p)
    p = _add_common_chunk_options(p)
    return p


def _args_chunk_fofn(args):
    fofn_files = fofn_to_files(args.input_fofn)
    log.info("read in fofn with {n} files.".format(n=len(fofn_files)))
    chunks = CU.write_grouped_fofn_chunks(fofn_files, args.max_total_chunks, args.output_dir, args.chunk_report_json)
    log.debug("Converted {x} Fofn into {n} chunks. Write chunks to {f}".format(n=len(chunks), f=args.chunk_report_json, x=len(fofn_files)))
    return 0


def _add_chunk_contig_ids_options(p):
    add_input_fasta_option(p)
    add_input_fasta_reference_option(p)
    _add_common_chunk_options(p)
    return p


def _add_chunk_contig_ids_fofn_options(p):
    # p = add_input_fasta_reference_option(p)
    p.add_argument('reference_fasta', type=str,
                   help="Path to Reference Entry info XML file, Reference Fasta file, or Reference Directory.")
    p.add_argument('--contig-prefix-id', type=str, default="contig_group", help="Prefix of the contig group FOFN file name.")
    _add_common_chunk_options(p)
    return p


def fasta_to_pipeline_chunks(path):
    items = []
    with FastaReader(path) as r:
        for i, record in enumerate(r):
            _d = {Constants.CHUNK_KEY_CONTIG_NAME: record.id, "seq_size": len(record.sequence)}
            c = PipelineChunk("chunk-id-{i}".format(i=i), **_d)
            items.append(c)
    return items


def chunk_contig_by_ids(fasta_file, output_file):
    chunks = fasta_to_pipeline_chunks(fasta_file)
    IO.write_pipeline_chunks(chunks, output_file, "Chunk.json from {f}".format(f=fasta_file))
    return True


def to_chunks_by_contig_references(reference_entry):
    """

    :type reference_entry: ReferenceEntry
    :param reference_entry:
    :return: list[ChunkContig]
    """
    chunks = []
    for i, contig in enumerate(reference_entry.reference_info.contigs):
        _d = {Constants.CHUNK_KEY_CONTIG_NAME: contig.display_name, "contig_id": contig.id}
        c = PipelineChunk("chunk-id-{i}".format(i=i), **_d)
        chunks.append(c)

    return chunks


def write_contig_id_fofn(reference_entry, contig_id_prefix, output_dir, output_chunk_json, max_nchunks):
    """
    Write a chunk.json which points to fofn's containing contig id groups

    :param reference_entry:
    :param contig_id_prefix: (prefix file name of contig)
    :param output_dir:
    :param output_chunk_json:
    :param max_nchunks:
    :return:
    """
    contigs = reference_entry.reference_info.contigs
    grouped_contigs = chunker_by_max_nchunks(contigs, max_nchunks)

    log.debug("Writing {n} contig group chunks to {c}".format(c=output_dir, n=len(grouped_contigs)))

    chunks = []
    for i, contig_group in enumerate(grouped_contigs):
        cg_id = "contig_group_{i}".format(i=i)
        file_name = "_".join([contig_id_prefix, str(i) + ".fofn"])
        contig_fofn = os.path.join(output_dir, file_name)
        _d = {Constants.CHUNK_KEY_CONTIG_ID: contig_fofn, "ncontigs": len(contig_group)}
        c = PipelineChunk(cg_id, **_d)
        chunks.append(c)
        with open(contig_fofn, 'w') as f:
            for contig in contig_group:
                f.write(contig.id + "\n")

    IO.write_pipeline_chunks(chunks, output_chunk_json, "Chunk by Contig id from {f}".format(f=reference_entry))
    return 0


def _args_chunk_contig_ids(args):
    chunk_contig_by_ids(args.fasta_file, args.chunk_report_json)
    return 0


def _args_write_contig_id_fofn(args):
    reference_entry = load_reference_entry(args.reference_fasta)
    output_dir = os.getcwd() if args.output_dir is None else args.output_dir
    output_dir = os.path.abspath(output_dir)
    write_contig_id_fofn(reference_entry, args.contig_prefix_id, output_dir, args.chunk_report_json, args.max_total_chunks)
    return 0


def _to_movie_region_chunk(chunk_id, movie_fofn, rgn_fofn):
    _d = {Constants.CHUNK_KEY_MOVIE_FOFN: movie_fofn, Constants.CHUNK_KEY_RGN_FOFN: rgn_fofn}
    return PipelineChunk(chunk_id, **_d)


def chunk_movie_fofn(movie_fofn):
    movie_files = sorted(fofn_to_files(movie_fofn))
    chunks = []

    for i, movie_file in enumerate(movie_files):
        chunk_id = "chunk-{i}".format(i=i)
        nfiles = len(fofn_to_files(movie_file))
        _d = {Constants.CHUNK_KEY_MOVIE_FOFN: movie_file, 'nfofns': nfiles}
        c = PipelineChunk(chunk_id, **_d)
        chunks.append(c)

    return chunks


def chunk_movie_fofn_region_fofn_to_chunks(movie_fofn, rgn_fofn):
    # this should do something smarter
    movie_files = sorted(fofn_to_files(movie_fofn))
    rgn_files = sorted(fofn_to_files(rgn_fofn))

    chunks = []
    xs = zip(movie_files, rgn_files)
    for i, x in enumerate(xs):
        chunk_id = "chunk-{i}".format(i=i)
        movie_fofn, rgn_fofn = x
        c = _to_movie_region_chunk(chunk_id, movie_fofn, rgn_fofn)
        chunks.append(c)

    return chunks


def _chunk_movie_fofn_region_fofn(movie_fofn, region_fofn, output_json_file):
    chunks = chunk_movie_fofn_region_fofn_to_chunks(movie_fofn, region_fofn)
    IO.write_pipeline_chunks(chunks, output_json_file, "Movie and Region Fofn Chunks")
    return True


def _add_movie_rgn_fofn_options(p):
    add_input_movie_fofn_option(p)
    add_input_rgn_fofn_option(p)
    _add_common_chunk_options(p)
    return p


def _args_run_chunk_movie_rgn_fofn(args):
    _chunk_movie_fofn_region_fofn(args.movie_fofn, args.region_fofn, args.chunk_report_json)
    return 0


def _add_chunk_fasta_options(p):
    p = add_input_fasta_option(p)
    p = _add_common_chunk_options(p)
    return p


def _args_run_chunk_fasta(args):
    return CU.write_fasta_chunks_to_file(args.chunk_report_json, args.fasta, args.max_total_chunks, args.output_dir, "chunk_fa", 'fasta')


def _add_chunk_fastq_options(p):
    add_input_fastq_option(p)
    _add_common_chunk_options(p)
    return p


def _args_run_chunk_fastq(args):
    return CU.write_fastq_chunks_to_file(args.chunk_report_json, args.fasta, args.max_total_chunks, args.output_dir, "chunk_fq", 'fastq')


def _add_chunk_alignmentset_options(p):
    add_input_alignmentset_option(p)
    add_input_fasta_reference_option(p)
    _add_common_chunk_options(p)
    return p


def _args_run_chunk_alignmentset(args):
    return CU.write_alignmentset_chunks_to_file(args.chunk_report_json,
                                                args.alignmentset, args.fasta,
                                                args.max_total_chunks,
                                                args.output_dir,
                                                "chunk_alignmentset", 'xml')


def _add_chunk_csv_options(p):
    p = add_input_csv_option(p)
    p = _add_common_chunk_options(p)
    return p


def _args_run_chunk_csv(args):
    return CU.write_csv_chunks_to_file(args.chunk_report_json, args.csv, args.max_total_chunks, args.output_dir, "chunk", "csv")


def _add_movie_region_references_options(p):
    add_input_movie_fofn_option(p)
    add_input_rgn_fofn_option(p)
    add_input_fasta_reference_option(p)
    _add_common_chunk_options(p)
    return p


def _args_run_movie_region_reference(args):

    chunks = chunk_movie_fofn_region_fofn_to_chunks(args.movie_fofn, args.region_fofn)

    # The reference is consistent in all the chunks
    for chunk in chunks:
        chunk._datum['$chunk.reference_id'] = args.reference_fasta

    IO.write_pipeline_chunks(chunks, args.chunk_report_json, "Chunk by Movie, Region and Reference Fasta")
    return 0


def get_parser():
    desc = "Tool to create Chunk json files."
    p = U.get_base_pacbio_parser(__version__, desc)

    sp = p.add_subparsers(help="Subparser Commands")

    def builder(sid_, help_, opt_func_, exe_func_):
        return U.subparser_builder(sp, sid_, help_, opt_func_, exe_func_)

    builder("fofn", "Create a generic chunk.json from a FOFN.", _add_chunk_fofn_options, _args_chunk_fofn)

    builder("fasta", "Create a chunk.json from a Fasta file", _add_chunk_fasta_options, _args_run_chunk_fasta)

    builder("fastq", "Create a chunk.json from a Fastq file", _add_chunk_fastq_options, _args_run_chunk_fastq)

    builder("alignmentset",
            "Create a chunk.json from an AlignmentSet XML file",
            _add_chunk_alignmentset_options, _args_run_chunk_alignmentset)

    builder("csv", "Create a chunk.json CSV from a CSV file", _add_chunk_csv_options, _args_run_chunk_csv)

    builder("contig-ids", "Create a chunk.json from Fasta Contig Ids", _add_chunk_contig_ids_options, _args_chunk_contig_ids)

    builder('fofn-contig-ids', "Create scatter.chunk.json file with chunk Contig Ids FOFN", _add_chunk_contig_ids_fofn_options, _args_write_contig_id_fofn)

    builder('movie-rgn-fofn', "Create chunked Movie-Region FOFN", _add_movie_rgn_fofn_options, _args_run_chunk_movie_rgn_fofn)

    builder('align', "Create a chunked Movie-Region-Reference Chunked file.", _add_movie_region_references_options, _args_run_movie_region_reference)

    return p


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_parser()

    return main_runner_default(argv_[1:], parser, log)
