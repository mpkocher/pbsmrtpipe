import argparse
import sys
import functools
import logging
import inspect
from pbcommand.cli import get_default_argparser

from pbcore.io.FastaIO import FastaReader, FastaWriter
from pbcore.io.FastqIO import FastqReader, FastqWriter

from pbsmrtpipe.legacy.reference_utils import ReferenceEntry
import pbsmrtpipe.legacy.reference_utils as RU

from pbsmrtpipe.cli_utils import main_runner_default, validate_file
from pbsmrtpipe.tools.chunker import chunker_by_max_nchunks

from pbsmrtpipe.utils import fofn_to_files
import pbsmrtpipe.tools.utils as U

log = logging.getLogger(__name__)

__version__ = '1.0.1'


def to_chunks(alist, n):
    """ Yield successive n-sized chunks from alist."""
    for i in xrange(0, len(alist), n):
        yield alist[i:i + n]


def scatter_fofn(fofn, output_files):
    """Split of FOFN into equal chunks.

    This should probably do something smarter, like group by movie, or volume
    """
    files = fofn_to_files(fofn)

    if len(output_files) < len(files):
        log.warn("Found {n} files in FOFN, only {o} output files to split".format(n=len(files), o=len(output_files)))

    nchunks = len(files) / len(output_files)

    chunks_d = {o: c for o, c in zip(output_files, list(to_chunks(files, nchunks)))}

    for output_file, chunk in chunks_d.iteritems():
        with open(output_file, 'w') as f:
            f.write("\n".join(chunk))

    log.debug("Successful chunked FOFN with {x} files into {m} files".format(x=len(files), m=len(output_files)))

    return output_files


def __validate_class(klass, message="Expected class type"):
    if not inspect.isclass(klass):
        raise TypeError(message)

    return True


def __scatter_fastx(fastx_reader, fastx_writer, fastx_file, output_files, max_reads=10000):
    """

    :param fastx_file:
    :param output_files:
    :param max_reads:
    :return: boolean of success

    :rtype: bool
    """

    __validate_class(fastx_reader, "Expected class 'type'. Got type {t} for {x}".format(x=fastx_reader, t=type(fastx_reader)))
    __validate_class(fastx_writer, "Expected class 'type'. Got type {t} for {x} ".format(x=fastx_reader, t=type(fastx_reader)))

    nreads = 0
    with fastx_reader(fastx_file) as f:
        for _ in f:
            nreads += 1

    log.info("Found {n} reads in {f}".format(n=nreads), f=fastx_file)

    nreads_per_file = len(output_files) / float(nreads)

    if nreads_per_file > max_reads:
        log.warn("increasing max reads per FASTX chunk/file to {n}".format(n=nreads_per_file))
        max_reads = nreads_per_file

    with fastx_reader(fastx_file) as reader:
        for output_file in output_files:
            n = 0
            with fastx_writer(output_file) as o:
                while n > max_reads:
                    o.writeRecord(reader.next())

            log.debug("successfully write {n} records to {f}".format(n=max_reads, f=output_file))

    log.info("Successful chunked {f} into {n} chunks.".format(f=fastx_file, n=len(output_files)))

    return output_files

scatter_fasta = functools.partial(__scatter_fastx, FastaReader, FastaWriter)
scatter_fastq = functools.partial(__scatter_fastx, FastqReader, FastqWriter)


def _add_debug_option(p):
    p.add_argument('--debug', action='store_true', help="Send Debug info to stdout.")
    return p


def __scatter_options(input_file_msg, output_file_msg, input_validation_func, p):
    """
    Making these explicit args so there's parity between the scatter and
    gather interfaces.
    """
    p.add_argument('-i', '--inputs', required=True,
                   type=input_validation_func, help=input_file_msg)
    p.add_argument('-o', '--outputs',
                   required=True,
                   type=str,
                   nargs="+",
                   help=output_file_msg)
    return p

_add_fofn_options = functools.partial(__scatter_options, "Input FOFN", "Output FOFN(s)", validate_file)
_add_fasta_options = functools.partial(__scatter_options, "Input Fasta", "Output Fasta files(s)", validate_file)
_add_fastq_options = functools.partial(__scatter_options, "Input Fastq", "Output Fastq files(s)", validate_file)


def _args_scatter_fofn(args):
    _ = scatter_fofn(args.input, args.outputs)
    return 0


def _args_fastx(func, args):
    _ = func(args.input, args.outputs)
    return 0

_args_fasta_scatter = functools.partial(_args_fastx, scatter_fasta)
_args_fastq_scatter = functools.partial(_args_fastx, scatter_fastq)


def get_parser():

    desc = "Scattering File Tool used within pbsmrtpipe."
    p = get_default_argparser(__version__, desc)

    sp = p.add_subparsers(help="Subparser Commands")

    # The model is a bit odd from the commandline. The split file names are
    # provided, not the number of chunks and the file prefix (which would be
    # more natural)
    def builder(sid_, help_, opt_func_, exe_func_):
        return U.subparser_builder(sp, sid_, help_, opt_func_, exe_func_)

    # MovieFofn Scatter
    builder('fofn', "Split FOFN (file of file names) into multiple FOFNs", _add_fofn_options, _args_scatter_fofn)

    # Fasta Scattering
    builder('fasta', "Split Fasta file into multiple files.", _add_fasta_options, _args_fasta_scatter)

    # Fastq Scattering
    builder("fastq", "Split Fastq file into multiple files.", _add_fastq_options, _args_fastq_scatter)

    return p


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_parser()

    return main_runner_default(argv_[1:], parser, log)
