import logging
import os

from pbsmrtpipe.models import SymbolTypes, FileTypes, TaskTypes
from pbsmrtpipe.core import register_task, register_scatter_task, register_gather_task

log = logging.getLogger(__name__)


def _scatter_filter_nchunks(max_chunks, nfofn):
    return min(nfofn, max_chunks)


@register_scatter_task('pbsmrtpipe.tasks.filter_scatter',
                       TaskTypes.LOCAL,
                       (FileTypes.MOVIE_FOFN, FileTypes.REPORT),
                       FileTypes.CHUNK, {}, 1, (),
                       [SymbolTypes.MAX_NCHUNKS, '$inputs.1.input_xml_report.nfofns', _scatter_filter_nchunks],
                       ['$chunk.rgn_id'],
                       output_file_names=(('filter_scatter', 'chunk.json'),))
def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):

    exe = "pbtools-chunker"
    _d = dict(e=exe, i=input_files[0], o=output_files[0], n=nchunks)
    return "{e} fofn --debug --max-total-chunks={n} {i} {o}".format(**_d)


@register_task('pbsmrtpipe.tasks.gather_csv',
               TaskTypes.DISTRIBUTED,
               FileTypes.CHUNK, FileTypes.CSV, {}, 1, (),
               output_file_names=(("gathered", 'csv'),))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    _d = dict(i=input_files[0], o=output_files[1])
    return 'pbtools-gather csv --chunk-key="csv_id" {i} --output="{o}"'.format(**_d)


@register_gather_task('pbsmrtpipe.tasks.gather_fofn',
                      TaskTypes.LOCAL,
                      FileTypes.CHUNK,
                      FileTypes.RGN_FOFN, {}, 1, (),
                      output_file_names=(('filtered_region', 'fofn'),))
def to_cmd(input_files, output_files, ropts, nproc, resources, chunk_key):
    cmds = []
    cmds.append('pbtools-gather fofn --chunk-key="rgn_fofn_id" {i} --output="{o}"'.format(i=input_files[0], o=output_files[0]))
    return cmds


@register_scatter_task('pbsmrtpipe.tasks.filter_subreads_scatter',
                       TaskTypes.LOCAL,
                       (FileTypes.MOVIE_FOFN, FileTypes.REPORT, FileTypes.RGN_FOFN, FileTypes.REPORT),
                       FileTypes.CHUNK, {}, 1, (),
                       [SymbolTypes.MAX_NCHUNKS, '$inputs.1.input_xml_report.nfofns', _scatter_filter_nchunks],
                       ['$chunk.csv_id'],
                       output_file_names=(('filter_subread_scatter', 'chunk.json'),))
def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
    # FIXME There's an assumption that the len of the fofn's is the same
    exe = 'pbtools-chunker movie-rgn-fofn'
    _d = dict(e=exe, i=input_files[0], n=nchunks, o=output_files[0], r=input_files[2])

    return "{e} --debug --max-total-chunks={n} {i} {r} {o}".format(**_d)


def _gather_fastx(fasta_or_fastq, chunk_id, input_file, output_file):
    _d = dict(x=fasta_or_fastq, c=chunk_id, i=input_file, o=output_file)
    return 'pbtools-gather {x} --debug --chunk-id="{c}" {i} {o}'.format(**_d)


@register_gather_task('pbsmrtpipe.tasks.gather_fasta', TaskTypes.DISTRIBUTED,
                      FileTypes.CHUNK,
                      FileTypes.FASTA, {}, 1, (),
                      output_file_names=(('filtered_subreads', 'fasta'),))
def to_cmd(input_files, output_files, ropts, nproc, resources, chunk_key):
    return _gather_fastx('fasta', 'fasta_id', input_files[0], output_files[0])


@register_gather_task('pbsmrtpipe.tasks.gather_fastq',
                      TaskTypes.DISTRIBUTED,
                      FileTypes.CHUNK,
                      FileTypes.FASTQ, {}, 1, (),
                      output_file_names=(('gathered', 'fastq'),))
def to_cmd(input_files, output_files, ropts, nproc, resources, chunk_key):
    return _gather_fastx('fastq', 'fastq_id', input_files[0], output_files[0])


@register_scatter_task('pbsmrtpipe.tasks.align_scatter',
                       TaskTypes.DISTRIBUTED,
                       (FileTypes.MOVIE_FOFN, FileTypes.RGN_FOFN, FileTypes.FASTA, FileTypes.REPORT),
                       FileTypes.CHUNK, {},
                       1, (),
                       [SymbolTypes.MAX_NCHUNKS, '$inputs.3.input_xml_report.nfofns', _scatter_filter_nchunks],
                       ['$chunk.movie_id', '$chunk.rgn_fofn_id', '$chunk.reference_id'],
                       output_file_names=(('align_scatter', 'chunks.json'),))
def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):

    exe = 'pbtools-chunker align'
    _d = dict(e=exe,
              i=input_files[0],
              r=input_files[2],
              f=input_files[3],
              d=os.path.dirname(output_files[0]),
              o=output_files[0],
              n=nchunks)
    # this is not quite right
    return "{e} --debug --max-total-chunks={n} --output-dir={d} {i} {r} {f} {o}".format(**_d)


@register_gather_task('pbsmrtpipe.tasks.gather_cmph5', TaskTypes.DISTRIBUTED,
                      FileTypes.CHUNK,
                      FileTypes.ALIGNMENT_CMP_H5, {}, 1, (),
                      output_file_names=(('gathered_alignment', 'cmp.h5'),))
def to_cmd(input_files, output_files, ropts, nproc, resources, chunk_key):

    exe = "pbtools-gather cmph5"
    _d = dict(e=exe, i=input_files[0], o=output_files[0])
    return "{e} --debug {i} {o}".format(**_d)


def _custom_nchunks_by_contig(max_chunks, ncontigs):
    # this isn't right
    return min(ncontigs, max_chunks)


@register_scatter_task('pbsmrtpipe.tasks.scatter_contig_ids',
                       TaskTypes.LOCAL,
                       (FileTypes.FASTA, FileTypes.REPORT, FileTypes.ALIGNMENT_CMP_H5),
                       FileTypes.CHUNK, {}, 1, (),
                       [SymbolTypes.MAX_NCHUNKS, '$inputs.0.ncontigs', _custom_nchunks_by_contig],
                       ['$chunk.fofn_id'],
                       output_file_names=(('contig_chunks.scatter', 'json'),))
def to_cmd(input_files, output_files, ropts, nproc, resources, nchunks):
    # this doesn't have the same signature as quiver task, but that's fine
    exe = "pbtools-chunker fofn-contig-ids"
    _d = dict(i=input_files[0], o=output_files[0], n=nchunks, e=exe, a=input_files[2])
    return "{e} --debug --total-max-chunks={n} {i} {a} {o}".format(**_d)


@register_gather_task('pbsmrtpipe.tasks.gather_gff', TaskTypes.DISTRIBUTED,
                      FileTypes.CHUNK,
                      FileTypes.GFF, {}, 1, (),
                      output_file_names=(('gathered', 'gff'),))
def to_cmd(input_files, output_files, ropts, nproc, resources, chunk_key):
    exe = 'pbtools-gather gff'
    c = 'gff_id'
    _d = dict(i=input_files[0], o=output_files[0], e=exe, c=c)
    return '{e} --debug --chunk_id="{c}" {o} {i}'.format(**_d)


@register_scatter_task("pbsmrtpipe.tasks.compute_modifications_scatter",
                       TaskTypes.DISTRIBUTED,
                       (FileTypes.FASTA, FileTypes.REPORT, FileTypes.ALIGNMENT_CMP_H5, FileTypes.FOFN),
                       FileTypes.CHUNK, {}, 1, (), 1, ("$chunk.gff_id", ))
def to_cmd(input_files, output_files, ropts, nproc, resources, chunk_key):
    return ""


@register_scatter_task("pbsmrtpipe.tasks.call_variants_with_fastx_scatter",
                       TaskTypes.DISTRIBUTED,
                       (FileTypes.FASTA, FileTypes.ALIGNMENT_CMP_H5, FileTypes.FOFN),
                       FileTypes.CHUNK, {}, 1, (), 1, ('$chunk.gff_id', ))
def to_cmd(input_files, output_files, ropts, nproc, resources, chunk_key):
    return ""
