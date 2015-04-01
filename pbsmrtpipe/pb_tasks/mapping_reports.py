import os
import logging

from pbsmrtpipe.core import register_task
from pbsmrtpipe.models import TaskTypes, FileTypes


log = logging.getLogger(__name__)


def _to_cmd(mode, movie_fofn, region_fofn, alignment_file_type_or_fofn, report_json, debug=True):
    exe = "mapping_stats.py"
    debug_str = "--debug" if debug else " "
    cmd = "{e} {s} --output={d} {m} {b} {r} {a} {j}"

    _d = dict(e=exe,
              s=debug_str,
              m=mode,
              r=region_fofn,
              b=movie_fofn,
              a=alignment_file_type_or_fofn,
              j=report_json,
              d=os.path.dirname(report_json))

    return cmd.format(**_d)


@register_task("pbsmrtpipe.tasks.mapping_stats_report",
               TaskTypes.DISTRIBUTED,
               (FileTypes.MOVIE_FOFN, FileTypes.RGN_FOFN, FileTypes.ALIGNMENT_CMP_H5),
               (FileTypes.REPORT, ), {}, 1, (),
               output_file_names=(('mapping_stats_report', 'json'), ))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    """Create a Mapping stats report from a Alignment CMP.h5 file"""
    mode = "--mode external"
    return _to_cmd(mode, input_files[0], input_files[1], input_files[2], output_files[0])


@register_task("pbsmrtpipe.tasks.mapping_stats_bam_report",
               TaskTypes.DISTRIBUTED,
               (FileTypes.MOVIE_FOFN, FileTypes.RGN_FOFN, FileTypes.FOFN),
               FileTypes.REPORT, {}, 1, (),
               output_file_names=(("mapping_stats_bam_report", "json"),))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    """Create a Mapping stats from BAM Contig Fofn"""
    mode = '--mode external'
    return _to_cmd(mode, input_files[0], input_files[1], input_files[2], output_files[0])


@register_task("pbsmrtpipe.tasks.coverage_report",
               TaskTypes.DISTRIBUTED,
               (FileTypes.GFF, FileTypes.FASTA),
               (FileTypes.REPORT, ), {}, 1, (),
               output_file_names=(('mapping_converage_report', 'json'), ))
def to_cmd(input_files, output_file, ropts, nproc, resources):
    j = os.path.basename(output_file[0])
    # the readlink is resolve symlinked reference
    ref_dir = os.path.dirname(os.path.dirname(input_files[1]))
    exe = "pbreport.py coverage"
    cmd = "{e} --debug {d} {j} '{r}' {g}"
    _d = dict(e=exe,
              r=ref_dir,
              d=os.path.dirname(output_file[0]),
              g=input_files[0],
              j=j)
    return cmd.format(**_d)
