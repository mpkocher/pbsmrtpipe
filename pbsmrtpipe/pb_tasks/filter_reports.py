import logging
import os

from pbsmrtpipe.core import register_task
from pbsmrtpipe.models import TaskTypes, FileTypes


log = logging.getLogger(__name__)


@register_task("pbsmrtpipe.tasks.filter_report",
               TaskTypes.DISTRIBUTED,
               (FileTypes.CSV, ),
               (FileTypes.REPORT, ), {}, 1, (),
               output_file_names=(('filter_reports_filter_stats', 'json'), ))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    """Generates histograms and summary stats for QV, classifier, and
    length, before and after filtering.
    """
    exe = "filter_stats.py"
    j = output_files[0]
    report_dir = os.path.dirname(j)
    _d = dict(e=exe,
              o=output_files[0],
              i=input_files[0],
              d=report_dir)
    cmd = "{e} --debug --output={d} --report={o} {i}".format(**_d)
    return cmd


@register_task("pbsmrtpipe.tasks.loading_report",
               TaskTypes.DISTRIBUTED,
               (FileTypes.CSV, ),
               (FileTypes.REPORT, ), {}, 1, (),
               output_file_names=(('loading_report', 'json'), ))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    """
    New implementation of loading report using pbreports.

    Generates a table exposing summary stats for primary's
    Productivity metric

    positional arguments:
        output      Output directory for associated files
        report      Filename of JSON output report. Should be name only,and will be
                      written to output dir
        csv         filtered_summary.csv

        optional arguments:
        -h, --help  show this help message and exit
        --debug     Enable debug output.
    """
    exe = "pbreport.py loading"

    # os.path should be removed
    report_dir = os.path.dirname(output_files[0])
    json_report = os.path.basename(output_files[0])

    _d = dict(e=exe, i=input_files[0], r=json_report, d=report_dir)
    cmd = "{e} --debug {d} {r} {i}".format(**_d)
    return cmd


@register_task("pbsmrtpipe.tasks.filter_subread_report",
               TaskTypes.DISTRIBUTED,
               (FileTypes.CSV, ),
               (FileTypes.REPORT, ), {}, 1, (),
               output_file_names=(('filter_reports_filter_subread_stats', 'json'), ))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    """
    Generate Histogram and a Subread Report from the
    filtered_subread_summary.csv

    """
    exe = "filter_subread.py"
    d = os.path.dirname(output_files[0])

    _d = dict(i=input_files[0], r=output_files[0], d=d, e=exe)
    cmd = "{e} --debug --report={r} --output={d} {i}".format(**_d)
    return cmd
