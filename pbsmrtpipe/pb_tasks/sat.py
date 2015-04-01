import logging
import os


from pbsmrtpipe.core import register_task
from pbsmrtpipe.models import TaskTypes, FileTypes


log = logging.getLogger(__name__)


@register_task("pbsmrtpipe.tasks.sat_report", TaskTypes.DISTRIBUTED,
               (FileTypes.ALIGNMENT_CMP_H5, FileTypes.REPORT, FileTypes.REPORT),
               FileTypes.REPORT, {}, 1, (),
               output_file_names=(('sat_report', 'json'),))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    e = 'pbreport.py sat'
    cmd = "{e} --debug {o} {j} {a} {v} {m}"
    sat_report = output_files[0]
    d = dict(e=e, o=os.path.dirname(sat_report.path),
             j=os.path.basename(sat_report.path),
             a=input_files[0],
             v=input_files[2],
             m=input_files[1])

    return cmd.format(**d)
