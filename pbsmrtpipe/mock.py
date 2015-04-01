import json
import logging
import os
import random

import pbsmrtpipe.pb_io as IO
import pbsmrtpipe.bgraph as B

from pbsmrtpipe.models import TaskStates


from pbsmrtpipe.models import RunnableTask


log = logging.getLogger(__name__)


def _load_env_preset(env_var):
    path = os.environ.get(env_var, None)

    if path is None:
        return None
    else:
        return IO.parse_pipeline_preset_xml(path)


def _mock_task_exe_runner(task_dir, output_files):

    log.debug("Running Mock TASK {d}".format(d=task_dir))

    stderr = os.path.join(task_dir, 'stderr')
    stdout = os.path.join(task_dir, 'stdout')

    def _write_mock_file(p, message="MOCK_FILE"):
        log.debug("Writing mock file {p}".format(p=p))
        with open(p, 'w') as w:
            w.write(message + "\n")

    for x in [stderr, stdout]:
        _write_mock_file(x)

    for mock_file in output_files:
        # just to make sure json files are well-formed
        if mock_file.endswith('.json'):
            d = dict(data="MOCK DATA")
            s = json.dumps(d, indent=4)
            _write_mock_file(mock_file, s)
        else:
            _write_mock_file(mock_file, "MOCK OUTPUT FILE")

    B.write_mock_task_report(task_dir)

    return TaskStates.SUCCESSFUL, "", random.randint(1, 1000)


def mock_run_task_manifest(path):
    output_dir = os.path.dirname(path)
    rt = RunnableTask.from_manifest_json(path)
    return _mock_task_exe_runner(output_dir, rt.output_files)
