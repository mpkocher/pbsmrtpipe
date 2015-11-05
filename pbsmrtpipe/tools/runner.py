import os
import shutil
import stat
import pprint
import random
import sys
import logging
import time
import datetime
import functools
import platform
from pbcommand.cli import get_default_argparser
from pbcommand.models.report import Attribute, Report
from pbcommand.utils import which

from pbsmrtpipe.cli_utils import main_runner_default, validate_file
from pbsmrtpipe.cluster import ClusterTemplateRender, ClusterTemplate
from pbsmrtpipe.cluster import Constants as ClusterConstants
from pbsmrtpipe.engine import run_command, backticks
from pbsmrtpipe.models import RunnableTask, TaskStates
from pbcommand.models import ResourceTypes, TaskTypes
from pbsmrtpipe.utils import nfs_exists_check
import pbsmrtpipe.pb_io as IO

import pbsmrtpipe.tools.utils as U

log = logging.getLogger(__name__)
slog = logging.getLogger('status.' + __name__)

__version__ = '1.0.1'


def _resolve_exe(exe):
    """
    Try to resolve the abspath to the exe, default to the exe if not found
    in path"""
    x = which(exe)
    return exe if x is None else os.path.abspath(x)


def validate_file_and_load_manifest(path):
    rt = RunnableTask.from_manifest_json(validate_file(path))
    # if we got here everything is valid
    return path


def _add_manifest_json_option(p):
    d = "Path to task-manifest.json"
    p.add_argument('task_manifest', type=validate_file_and_load_manifest, help=d)
    return p


def _add_stderr_file(p):
    _d = "Stderr of exe'ed manifest task commands."
    p.add_argument('--task-stderr', type=str, required=True, help=_d)
    return p


def _add_stdout_file(p):
    _d = "Stdout of exe'ed manifest task commands."
    p.add_argument('--task-stdout', type=str, required=True, help=_d)
    return p


def _add_base_options(p):
    return _add_manifest_json_option(U.add_debug_option(p))


def _add_run_on_cluster_option(p):
    p.add_argument('--cluster', action='store_true', default=False,
                   help="Submit tasks to cluster if the cluster env is defined and task type is 'distributed.'")
    return p


def to_task_report(host, task_id, run_time_sec, exit_code, error_message, warning_message):
    # Move this somewhere that makes sense

    def to_a(idx, value):
        return Attribute(idx, value)

    datum = [('host', host),
             ('task_id', task_id),
             ('run_time', run_time_sec),
             ('exit_code', exit_code),
             ('error_msg', error_message),
             ('warning_msg', warning_message)]

    attributes = [to_a(i, v) for i, v in datum]
    r = Report("workflow_task", attributes=attributes)
    return r


def write_task_report(job_resources, task_id, path_to_report, report_images):
    """
    Copy image files to job html images dir, convert the task report to HTML

    :type job_resources: JobResources

    :param task_id:
    :param job_resources:
    :param path_to_report: abspath to the json pbreport
    :return:
    """
    report_html = os.path.join(job_resources.html, "{t}.html".format(t=task_id))
    task_image_dir = os.path.join(job_resources.images, task_id)
    if not os.path.exists(task_image_dir):
        os.mkdir(task_image_dir)

    shutil.copy(path_to_report, report_html)
    for image in report_images:
        shutil.copy(image, os.path.join(task_image_dir, os.path.basename(image)))

    log.debug("Completed writing {t} report".format(t=task_id))


def _create_tmp_file_resource(path):
    if not os.path.exists(path):
        with open(path, 'a'):
            os.utime(path, None)
        log.debug("Created resource {r} {p}".format(r=ResourceTypes.TMP_FILE, p=path))


def _create_tmp_dir_resource(path):
    if not os.path.exists(path):
        os.makedirs(path)
        log.debug("Created resource {r} {p}".format(r=ResourceTypes.TMP_DIR, p=path))


def create_tmp_resource(rtype, path):
    if rtype == ResourceTypes.TMP_FILE:
        _create_tmp_file_resource(path)

    if rtype == ResourceTypes.TMP_DIR:
        _create_tmp_dir_resource(path)


def create_tmp_resource_ignore_error(rtype, path):
    try:
        create_tmp_resource(rtype, path)
    except Exception as e:
        log.error("Failed to create resource type {t} -> '{p}'".format(t=rtype, p=path))
        log.error(e)


def create_tmp_resources_ignore_error(resources):
    for r in resources:
        rtype = r['resource_type']
        p = r['path']
        create_tmp_resource(rtype, p)


def _cleanup_resource_type(rtype, validation_func, remove_func, path):
    if rtype not in ResourceTypes.ALL():
        log.warn("Invalid resource type {x}. Ignoring resource {p}".format(x=rtype, p=path))
        return False
    if rtype in ResourceTypes.is_tmp_resource(rtype):
        if validation_func(path):
            remove_func(path)
        else:
            log.warn("Unable to find resource type {t} -> {p}".format(t=rtype, p=path))

    return True

cleanup_tmp_file = functools.partial(_cleanup_resource_type, ResourceTypes.TMP_FILE, os.path.isfile, os.remove)
cleanup_tmp_dir = functools.partial(_cleanup_resource_type, ResourceTypes.TMP_FILE, os.path.isfile, lambda x: shutil.rmtree(x, ignore_errors=True))


def cleanup_resource(rtype, path):
    if rtype == ResourceTypes.TMP_FILE:
        cleanup_tmp_file(path)
    if rtype == ResourceTypes.TMP_DIR:
        cleanup_tmp_dir(path)

    return True


def cleanup_resources(runnable_task):

    for resource in runnable_task.task.resources:
        rtype = resource['resource_type']
        path = resource['path']
        try:
            cleanup_resource(rtype, path)
        except Exception as e:
            log.error("Error cleanup resource {r} -> {p}".format(r=rtype, p=path))

    return True


def run_task(runnable_task, output_dir, task_stdout, task_stderr, debug_mode):
    """
    Run a runnable task locally.

    :param runnable_task:
    :type runnable_task: RunnableTask

    :return: exit code, run_time
    :rtype: (int, int)
    """
    started_at = time.time()

    rcode = -1
    err_msg = ""
    # host = socket.getfqdn()
    host = platform.node()

    ncmds = len(runnable_task.task.cmds)

    # so core dumps are written to the job dir
    os.chdir(output_dir)

    env_json = os.path.join(output_dir, '.env.json')

    IO.write_env_to_json(env_json)

    with open(task_stdout, 'w') as stdout_fh:
        with open(task_stderr, 'w') as stderr_fh:
            stdout_fh.write(repr(runnable_task) + "\n")
            stdout_fh.write("Created at {x} on {h}\n".format(x=datetime.datetime.now(), h=host))
            stderr_fh.write("Running task in {o}\n".format(o=output_dir))

            # Validate Inputs
            for input_file in runnable_task.task.input_files:
                if os.path.exists(input_file):
                    stdout_fh.write("Validated INPUT file '{i}\n".format(i=input_file))
                else:
                    err_msg = "Unable to find INPUT file '{i}".format(i=input_file)
                    stderr_fh.write(err_msg + "\n")
                    sys.stderr.write(err_msg + "\n")
                    break

            # Create resources if necessary
            #if runnable_task.task.resources:
            #    create_tmp_resources_ignore_error(runnable_task.task.resources)

            # Run commands
            for i, cmd in enumerate(runnable_task.task.cmds):
                log.debug("Running command \n" + cmd)

                rcode, out, error, run_time = run_command(cmd, stdout_fh, stderr_fh, time_out=None)

                if rcode != 0:
                    err_msg_ = "Failed task {i} exit code {r} in {s:.2f} sec (See file '{f}'.)".format(i=runnable_task.task.task_id, r=rcode, s=run_time, f=task_stderr)
                    t_error_msg = _extract_last_nlines(task_stderr)
                    err_msg = "\n".join([err_msg_, t_error_msg])
                    log.error(err_msg)
                    log.error(error)

                    stderr_fh.write(str(error) + "\n")
                    sys.stderr.write(str(error) + "\n")

                    stderr_fh.write(err_msg + "\n")
                    sys.stderr.write(err_msg + "\n")
                    break
                else:
                    stdout_fh.write("completed running cmd {i} of {n}. exit code {x} in {s:.2f} sec on host {h}\n".format(x=rcode, s=run_time, h=host, i=i + 1, n=ncmds))

            smsg_ = "completed running commands. Exit code {i}".format(i=rcode)
            log.debug(smsg_)

            if rcode == 0:
                # Validate output files of a successful task.
                for ix, output_file in enumerate(runnable_task.task.output_files):
                    if os.path.exists(output_file):
                        stdout_fh.write("Successfully validated {i} output file '{o}' on {h} \n".format(o=output_file, i=ix, h=host))
                    else:
                        err_msg = "Unable to find {i} output file '{x}'. Marking task as failed.".format(x=output_file, i=ix)
                        stderr_fh.write(err_msg + "\n")
                        stdout_fh.write(err_msg + "\n")
                        sys.stderr.write(err_msg + "\n")
                        rcode = -1

            total_run_time = time.time() - started_at
            # FIXME. There should be a better way to communicate warnings
            warn_msg = ""
            r = to_task_report(host, runnable_task.task.task_id, total_run_time, rcode, err_msg, warn_msg)
            task_report_path = os.path.join(output_dir, 'task-report.json')
            msg = "Writing task id {i} task report to {r}".format(r=task_report_path, i=runnable_task.task.task_id)
            log.info(msg)
            stdout_fh.write(msg + "\n")
            r.write_json(task_report_path)
            stderr_fh.flush()
            stdout_fh.flush()

    # Cleanup resource files
    if not debug_mode and runnable_task.task.resources:
        try:
            cleanup_resources(runnable_task)
            log.debug("successfully cleaned up {n} resources.".format(n=len(runnable_task.task.resources)))
        except Exception as e:
            log.error(str(e))
            log.error("failed to successfully cleanup resources. {f}".format(f=runnable_task.task.resources))

    run_time = time.time() - started_at
    return rcode, run_time


def to_job_id(base_name, base_id):
    return ''.join(['job.', base_name, str(base_id), str(random.randint(10000, 99999))])


def to_random_job_id(base_name):
    return ''.join(['job.', str(random.randint(1000000, 10000000)), base_name])


def _extract_last_nlines(path, nlines=25):
    """Attempt to extract the last nlines from a file

    If the file is not found or there's an error parsing the file,
    an empty string is returned.
    """
    try:
        n = nlines + 1
        nfs_exists_check(path)
        with open(path, 'r') as f:
            s = f.readlines()
            return "\n".join(s[-1: n])
    except Exception as e:
        log.exception("Unable to extract stderr from {p}. {e}".format(p=path, e=e))
        return ""


def run_task_on_cluster(runnable_task, task_manifest_path, output_dir, debug_mode):
    """

    :param runnable_task:
    :param output_dir:
    :param debug_mode:
    :return:

    :type runnable_task: RunnableTask
    """
    def _to_p(x_):
        return os.path.join(output_dir, x_)

    stdout_ = _to_p('stdout')
    stderr_ = _to_p('stderr')

    if runnable_task.task.is_distributed is False:
        return run_task(runnable_task, output_dir, stdout_, stderr_, debug_mode)

    if runnable_task.cluster is None:
        log.warn("No cluster provided. Running task locally.")
        return run_task(runnable_task, output_dir, stdout_, stderr_, debug_mode)

    os.chdir(runnable_task.task.output_dir)
    env_json = os.path.join(output_dir, '.cluster-env.json')
    IO.write_env_to_json(env_json)

    # sloppy API
    if isinstance(runnable_task.cluster, ClusterTemplateRender):
        render = runnable_task.cluster
    else:
        ctmpls = [ClusterTemplate(name, tmpl) for name, tmpl in runnable_task.cluster.iteritems()]
        render = ClusterTemplateRender(ctmpls)

    job_id = to_random_job_id(runnable_task.task.task_id)
    log.debug("Using job id {i}".format(i=job_id))

    qstdout = _to_p('cluster.stdout')
    qstderr = _to_p('cluster.stderr')
    qshell = _to_p('cluster.sh')

    rcmd_shell = _to_p('run.sh')

    # This needs to be flattened due to the new RTC layer
    # Task Manifest Runner output
    stdout = _to_p('stdout')
    stderr = _to_p('stderr')
    mstdout = _to_p('mstdout')
    mstderr = _to_p('mstderr')

    with open(qstdout, 'w+') as f:
        f.write("Creating cluster stdout for Job {i} {r}\n".format(i=job_id, r=runnable_task))

    debug_str = " --debug "
    exe = _resolve_exe("pbtools-runner")
    _d = dict(x=exe,
              t=task_manifest_path,
              o=stdout,
              e=stderr,
              d=debug_str,
              m=stdout,
              n=stderr,
              r=output_dir)

    cmd = "{x} run {d} --output-dir=\"{r}\" --task-stderr=\"{e}\" --task-stdout=\"{o}\" \"{t}\" > \"{m}\" 2> \"{n}\"".format(**_d)

    with open(rcmd_shell, 'w+') as x:
        x.write(cmd + "\n")

    # Make +x
    os.chmod(rcmd_shell, os.stat(rcmd_shell).st_mode | stat.S_IEXEC)

    cluster_cmd = render.render(ClusterConstants.START, rcmd_shell, job_id, qstdout, qstderr, runnable_task.task.nproc)
    log.debug(cluster_cmd)

    with open(qshell, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write(cluster_cmd + "\n")

    os.chmod(qshell, os.stat(qshell).st_mode | stat.S_IEXEC)

    # host = socket.getfqdn()
    host = platform.node()

    # so core dumps are written to the job dir
    os.chdir(output_dir)

    rcode, cstdout, cstderr, run_time = backticks("bash {q}".format(q=qshell))

    if rcode == 0:
        err_msg = ""
        warn_msg = ""
    else:
        p_err_msg = "task {i} failed (exit-code {x}) after {r:.2f} sec".format(i=runnable_task.task.task_id, r=run_time, x=rcode)
        raw_stderr = _extract_last_nlines(stderr)
        cluster_raw_stderr = _extract_last_nlines(cstderr)
        err_msg = "\n".join([p_err_msg, raw_stderr, cluster_raw_stderr])
        warn_msg = ""

    msg_ = "Completed running cluster command in {t:.2f} sec. Exit code {r}".format(r=rcode, t=run_time)
    log.info(msg_)

    with open(qstdout, 'a') as qf:
        qf.write(str(cstdout) + "\n")
        qf.write(msg_ + "\n")

    with open(qstderr, 'a') as f:
        if rcode != 0:
            f.write(str(cstderr) + "\n")
            f.write(msg_ + "\n")

    r = to_task_report(host, runnable_task.task.task_id, run_time, rcode, err_msg, warn_msg)
    task_report_path = os.path.join(output_dir, 'task-report.json')
    msg = "Writing task id {i} task report to {r}".format(r=task_report_path, i=runnable_task.task.task_id)
    log.info(msg)
    r.write_json(task_report_path)

    return rcode, run_time


def run_task_manifest(path):
    output_dir = os.path.dirname(path)
    os.chdir(output_dir)
    stderr = os.path.join(output_dir, 'stderr')
    stdout = os.path.join(output_dir, 'stdout')

    try:
        rt = RunnableTask.from_manifest_json(path)
    except KeyError:
        emsg = "Unable to deserialize RunnableTask from manifest {p}".format(p=path)
        log.error(emsg)
        raise

    rcode, run_time = run_task(rt, output_dir, stdout, stderr, True)

    state = TaskStates.SUCCESSFUL if rcode == 0 else TaskStates.FAILED
    msg = "" if rcode == 0 else "Failed with exit code {r}".format(r=rcode)

    return state, msg, run_time


def run_task_manifest_on_cluster(path):
    """
    Run the Task on the queue (of possible)

    :param path:
    :return:
    """
    output_dir = os.path.dirname(path)
    os.chdir(output_dir)
    rt = RunnableTask.from_manifest_json(path)

    rcode, run_time = run_task_on_cluster(rt, path, output_dir, True)

    state = TaskStates.SUCCESSFUL if rcode == 0 else TaskStates.FAILED
    msg = "{r} failed".format(r=rt) if rcode != 0 else ""

    return state, msg, run_time


def _args_run_task_manifest(args):
    output_dir = os.getcwd() if args.output_dir is None else args.output_dir
    task_manifest_path = args.task_manifest

    rt = RunnableTask.from_manifest_json(task_manifest_path)

    rcode, _ = run_task(rt, output_dir, args.task_stdout, args.task_stderr, args.debug)

    return rcode


def _add_run_options(p):
    _add_base_options(p)
    U.add_output_dir_option(p)
    _add_stdout_file(p)
    _add_stderr_file(p)
    return p


def run_to_cmd(runnable_task):
    """
    Extract the cmds from the json and print them to stdout

    :type runnable_task: RunnableTask
    """
    print "\n".join(runnable_task.task.cmds)
    return 0


def _args_to_cmd(args):
    return run_to_cmd(RunnableTask.from_manifest_json(args.task_manifest))


def pprint_task_manifest(runnable_task):
    print pprint.pformat(runnable_task.__dict__)
    return 0


def _args_pprint_task_manifest(args):
    return pprint_task_manifest(RunnableTask.from_manifest_json(args.task_manifest))


def get_main_parser():
    """
    Returns an argparse Parser with all the commandline utils as
    subparsers
    """
    desc = "General tool used by run task-manifests.json files."
    p = get_default_argparser(__version__, desc)

    sp = p.add_subparsers(help='Subparser Commands')

    def builder(sid_, help_, opt_func_, exe_func_):
        return U.subparser_builder(sp, sid_, help_, opt_func_, exe_func_)

    # Run command
    builder('run', "Convert a Pacbio Input.xml file to Movie FOFN", _add_run_options, _args_run_task_manifest)

    builder("to-cmds", "Extract the cmds from manifest.json", _add_manifest_json_option, _args_to_cmd)

    builder("inspect", "Pretty-Print a summary of the task-manifestExtract the cmds from manifest.json",
            _add_base_options, _args_pprint_task_manifest)

    return p


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_main_parser()

    return main_runner_default(argv_[1:], parser, log)
