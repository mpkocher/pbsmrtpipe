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

from pbsmrtpipe.cli_utils import main_runner_default, validate_file
from pbsmrtpipe.cluster import ClusterTemplateRender, ClusterTemplate
from pbsmrtpipe.engine import run_command, backticks
from pbsmrtpipe.models import RunnableTask, ResourceTypes, TaskTypes
import pbsmrtpipe.bgraph as B
import pbsmrtpipe.pb_io as IO

import pbsmrtpipe.tools.utils as U

log = logging.getLogger(__name__)
slog = logging.getLogger('status.' + __name__)

__version__ = '1.0'


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

    for resource in runnable_task.resources:
        rtype = resource['resource_type']
        path = resource['path']
        try:
            cleanup_resource(rtype, path)
        except Exception as e:
            log.error("Error cleanup resource {r} -> {p}".format(r=rtype,p=path))

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

    ncmds = len(runnable_task.cmds)

    # so core dumps are written to the job dir
    os.chdir(output_dir)

    env_json = os.path.join(output_dir, 'env.json')

    IO.write_env_to_json(env_json)

    with open(task_stdout, 'w') as stdout_fh:
        with open(task_stderr, 'w') as stderr_fh:
            stdout_fh.write(repr(runnable_task) + "\n")
            stdout_fh.write("Created at {x} on {h}\n".format(x=datetime.datetime.now(), h=host))

            # Validate Inputs
            for input_file in runnable_task.input_files:
                if os.path.exists(input_file):
                    stdout_fh.write("Validated INPUT file '{i}\n".format(i=input_file))
                else:
                    err_msg = "Unable to find INPUT file '{i}".format(i=input_file)
                    stderr_fh.write(err_msg + "\n")
                    sys.stderr.write(err_msg + "\n")
                    break

            # Create resources if necessary
            if runnable_task.resources:
                create_tmp_resources_ignore_error(runnable_task.resources)

            # Run commands
            for i, cmd in enumerate(runnable_task.cmds):
                log.debug("Running command \n" + cmd)

                rcode, out, error, run_time = run_command(cmd, stdout_fh, stderr_fh, time_out=None)

                if rcode != 0:
                    err_msg = "Failed task {i} exit code {r} in {s:.2f} sec".format(i=runnable_task.task_id, r=rcode, s=run_time)
                    log.error(err_msg)
                    log.error(error)

                    stderr_fh.write(str(error) + "\n")
                    sys.stderr.write(str(error) + "\n")

                    stderr_fh.write(err_msg + "\n")
                    sys.stderr.write(err_msg + "\n")
                    break
                else:
                    stdout_fh.write("completed running cmd {i} of {n}. exit code {x} in {s:.2f} sec on host {h}\n".format(x=rcode, s=run_time, h=host, i=i+1, n=ncmds))

            smsg_ = "completed running commands. Exit code {i}".format(i=rcode)
            log.debug(smsg_)

            if rcode == 0:
                # Validate output files of a successful task.
                for output_file in runnable_task.output_files:
                    if os.path.exists(output_file):
                        stdout_fh.write("Successfully validated file '{o}'\n".format(o=output_file))
                    else:
                        err_msg = "Unable to find file '{x}'".format(x=output_file)
                        stderr_fh.write(err_msg + "\n")
                        stdout_fh.write(err_msg + "\n")
                        sys.stderr.write(err_msg + "\n")
                        rcode = -1

            total_run_time = time.time() - started_at
            warn_msg = ""
            r = B.to_task_report(host, runnable_task.task_id, total_run_time, rcode, err_msg, warn_msg)
            task_report_path = os.path.join(output_dir, 'task-report.json')
            msg = "Writing task id {i} task report to {r}".format(r=task_report_path, i=runnable_task.task_id)
            log.info(msg)
            stdout_fh.write(msg + "\n")
            B.write_task_report(r, task_report_path)
            stderr_fh.flush()
            stdout_fh.flush()

    # Cleanup resource files
    if not debug_mode and runnable_task.resources:
        try:
            cleanup_resources(runnable_task)
            log.debug("successfully cleaned up {n} resources.".format(n=len(runnable_task.resources)))
        except Exception as e:
            log.error(str(e))
            log.error("failed to successfully cleanup resources. {f}".format(f=runnable_task.resources))

    run_time = time.time() - started_at
    return rcode, run_time


def to_job_id(base_name, base_id):
    return ''.join(['job.', base_name, str(base_id), str(random.randint(10000, 99999))])


def to_random_job_id(base_name):
    return ''.join(['job.', str(random.randint(1000000, 10000000)), base_name])


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

    if runnable_task.task_type is TaskTypes.LOCAL:
        return run_task(runnable_task, output_dir, stdout_, stderr_, debug_mode)

    if runnable_task.cluster is None:
        log.warn("No cluster provided. Running task locally.")
        return run_task(runnable_task, output_dir, stdout_, stderr_, debug_mode)

    env_json = os.path.join(output_dir, 'env.json')
    IO.write_env_to_json(env_json)

    # sloppy API
    if isinstance(runnable_task.cluster, ClusterTemplateRender):
        render = runnable_task.cluster
    else:
        ctmpls = [ClusterTemplate(name, tmpl) for name, tmpl in runnable_task.cluster.iteritems()]
        render = ClusterTemplateRender(ctmpls)

    job_id = to_random_job_id(runnable_task.task_id)
    log.debug("Using job id {i}".format(i=job_id))

    qstdout = _to_p('cluster.stdout')
    qstderr = _to_p('cluster.stderr')
    qshell = _to_p('cluster.sh')

    rcmd_shell = _to_p('run.sh')

    # Task Manifest Runner output
    stdout = _to_p('stdout')
    stderr = _to_p('stderr')
    mstdout = _to_p('mstdout')
    mstderr = _to_p('mstderr')

    with open(qstdout, 'w+') as f:
        f.write("Creating cluster stdout for Job {i} {r}\n".format(i=job_id, r=runnable_task))

    debug_str = " --debug "
    _d = dict(t=task_manifest_path, o=stdout, e=stderr, d=debug_str, m=mstdout, n=mstderr)
    cmd = "pbtools-runner run {d} --task-stderr=\"{e}\" --task-stdout=\"{o}\" \"{t}\" > \"{m}\" 2> \"{n}\"".format(**_d)

    with open(rcmd_shell, 'w+') as x:
        x.write(cmd + "\n")

    # Make +x
    os.chmod(rcmd_shell, os.stat(rcmd_shell).st_mode | stat.S_IEXEC)

    cluster_cmd = render.render('interactive', rcmd_shell, job_id, qstdout, qstderr, runnable_task.nproc)
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
        # not sure how to scrape this from the stderr/stdout
        err_msg = "task {i} failed".format(i=runnable_task.task_id)
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

    r = B.to_task_report(host, runnable_task.task_id, run_time, rcode, err_msg, warn_msg)
    task_report_path = os.path.join(output_dir, 'task-report.json')
    msg = "Writing task id {i} task report to {r}".format(r=task_report_path, i=runnable_task.task_id)
    log.info(msg)
    B.write_task_report(r, task_report_path)

    return rcode, run_time


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
    print "\n".join(runnable_task.cmds)
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

