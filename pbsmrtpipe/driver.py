import logging
import os
import time
import sys
import random
import multiprocessing
import json
import Queue
import pprint
import traceback
import types
import shutil
import functools
from collections import namedtuple
import uuid
from pbcore.io import DataSet

import pbsmrtpipe
from pbsmrtpipe import opts_graph as GX
from pbsmrtpipe.exceptions import PipelineRuntimeError, \
    PipelineRuntimeKeyboardInterrupt
import pbsmrtpipe.pb_io as IO
import pbsmrtpipe.bgraph as B
import pbsmrtpipe.cluster as C
import pbsmrtpipe.mock as M
import pbsmrtpipe.tools.runner as T
import pbsmrtpipe.report_renderer as R

from pbsmrtpipe.report_renderer import AnalysisLink
from pbsmrtpipe.bgraph import (TaskStates, DataStoreFile,
                               TaskBindingNode,
                               TaskChunkedBindingNode,
                               TaskScatterBindingNode,
                               EntryOutBindingFileNode)
from pbsmrtpipe.models import (FileTypes, RunnableTask, TaskTypes, Pipeline,
                               PipelineChunk, MetaStaticTask, MetaTask)
from pbsmrtpipe.utils import setup_log
from pbsmrtpipe.pb_io import WorkflowLevelOptions
from pbsmrtpipe.report_model import Attribute, Report, Table, Column, Plot, PlotGroup
import pbsmrtpipe.constants as GlobalConstants


log = logging.getLogger(__name__)
slog = logging.getLogger('status.' + __name__)

# logging.basicConfig(level=logging.DEBUG)

TaskResult = namedtuple('TaskResult', "task_id state error_message run_time_sec")


class Constants(object):
    SHUTDOWN = "SHUTDOWN"


def _log_pbsmrptipe_header():
    s = '''

       _                        _         _
      | |                      | |       (_)
 _ __ | |__  ___ _ __ ___  _ __| |_ _ __  _ _ __   ___
| '_ \| '_ \/ __| '_ ` _ \| '__| __| '_ \| | '_ \ / _ \\
| |_) | |_) \__ \ | | | | | |  | |_| |_) | | |_) |  __/
| .__/|_.__/|___/_| |_| |_|_|   \__| .__/|_| .__/ \___|
| |                                | |     | |
|_|                                |_|     |_|

'''
    return s


def _to_table(tid, bg, nodes):
    """Create a table from File nodes or Entry nodes"""
    columns = [Column('id', header="Id"),
               Column('is_resolved', header='Is Resolved'),
               Column('path', header="Path")]

    table = Table(tid, columns=columns)
    for node in nodes:
        table.add_data_by_column_id('id', str(node))
        table.add_data_by_column_id('is_resolved', bg.node[node]['is_resolved'])
        try:
            table.add_data_by_column_id('path', bg.node[node]['path'])
        except KeyError as e:
            slog.error("Failed to get path from {n}".format(n=repr(node)))
            slog.error(e)
            table.add_data_by_column_id('path', "NA")

    return table


def _to_report(bg, job_output_dir, job_id, state, was_successful, run_time, error_message=None):
    """ High Level Report of the workflow state

    Write the output of workflow datastore to pbreports report object

    Workflow summary .dot/svg (collapsed workflow)
    Workflow details .dot/svg (chunked workflow)

    To add:
    - Resolved WorkflowSettings (e.g., nproc, max_workers)
    -

    :type bg: BindingsGraph

    """
    emsg = "" if error_message is None else error_message

    attributes = [Attribute('was_successful', was_successful, name="Was Successful"),
                  Attribute('total_run_time_sec', int(run_time), name="Walltime (sec)"),
                  Attribute('error_message', emsg, name="Error Message"),
                  Attribute('job_id', job_id, name="Job Id"),
                  Attribute('job_state', state, name="Job State"),
                  Attribute('job_output_dir', job_output_dir, name="Job Output Directory"),
                  Attribute('pbsmrtpipe_version', pbsmrtpipe.get_version(), name="pbsmrtpipe Version")]

    columns = [Column('task_id', header='Task id'),
               Column('was_successful', header='Was Successful'),
               Column('state', header="Task State"),
               Column('run_time_sec', header="Run Time (sec)"),
               Column('nproc', header="# of procs")]

    tasks_table = Table('tasks', columns=columns)
    for tnode in bg.task_nodes():
        tasks_table.add_data_by_column_id('task_id', str(tnode))
        tasks_table.add_data_by_column_id('nproc', bg.node[tnode]['nproc'])
        tasks_table.add_data_by_column_id('state', bg.node[tnode]['state'])
        tasks_table.add_data_by_column_id('was_successful', bg.node[tnode]['state'] == TaskStates.SUCCESSFUL)
        # rt_ = bg.node[tnode]['run_time']
        # rtime = None if rt_ is None else int(rt_)
        tasks_table.add_data_by_column_id('run_time_sec', bg.node[tnode]['run_time'])

    ep_table = _to_table("entry_points", bg, bg.entry_binding_nodes())
    fnodes_table = _to_table("file_node", bg, bg.file_nodes())

    report = Report('pbsmrtpipe', tables=[tasks_table, ep_table, fnodes_table],
                    attributes=attributes)
    return report


def _dict_to_report_table(table_id, key_attr, value_attr, d):
    """
    General {k->v} to create a pbreport Table

    :param table_id: Table id
    :param key_attr: Column id
    :param value_attr: Column id
    :param d: dict
    :return:
    """
    columns = [Column(key_attr, header="Attribute"),
               Column(value_attr, header="Value")]

    table = Table(table_id, columns=columns)
    for k, v in d.iteritems():
        table.add_data_by_column_id(key_attr, k)
        table.add_data_by_column_id(value_attr, v)

    return table


def _workflow_opts_to_table(workflow_opts):
    """
    :type workflow_opts: WorkflowLevelOptions
    :param workflow_opts:
    :return:
    """
    tid = "workflow_options"
    wattr = "workflow_attribute"
    wvalue = "workflow_value"
    return _dict_to_report_table(tid, wattr, wvalue, workflow_opts.to_dict())


def _task_opts_to_table(task_opts):
    tattr = "task_attribute_id"
    vattr = "task_value"
    # FIXME this is not being generated correctly
    return _dict_to_report_table("task_options", tattr, vattr, task_opts)


def _to_workflow_settings_report(bg, workflow_opts, task_opts, state, was_successful):

    tables = [_workflow_opts_to_table(workflow_opts), _task_opts_to_table(task_opts)]
    report = Report("workflow_settings_report", tables=tables)
    return report


def _to_task_summary_report(bg):

    cs = [Column("workflow_task_id", header="Task Id"),
          Column("workflow_task_status", header="Status"),
          Column("workflow_task_run_time", header="Task Runtime"),
          Column('workflow_task_nproc', header="Number of Procs"),
          Column("workflow_task_emsg", header="Error Message")]

    t = Table("workflow_task_summary", title="Task Summary", columns=cs)
    for tnode in bg.task_nodes():
        if isinstance(tnode, TaskBindingNode):
            t.add_data_by_column_id("workflow_task_id", tnode.idx)
            t.add_data_by_column_id("workflow_task_status", bg.node[tnode]['state'])
            t.add_data_by_column_id("workflow_task_run_time", bg.node[tnode]['run_time'])
            t.add_data_by_column_id("workflow_task_nproc", bg.node[tnode]['nproc'])
            t.add_data_by_column_id("workflow_task_emsg", bg.node[tnode]['error_message'])

    return Report("workflow_task_summary", tables=[t])


def _to_workflow_report(job_resources, bg, workflow_opts, task_opts, state, was_successful, plot_images):
    """
    Copy images to image local directory and return a pbreport Report

    """
    plot_groups = []
    if plot_images:
        plots = []
        for i, plot_image in enumerate(plot_images):
            html_image_abs = os.path.join(job_resources.images, os.path.basename(plot_image))
            shutil.copy(plot_image, html_image_abs)
            # Make the file path relative to images/my-image.png
            html_image = os.path.join(os.path.basename(job_resources.images), os.path.basename(plot_image))
            p = Plot("plot_{i}".format(i=i), html_image)
            plots.append(p)

        pg = PlotGroup("workflow_state_plots", plots=plots)
        plot_groups.append(pg)

    return Report("workflow_report", plotgroups=plot_groups)


def datastore_to_report(ds):
    """

    :type ds: DataStore
    :param ds:
    :return:
    """
    attrs = [Attribute("ds_nfiles", len(ds.files), name="Number of files"),
             Attribute("ds_version", ds.version, name="Datastore version"),
             Attribute("ds_created_at", ds.created_at, name="Created At"),
             Attribute("ds_updated_at", ds.updated_at, name="Updated At")]

    columns_names = [("file_id", "File Id"),
                     ("file_type_obj", "File Type"),
                     ("path", "Path"),
                     ("file_size", "Size"),
                     ("created_at", "Created At"),
                     ("modified_at", "Modified At")]

    to_i = lambda s: "ds_" + s
    columns = [Column(to_i(i), header=h) for i, h in columns_names]
    t = Table("datastore", title="DataStore Summary", columns=columns)

    def _to_relative_path(p):
        return "/".join(p.split("/")[-3:])

    for file_id, ds_file in ds.files.iteritems():
        t.add_data_by_column_id(to_i("file_id"), ds_file.file_id)
        t.add_data_by_column_id(to_i("file_type_obj"), ds_file.file_type_id)
        t.add_data_by_column_id(to_i("path"), _to_relative_path(ds_file.path))
        t.add_data_by_column_id(to_i("file_size"), ds_file.file_size)
        t.add_data_by_column_id(to_i("created_at"), ds_file.created_at)
        t.add_data_by_column_id(to_i("modified_at"), ds_file.modified_at)

    return Report("datastore_report", tables=[t], attributes=attrs)


def _get_images_in_dir(dir_name, formats=(".png", ".svg")):
    # report plots only support
    return [os.path.join(dir_name, i_) for i_ in os.listdir(dir_name) if any(i_.endswith(x) for x in formats)]


def write_main_workflow_report(job_id, job_resources, workflow_opts, task_opts, bg_, state_, was_successful_, run_time_sec):
    """
    Write the main workflow level report.

    :type job_resources: JobResources
    :type workflow_opts: WorkflowLevelOptions
    :type bg_: BindingsGraph
    :type was_successful_: bool
    :type run_time_sec: float

    :param job_id:
    :param job_resources:
    :param workflow_opts:
    :param task_opts:
    :param bg_:
    :param state_:
    :param was_successful_:
    :param run_time_sec:
    :return:
    """
    # workflow_json = os.path.join(job_resources.workflow, 'workflow.json')
    # with open(workflow_json, 'w+') as f:
    #     f.write(json.dumps(json_graph.node_link_data(bg_)))

    report_path = os.path.join(job_resources.workflow, 'report-tasks.json')
    report_ = _to_report(bg_, job_resources.root, job_id, state_, was_successful_, run_time_sec)
    report_.write_json(report_path)
    R.write_report_with_html_extras(report_, os.path.join(job_resources.html, 'index.html'))

    setting_report = _to_workflow_settings_report(bg_, workflow_opts, task_opts, state_, was_successful_)
    R.write_report_to_html(setting_report, os.path.join(job_resources.html, 'settings.html'))

    setting_report = _to_workflow_report(job_resources, bg_, workflow_opts, task_opts, state_, was_successful_, _get_images_in_dir(job_resources.workflow, formats=(".svg",)))
    R.write_report_to_html(setting_report, os.path.join(job_resources.html, 'workflow.html'))


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


class TaskManifestWorker(multiprocessing.Process):

    def __init__(self, q_out, event, sleep_time, run_manifest_func, task_id, manifest_path, group=None, name=None, target=None):
        self.q_out = q_out
        self.event = event
        self.sleep_time = sleep_time
        self.task_id = task_id
        self.manifest_path = manifest_path

        # runner func (path/to/manifest.json ->) (task_id, state, message, run_time)
        self.runner_func = run_manifest_func
        super(TaskManifestWorker, self).__init__(group=group, name=name, target=target)

    def shutdown(self):
        self.event.set()

    def run(self):
        log.info("Starting process:{p} {k} worker {i} task id {t}".format(k=self.__class__.__name__, i=self.name, t=self.task_id, p=self.pid))

        try:
            if os.path.exists(self.manifest_path):
                log.debug("Running task {i} with func {f}".format(i=self.task_id, f=self.runner_func.__name__))
                state, msg, run_time = self.runner_func(self.manifest_path)
                self.q_out.put(TaskResult(self.task_id, state, msg, round(run_time, 2)))
            else:
                emsg = "Unable to find manifest {p}".format(p=self.manifest_path)
                run_time = 1
                self.q_out.put(TaskResult(self.task_id, "failed", emsg, round(run_time, 2)))
        except Exception as ex:
            traceback.print_exc(file=sys.stderr)
            emsg = "Unhandled exception in Worker {n} running task {i}. Exception {e}".format(n=self.name, i=self.task_id, e=ex.message)
            log.error(emsg)
            self.q_out.put(TaskResult(self.task_id, "failed", emsg, 0.0))

        log.info("exiting Worker {i} (pid {p}) {k}.run".format(k=self.__class__.__name__, i=self.name, p=self.pid))
        return True


def _job_resource_create_and_setup_logs(job_root_dir, bg, task_opts, workflow_level_opts, ep_d):
    """
    Create job resource dirs and setup log handlers

    :type job_root_dir: str
    :type bg: BindingsGraph
    :type task_opts: dict
    :type workflow_level_opts: WorkflowLevelOptions
    :type ep_d: dict
    """

    job_resources = B.to_job_resources_and_create_dirs(job_root_dir)

    setup_log(log, level=logging.INFO, file_name=os.path.join(job_resources.logs, 'pbsmrtpipe.log'))
    setup_log(log, level=logging.DEBUG, file_name=os.path.join(job_resources.logs, 'master.log'))

    log.info("Starting pbsmrtpipe v{v}".format(v=pbsmrtpipe.get_version()))
    log.info("\n" + _log_pbsmrptipe_header())

    B.write_binding_graph_images(bg, job_resources.workflow)

    B.write_entry_points_json(job_resources.entry_points_json, ep_d)

    # Need to map entry points to a FileType
    ds = B.write_and_initialize_data_store_json(job_resources.datastore_json, [])
    slog.info("successfully initialized datastore.")

    B.write_workflow_settings(workflow_level_opts, os.path.join(job_resources.workflow, 'options-workflow.json'))
    log.info("Workflow Options:")
    log.info(pprint.pformat(workflow_level_opts.to_dict(), indent=4))

    task_opts_path = os.path.join(job_resources.workflow, 'options-task.json')
    with open(task_opts_path, 'w') as f:
        f.write(json.dumps(task_opts, sort_keys=True, indent=4))

    env_path = os.path.join(job_resources.workflow, 'env.json')
    IO.write_env_to_json(env_path)

    try:
        sa_system, sa_components = IO.get_smrtanalysis_system_and_components_from_env()
        log.info(sa_system)
        for c in sa_components:
            log.info(c)
    except Exception:
        # black hole exception
        log.warn("unable to determine SMRT Analysis version.")
        pass

    slog.info("completed setting up job directory resources and logs in {r}".format(r=job_root_dir))
    return job_resources, ds


def write_task_manifest(manifest_path, tid, task, resource_types, task_version, python_mode_str, cluster_renderer):
    """

    :type manifest_path: str
    :type tid: str
    :type task: Task
    :type task_version: str
    :type python_mode_str: str
    :type cluster_renderer: ClusterTemplateRender | None

    :return:
    """
    env = []
    resources_list_d = [dict(resource_type=r, path=p) for r, p in zip(resource_types, task.resources)]

    task_manifest = os.path.join(manifest_path)
    with open(task_manifest, 'w+') as f:
        # this version should be the global pbsmrtpipe version
        # or the manifest file spec version?
        m = B.to_manifest_d(tid,
                            task,
                            resources_list_d,
                            env,
                            cluster_renderer,
                            python_mode_str,
                            task_version)

        f.write(json.dumps(m, sort_keys=True, indent=2))

    log.debug("wrote task id {i} to {p}".format(i=tid, p=manifest_path))
    return True


def _init_bg(bg, ep_d):
    # Help initialize graph/epoints
    B.resolve_entry_points(bg, ep_d)
    # Update Task-esque EntryBindingPoint
    B.resolve_entry_binding_points(bg)

    # initialization File node attributes
    for eid, path in ep_d.iteritems():
        B.resolve_entry_point(bg, eid, path)
        B.resolve_successor_binding_file_path(bg)

    return bg


def run_task_manifest(path):
    output_dir = os.path.dirname(path)
    stderr = os.path.join(output_dir, 'stderr')
    stdout = os.path.join(output_dir, 'stdout')

    try:
        rt = RunnableTask.from_manifest_json(path)
    except KeyError:
        emsg = "Unable to deserialize RunnableTask from manifest {p}".format(p=path)
        log.error(emsg)
        raise

    rcode, run_time = T.run_task(rt, output_dir, stdout, stderr, True)

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
    rt = RunnableTask.from_manifest_json(path)

    rcode, run_time = T.run_task_on_cluster(rt, path, output_dir, True)

    state = TaskStates.SUCCESSFUL if rcode == 0 else TaskStates.FAILED
    msg = "{r} failed".format(r=rt) if rcode != 0 else ""

    return state, msg, run_time


def _get_scatterable_task_id(chunk_operators_d, task_id):
    """Get the companion scatterable task id from the original meta task id"""
    for operator_id, chunk_operator in chunk_operators_d.iteritems():
        if task_id == chunk_operator.scatter.task_id:
            return chunk_operator.scatter.scatter_task_id
    return None


def _is_task_id_scatterable(chunk_operators_d, task_id):
    return _get_scatterable_task_id(chunk_operators_d, task_id) is not None


def _get_chunk_operator_by_scatter_task_id(scatter_task_id, chunk_operators_d):
    for operator_id, chunk_operator in chunk_operators_d.iteritems():
        if scatter_task_id == chunk_operator.scatter.scatter_task_id:
            return chunk_operator

    raise KeyError("Unable to find chunk operator for scatter task id {i}".format(i=scatter_task_id))


def _status(bg):
    """
    Create a terse status messages from the overall status of the graph

    :type bg: `pbsmrtpipe.bgraph.BindingsGraph`
    :param bg:
    :return:
    """
    ntasks = len(bg.task_nodes())
    ncompleted_tasks = len(B.get_tasks_by_state(bg, TaskStates.SUCCESSFUL))
    return "Workflow status {n}/{t} completed/total tasks".format(t=ntasks, n=ncompleted_tasks)


def _get_dataset_uuid_or_create(path):
    """
    Extract the uuid from the DataSet or assign a new UUID

    :param path: Path to file

    :rtype: str
    :return: uuid string
    """
    try:
        ds = DataSet(path)
        ds_id = ds.uuid
        # make sure it's a validate uuid
        _ = uuid.UUID(ds_id)
    except ValueError as e:
        log.error("DataSet {p} uuid is malformed. {e}".format(e=e, p=path))
        ds_id = uuid.uuid4
    except Exception:
        # not a DataSet file
        ds_id = uuid.uuid4()

    return ds_id



def _get_last_lines_of_stderr(n, stderr_path):
    lines = []
    if os.path.exists(stderr_path):
        with open(stderr_path, 'r') as f:
            lines = f.readlines()

    x = min(n, len(lines))
    return lines[-x:]


def _terminate_all_workers(workers, shutdown_event):
    if workers:
        nworkers = len(workers)
        log.info("terminating {n} workers.".format(n=nworkers))
        shutdown_event.set()
        # this is brutal terminate
        for worker in workers:
            log.debug("terminating worker {n}".format(n=worker.name))
            worker.terminate()
            # time.sleep(0.25)

        log.info("successfully terminated {n} workers.".format(n=nworkers))


def _terminate_worker(worker):
    """
    :type worker: TaskManifestWorker
    """
    tid = worker.task_id
    name = worker.name
    pid_ = worker.pid
    try:
        worker.terminate()
    except Exception as e:
        log.error("Failed to terminate worker {n} task-id:{i} Pid {p}. {c} {e}".format(n=name, i=tid, p=pid_, e=e.message, c=e.__class__))


def _are_workers_alive(workers):
    return all(w.is_alive() for w in workers.values())


def __exe_workflow(chunk_operators_d, ep_d, bg, task_opts, workflow_opts, output_dir, registered_tasks_d, registered_file_types, cluster_renderer, task_manifest_runner_func, task_manifest_cluster_runner_func, workers, shutdown_event):
    """

    :type bg: BindingsGraph
    :type cluster_renderer: ClusterTemplateRender | None
    :type workflow_opts: WorkflowLevelOptions
    :type output_dir: str

    :param workers: {taskid:Worker}
    :return:

    The function is doing way too much.
    """
    job_id = random.randint(100000, 999999)
    m_ = "Distributed" if workflow_opts.distributed_mode is not None else "Local"
    slog.info("starting to execute {m} workflow with assigned job_id {i}".format(i=job_id, m=m_))
    log.info("exe'ing workflow Cluster renderer {c}".format(c=cluster_renderer))
    started_at = time.time()

    # To store all the reports that are displayed in the analysis.html
    # {id:task-id, report_path:path/to/report.json}
    analysis_file_links = []

    def write_analysis_report(analysis_file_links_):
        analysis_report_html = os.path.join(job_resources.html, 'analysis.html')
        R.write_analysis_link_report(analysis_file_links_, analysis_report_html)

    def update_analysis_file_links(task_id_, report_path_):
        analysis_link = AnalysisLink(task_id_, report_path_)
        log.info("updating report analysis file links {a}".format(a=analysis_link))
        analysis_file_links.append(analysis_link)
        write_analysis_report(analysis_file_links)

    def _to_run_time():
        return time.time() - started_at

    # Help initialize graph/epoints
    B.resolve_entry_points(bg, ep_d)
    # Update Task-esque EntryBindingPoint
    B.resolve_entry_binding_points(bg)

    # initialization File node attributes
    for eid, path in ep_d.iteritems():
        B.resolve_entry_point(bg, eid, path)
        B.resolve_successor_binding_file_path(bg)

    B.label_chunkable_tasks(bg, chunk_operators_d)

    slog.info("validating binding graph")
    B.validate_binding_graph_integrity(bg)
    slog.info("successfully validated binding graph.")

    log.info(B.to_binding_graph_summary(bg))

    # file type id counter {str: int}
    file_type_id_to_count = {file_type.file_type_id: 0 for _, file_type in registered_file_types.iteritems()}

    # Create a closure to allow file types to assign unique id
    # this returns a func
    to_resolve_files_func = B.to_resolve_files(file_type_id_to_count)

    q_out = multiprocessing.Queue()

    worker_sleep_time = 1

    # local factory funcs
    def _to_w(wid, task_id, manifest_path_):
        return TaskManifestWorker(q_out, shutdown_event, worker_sleep_time, task_manifest_cluster_runner_func, task_id, manifest_path_, name=wid)

    def _to_local_w(wid, task_id, manifest_path_):
        return TaskManifestWorker(q_out, shutdown_event, worker_sleep_time, task_manifest_runner_func, task_id, manifest_path_, name=wid)

    max_total_nproc = workflow_opts.total_max_nproc
    max_nworkers = workflow_opts.max_nworkers
    max_nproc = workflow_opts.max_nproc
    max_nchunks = workflow_opts.max_nchunks

    # Setup logger, job directory and initialize DS
    slog.info("creating jobs resources in {o}".format(o=output_dir))
    job_resources, ds = _job_resource_create_and_setup_logs(output_dir, bg, task_opts, workflow_opts, ep_d)
    slog.info("successfully created job resources.")

    def write_report_(bg_, current_state_, was_successful_):
        return write_main_workflow_report(job_id, job_resources, workflow_opts, task_opts, bg_, current_state_, was_successful_, _to_run_time())

    def write_task_summary_report(bg_):
        task_summary_report = _to_task_summary_report(bg_)
        p = os.path.join(job_resources.html, 'task_summary.html')
        R.write_report_to_html(task_summary_report, p)

    def is_task_id_scatterable(task_id_):
        return _is_task_id_scatterable(chunk_operators_d, task_id_)

    def get_scatter_task_id_from_task_id(task_id_):
        return _get_scatterable_task_id(chunk_operators_d, task_id_)

    has_failed = False
    sleep_time = 1
    # Running total of current number of slots/cpu's used
    total_nproc = 0

    def has_available_slots(n):
        if max_total_nproc is None:
            return True
        return total_nproc + n <= max_total_nproc

    write_report_(bg, TaskStates.CREATED, False)
    # write empty analysis reports
    write_analysis_report(analysis_file_links)

    # For bookkeeping
    # task id -> tnode
    tid_to_tnode = {}
    # tnode -> Task instance
    tnode_to_task = {}

    try:
        log.debug("Starting execution loop... in process {p}".format(p=os.getpid()))

        while True:

            # This will add new nodes to the graph if necessary
            B.apply_chunk_operator(bg, chunk_operators_d, registered_tasks_d)
            B.write_binding_graph_images(bg, job_resources.workflow)
            B.add_gather_to_completed_task_chunks(bg, chunk_operators_d, registered_tasks_d)
            B.write_binding_graph_images(bg, job_resources.workflow)

            if not _are_workers_alive(workers):
                for w_ in workers.values():
                    if not w_.is_alive():
                        log.warn("Worker {i} (pid {p}) is not alive. Worker exit code {e}.".format(i=w_.name, p=w_.pid, e=w_.exitcode))

            # Check if Any tasks are running or that there still runnable tasks
            is_completed = B.is_workflow_complete(bg)
            has_task_running = B.has_running_task(bg)

            if not is_completed:
                if not has_task_running:
                    if not B.has_task_in_states(bg, (TaskStates.CREATED, TaskStates.READY)):
                        if not B.has_next_runnable_task(bg):
                            msg = "Unable to find runnable task or any tasks running and workflow is NOT completed."
                            log.error(msg)
                            log.error(B.to_binding_graph_summary(bg))
                            raise PipelineRuntimeError(msg)

            time.sleep(sleep_time)
            # log.debug("Sleeping for {s}".format(s=sleep_time))
            log.debug("\n" + B.to_binding_graph_summary(bg))
            # B.to_binding_graph_task_summary(bg)

            write_report_(bg, TaskStates.RUNNING, is_completed)

            if is_completed:
                log.info("Workflow is completed. breaking out.")
                break

            if len(workers) >= max_nworkers:
                # don't do anything
                continue

            try:
                result = q_out.get_nowait()
            except Queue.Empty:
                result = None

            # log.info("Results {r}".format(r=result))
            if isinstance(result, TaskResult):
                log.debug("Task result {r}".format(r=result))

                tid_, state_, msg_, run_time_ = result
                tnode_ = tid_to_tnode[tid_]
                task_ = tnode_to_task[tnode_]

                # Process Successful Task Result
                if state_ == TaskStates.SUCCESSFUL:
                    slog.info("Task was successful {r}".format(r=result))

                    is_valid_or_emsg = B.validate_outputs_and_update_task_to_success(bg, tnode_, run_time_, bg.node[tnode_]['task'].output_files)

                    # Failed to find output files
                    if is_valid_or_emsg is not True:
                        B.update_task_state_to_failed(bg, tnode_, result.run_time_sec, result.error_message)
                        raise PipelineRuntimeError(is_valid_or_emsg)

                    B.update_task_output_file_nodes(bg, tnode_, tnode_to_task[tnode_])
                    B.resolve_successor_binding_file_path(bg)
                    total_nproc -= task_.nproc
                    w_ = workers.pop(tid_)
                    _terminate_worker(w_)

                    # Update Analysis Reports and Register output files to Datastore
                    for file_type_, path_ in zip(tnode_.meta_task.output_types, task_.output_files):
                        source_id = "{t}-{f}".format(t=task_.task_id, f=file_type_.file_type_id)
                        ds_uuid = _get_dataset_uuid_or_create(path_)
                        ds.add(DataStoreFile(ds_uuid, source_id, file_type_.file_type_id, path_))
                        ds.write_update_json(job_resources.datastore_json)
                        dsr = datastore_to_report(ds)
                        R.write_report_to_html(dsr, os.path.join(job_resources.html, 'datastore.html'))
                        if file_type_ == FileTypes.REPORT:
                            write_task_report(job_resources, task_.task_id, path_, _get_images_in_dir(task_.output_dir))
                            update_analysis_file_links(tnode_.idx, path_)

                else:
                    # Process Non-Successful Task Result
                    B.update_task_state(bg, tnode_, state_)
                    slog.error("Task was not successful {r}".format(r=result))
                    log.error(msg_ + "\n")
                    sys.stderr.write(msg_ + "\n")
                    stderr_ = os.path.join(task_.output_dir, "stderr")
                    for line_ in _get_last_lines_of_stderr(20, stderr_):
                        log.error(line_)
                        sys.stderr.write(line_ + "\n")

                    # let the remaining running jobs continue
                    w_ = workers.pop(tid_)
                    _terminate_worker(w_)
                    total_nproc -= task_.nproc
                    has_failed = True

                _update_msg = _status(bg)
                log.info(_update_msg)
                slog.info(_update_msg)

                B.write_binding_graph_images(bg, job_resources.workflow)
                s_ = TaskStates.FAILED if has_failed else TaskStates.RUNNING

                write_report_(bg, s_, False)
                write_task_summary_report(bg)

            elif isinstance(result, types.NoneType):
                pass
            else:
                log.error("Unexpected queue result type {t} {r}".format(t=type(result), r=result))

            if has_failed:
                log.error("job has failed. breaking out.")
                # Just kill everything
                break

            tnode = B.get_next_runnable_task(bg)

            if tnode is not None:
                log.debug("Got task node '{t}'".format(t=tnode))

            if tnode is None:
                continue
            elif isinstance(tnode, TaskBindingNode):

                def _to_base(task_id_):
                    # FIXME need to handle task namespaces
                    return GlobalConstants.RX_TASK_ID.match(task_id_).groups()[1]

                # base task_id-instance_id
                tid = '-'.join([_to_base(tnode.meta_task.task_id), str(tnode.instance_id)])

                # Exclude already chunked Tasks
                if not isinstance(tnode, TaskChunkedBindingNode):
                    if is_task_id_scatterable(tnode.meta_task.task_id):
                        scatterable_task_id = get_scatter_task_id_from_task_id(tnode.meta_task.task_id)
                        was_chunked = bg.node[tnode]['was_chunked']
                        if not was_chunked:
                            log.debug("Resolved scatter task {i} from task {x}".format(i=scatterable_task_id, x=tnode.meta_task.task_id))
                            B.add_scatter_task(bg, tnode, registered_tasks_d[scatterable_task_id])
                            bg.node[tnode]['was_chunked'] = True
                            B.write_binding_graph_images(bg, job_resources.workflow)


                    # Update node to scattered and breakout of loop
                    # let's just run the task for now
                    # B.update_task_state(bg, tnode, TaskStates.SCATTERED)
                    # continue

                task_dir = os.path.join(job_resources.tasks, tid)
                if not os.path.exists(task_dir):
                    os.mkdir(task_dir)

                to_resources_func = B.to_resolve_di_resources(task_dir, root_tmp_dir=workflow_opts.tmp_dir)
                input_files = B.get_task_input_files(bg, tnode)

                # convert metatask -> task
                try:
                    task = GX.meta_task_to_task(tnode.meta_task, input_files, task_opts, task_dir, max_nproc, max_nchunks,
                                                to_resources_func, to_resolve_files_func)
                except Exception as e:
                    log.error("Failed to convert metatask {i} to task. {m}".format(i=tnode.meta_task.task_id, m=e.message))
                    raise

                # log.debug(task)

                bg.node[tnode]['nproc'] = task.nproc

                if not has_available_slots(task.nproc):
                    # not enough slots to run in
                    continue

                bg.node[tnode]['task'] = task
                tnode_to_task[tnode] = task

                manifest_path = os.path.join(task_dir, GlobalConstants.TASK_MANIFEST_JSON)
                if isinstance(tnode.meta_task, MetaStaticTask):
                    # write driver manifest
                    driver_manifest_path = os.path.join(task_dir, GlobalConstants.DRIVER_MANIFEST_JSON)
                    IO.write_driver_manifest(tnode.meta_task, task, driver_manifest_path)

                write_task_manifest(manifest_path, tid, task, tnode.meta_task.resource_types,
                                    GlobalConstants.TASK_MANIFEST_VERSION, tnode.meta_task.__module__, cluster_renderer)

                if tnode.meta_task.task_type == TaskTypes.LOCAL or workflow_opts.distributed_mode is False:
                    to_worker_func = _to_local_w
                else:
                    to_worker_func = _to_w

                w = to_worker_func("worker-task-{i}".format(i=tid), tid, manifest_path)

                workers[tid] = w
                w.start()
                total_nproc += task.nproc
                log.debug("Starting worker {i} ({n} workers running, {m} total proc in use)".format(i=tid, n=len(workers), m=total_nproc))

                # Submit job to be run.
                B.update_task_state(bg, tnode, TaskStates.SUBMITTED)
                log.debug("Updating task {t} to SUBMITTED".format(t=tid))
                tid_to_tnode[tid] = tnode

            elif isinstance(tnode, EntryOutBindingFileNode):
                # Handle EntryPoint types. This is not a particularly elegant design :(
                bg.node[tnode]['nproc'] = 1
                log.info("Marking task as completed {t}".format(t=tnode))
                B.update_task_state_to_success(bg, tnode, 0.0)
                # Update output paths
                mock_file_index = 0
                for fnode in bg.successors(tnode):
                    file_path = "/path/to/mock-file-{i}.txt".format(i=mock_file_index)
                    B.update_file_state_to_resolved(bg, fnode, file_path)
                    mock_file_index += 1
            else:
                raise TypeError("Unsupported type {t} of '{x}'".format(t=type(tnode), x=tnode))

            # Update state of any files
            B.resolve_successor_binding_file_path(bg)
            # Final call to write empty analysis reports
            write_analysis_report(analysis_file_links)

        # end of while loop
        _terminate_all_workers(workers.values(), shutdown_event)

        B.write_binding_graph_images(bg, job_resources.workflow)

        if has_failed:
            log.debug("\n" + B.to_binding_graph_summary(bg))

        was_successful = B.was_workflow_successful(bg)
        s_ = TaskStates.SUCCESSFUL if was_successful else TaskStates.FAILED
        write_report_(bg, s_, was_successful)

    except PipelineRuntimeKeyboardInterrupt:
        write_report_(bg, TaskStates.KILLED, False)
        write_task_summary_report(bg)
        raise
    except Exception:
        # update workflow reports to failed
        write_report_(bg, TaskStates.FAILED, False)
        write_task_summary_report(bg)
        raise

    return True if was_successful else False


def _write_final_results_message(state):
    if state is True:
        msg = "Successfully completed workflow."
    else:
        msg = "Failed to run workflow."
        sys.stderr.write(msg + "\n")

    sys.stdout.write(msg + "\n")


def _validate_entry_points_or_raise(entry_points_d):
    for entry_id, path in entry_points_d.iteritems():
        if not os.path.exists(path):
            raise IOError("Unable to find entry point {e} path {p}".format(e=entry_id, p=path))

    return True


def _load_io_for_workflow(registered_tasks, registered_pipelines, workflow_template_xml_or_pipeline, entry_points_d, preset_xml, rc_preset_or_none):
    """
    Load and resolve input IO layer

    # Load Presets and Workflow Options. Resolve and Merge
    # The Order of loading is
    # - rc, workflow.xml, then preset.xml

    :returns: A tuple of Workflow Bindings, Workflow Level Options, Task Opts, ClusterRenderer)
    :rtype: (List[(str, str)], WorkflowLevelOpts, {TaskId:value}, ClusterRenderer)
    """
    # Load Presets and Workflow Options. Resolve and Merge
    # The Order of loading is
    # - rc, workflow.xml, then preset.xml

    # A little sanity check
    # Validate that entry points exist

    slog.info("validating entry points.")
    _validate_entry_points_or_raise(entry_points_d)
    slog.info("successfully validated {n} entry points".format(n=len(entry_points_d)))

    wopts = {}
    topts = {}

    if rc_preset_or_none is None:
        rc_preset = IO.load_preset_from_env()
    else:
        rc_preset = IO.parse_pipeline_preset_xml(rc_preset_or_none)

    if isinstance(workflow_template_xml_or_pipeline, Pipeline):
        builder_record = IO.BuilderRecord(workflow_template_xml_or_pipeline.all_bindings, {}, {})
    else:
        slog.info("Loading workflow template.")
        builder_record = IO.parse_pipeline_template_xml(workflow_template_xml_or_pipeline, registered_pipelines)
        slog.info("successfully loaded workflow template.")

    if preset_xml is None:
        slog.info("No preset provided. Skipping preset.xml loading.")
        preset_record = None
    else:
        slog.info("Loading preset {p}".format(p=preset_xml))
        preset_record = IO.parse_pipeline_preset_xml(preset_xml)
        slog.info("successfully loaded preset.")

    if rc_preset is not None:
        topts.update(dict(rc_preset.task_options))
        wopts.update(dict(rc_preset.workflow_options))

    wopts.update(dict(builder_record.workflow_options))
    topts.update(builder_record.task_options)

    if preset_record is not None:
        wopts.update(dict(preset_record.workflow_options))
        topts.update(dict(preset_record.task_options))

    workflow_level_opts = IO.WorkflowLevelOptions.from_id_dict(wopts)

    workflow_level_opts = IO.validate_or_modify_workflow_level_options(workflow_level_opts)

    slog.info("Successfully validated workflow options.")

    slog.info("validating supplied task options.")
    topts = IO.validate_raw_task_options(registered_tasks, topts)
    slog.info("successfully validated (pre DI) task options.")

    log.debug("Resolved task options to {d}".format(d=workflow_level_opts))
    log.debug(pprint.pformat(workflow_level_opts.to_dict(), indent=4))

    workflow_bindings = builder_record.bindings

    if isinstance(workflow_level_opts.cluster_manager_path, str):
        cluster_render = C.load_cluster_templates(workflow_level_opts.cluster_manager_path)
    else:
        cluster_render = None

    return workflow_bindings, workflow_level_opts, topts, cluster_render


def _load_io_for_task(registered_tasks, entry_points_d, preset_xml, rc_preset_or_none):

    slog.info("validating entry points. {e}".format(e=entry_points_d))
    _validate_entry_points_or_raise(entry_points_d)
    slog.info("successfully validated {n} entry points".format(n=len(entry_points_d)))

    wopts = {}
    topts = {}

    if rc_preset_or_none is None:
        rc_preset = IO.load_preset_from_env()
    else:
        rc_preset = IO.parse_pipeline_preset_xml(rc_preset_or_none)

    if rc_preset:
        topts.update(dict(rc_preset.task_options))
        wopts.update(dict(rc_preset.workflow_options))

    if preset_xml is not None:
        preset_record = IO.parse_pipeline_preset_xml(preset_xml)
        wopts.update(dict(preset_record.workflow_options))
        topts.update(dict(preset_record.task_options))

    workflow_level_opts = IO.WorkflowLevelOptions.from_id_dict(wopts)

    workflow_level_opts = IO.validate_or_modify_workflow_level_options(workflow_level_opts)

    # Validate
    topts = IO.validate_raw_task_options(registered_tasks, topts)

    log.debug("Resolved task options to {d}".format(d=workflow_level_opts))
    log.debug(pprint.pprint(workflow_level_opts.to_dict(), indent=4))

    if isinstance(workflow_level_opts.cluster_manager_path, str):
        cluster_render = C.load_cluster_templates(workflow_level_opts.cluster_manager_path)
    else:
        cluster_render = None

    return workflow_level_opts, topts, cluster_render


def exe_workflow(chunk_operators, entry_points_d, bg, task_opts, workflow_level_opts, output_dir,
                 registered_tasks_d, registered_file_types_d,
                 cluster_render, task_runner_func, task_cluster_runner_func):
    """This is the fundamental entry point to running a pbsmrtpipe workflow."""

    slog.info("Initializing Workflow")

    # Create workers container here so we can catch exceptions and shutdown
    # gracefully
    workers = {}
    manager = multiprocessing.Manager()
    shutdown_event = manager.Event()

    try:
        state = __exe_workflow(chunk_operators, entry_points_d, bg, task_opts, workflow_level_opts,
                               output_dir, registered_tasks_d, registered_file_types_d, cluster_render,
                               task_runner_func, task_cluster_runner_func, workers, shutdown_event)
    except Exception as e:
        if isinstance(e, KeyboardInterrupt):
            emsg = "received SIGINT. Attempting to abort gracefully."
        else:
            emsg = "Unexpected exception. shutting down."

        slog.error(emsg)
        sys.stderr.write(emsg + "\n")
        state = False
        _terminate_all_workers(workers.values(), shutdown_event)
        _write_final_results_message(state)
        raise

    return state


def workflow_exception_exitcode_handler(func):
    """Decorator to call core workflow/task run funcs

    Funcs should return True/False or 0/1.

    It will log the run time and handle exception handling and logging/stderr.

    """

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        started_at = time.time()

        try:
            state = func(*args, **kwargs)
        except Exception as e:
            emsg = "Error executing function {f}".format(f=func.__name__)

            log.error(emsg)
            sys.stderr.write(emsg + "\n")

            slog.error(e)

            exc_type, exc_value, exc_tb = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_tb, file=sys.stderr)
            type_, value_, traceback_ = sys.exc_info()

            slog.exception(type_)
            slog.exception("\n")
            slog.exception(value_)
            slog.exception("\n")
            slog.exception(traceback_)

            state = False

        run_time = time.time() - started_at
        run_time_min = run_time / 60.0
        _m = "was Successful" if state else "Failed"
        msg = "Completed execution pbsmrtpipe v{x}. Workflow {s} in {r:.2f} sec ({m:.2f} min)".format(s=_m, r=run_time, x=pbsmrtpipe.get_version(), m=run_time_min)

        slog.info(msg)
        log.info(msg)
        sys.stdout.write(msg + "\n")

        return 0 if state else -1

    return _wrapper


@workflow_exception_exitcode_handler
def run_pipeline(registered_pipelines_d, registered_file_types_d, registered_tasks_d, chunk_operators, workflow_template_xml, entry_points_d, output_dir, preset_xml, rc_preset_or_none, mock_mode):
    """


    :param workflow_template_xml: path to workflow xml
    :param entry_points_d:
    :param output_dir:
    :param preset_xml: path to preset xml (or None)
    :param mock_mode:
    :return: exit code

    :type registered_tasks_d: dict[str, pbsmrtpipe.pb_tasks.core.MetaTask]
    :type registered_file_types_d: dict[str, pbsmrtpipe.pb_tasks.core.FileType]
    :type workflow_template_xml: str
    :type output_dir: str
    :type preset_xml: str | None
    :type mock_mode: bool

    :rtype: int
    """
    log.debug(pprint.pformat(entry_points_d))

    workflow_bindings, workflow_level_opts, task_opts, cluster_render = _load_io_for_workflow(registered_tasks_d, registered_pipelines_d, workflow_template_xml,
                                                                                              entry_points_d, preset_xml, rc_preset_or_none)

    slog.info("building graph")
    bg = B.binding_strs_to_binding_graph(registered_tasks_d, workflow_bindings)
    slog.info("successfully loaded graph from bindings.")

    if mock_mode:
        # Just run everything locally
        task_runner_func = M.mock_run_task_manifest
        task_cluster_runner_func = M.mock_run_task_manifest
    else:
        task_runner_func = run_task_manifest
        task_cluster_runner_func = run_task_manifest_on_cluster

    # Disabled chunk operators if necessary
    if workflow_level_opts.chunk_mode is False:
        slog.info("Chunk mode is False. Disabling {n} chunk operators.".format(n=len(chunk_operators)))
        chunk_operators = {}

    return exe_workflow(chunk_operators, entry_points_d, bg, task_opts, workflow_level_opts, output_dir,
                        registered_tasks_d, registered_file_types_d, cluster_render, task_runner_func, task_cluster_runner_func)


def _task_to_entry_point_ids(meta_task):
    def _to_e(i_):
        return "{e}:e_{i}".format(i=i_, e=GlobalConstants.ENTRY_PREFIX)
    return [_to_e(i) for i in xrange(len(meta_task.input_types))]


def _task_to_binding_strings(meta_task):
    """
    Create binding strings from a meta task instance. Entry points are create
    to correspond with the positional input type.

    e_0, e_1, ...

    :type meta_task: MetaTask
    :param meta_task:
    :return: List of binding strings
    """
    def _to_b(i_):
        return "{t}:{i}".format(t=meta_task.task_id, i=i_)

    entry_point_ids = _task_to_entry_point_ids(meta_task)

    return [(entry_point_id, _to_b(i)) for i, entry_point_id in enumerate(entry_point_ids)]


def _validate_task_entry_points_or_raise(meta_task, entry_points_d):

    entry_point_ids = _task_to_entry_point_ids(meta_task)

    zs = zip(entry_point_ids, meta_task.input_types)
    for ep_id, in_type in zs:
        if ep_id not in entry_points_d:
            _d = dict(n=ep_id, i=in_type, v=entry_points_d.keys(), t=meta_task.task_id)
            raise KeyError("Task {t} Required Entry point '{n}' for input type {i} not supplied. Supplied values {v}".format(**_d))

    return True


@workflow_exception_exitcode_handler
def run_single_task(registered_file_types_d, registered_tasks_d, chunk_operators, entry_points_d, task_id, output_dir, preset_xml, rc_preset_or_none):
    """
    Run a task by id.

    :param task_id:
    :param output_dir:
    :return:
    """

    meta_task = registered_tasks_d.get(task_id, None)

    if meta_task is None:
        raise KeyError("Unable to find task id '{i}' in registered tasks. Use "
                       "'show-tasks' to get a list of registered tasks.".format(i=task_id))

    print entry_points_d
    workflow_level_opts, task_opts, cluster_render = _load_io_for_task(registered_tasks_d, entry_points_d, preset_xml, rc_preset_or_none)

    task_runner_func = run_task_manifest
    task_cluster_runner_func = run_task_manifest_on_cluster

    slog.info("building bindings graph")
    binding_str = _task_to_binding_strings(meta_task)

    bg = B.binding_strs_to_binding_graph(registered_tasks_d, binding_str)
    slog.info("successfully built bindings graph for task {i}".format(i=task_id))

    return exe_workflow(chunk_operators, entry_points_d, bg, task_opts, workflow_level_opts, output_dir,
                        registered_tasks_d,
                        registered_file_types_d, cluster_render, task_runner_func, task_cluster_runner_func)



