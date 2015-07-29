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
import functools
import uuid
from pbcore.io import DataSet

from pbcommand.models import (FileTypes, DataStoreFile, TaskTypes)

import pbsmrtpipe
import pbsmrtpipe.constants as GlobalConstants
from pbsmrtpipe.exceptions import (PipelineRuntimeError,
                                   PipelineRuntimeKeyboardInterrupt,
                                   WorkflowBaseException,
                                   MalformedChunkOperatorError)
import pbsmrtpipe.pb_io as IO
import pbsmrtpipe.graph.bgraph as B
import pbsmrtpipe.graph.bgraph_utils as BU
import pbsmrtpipe.cluster as C
import pbsmrtpipe.tools.runner as T
import pbsmrtpipe.report_renderer as R
import pbsmrtpipe.driver_utils as DU
import pbsmrtpipe.services as WS
from pbsmrtpipe import opts_graph as GX

from pbsmrtpipe.graph.models import (TaskStates,
                                     TaskBindingNode,
                                     TaskChunkedBindingNode,
                                     EntryOutBindingFileNode)


from pbsmrtpipe.models import (Pipeline, MetaStaticTask, MetaTask,
                               GlobalRegistry, TaskResult, validate_operator,
                               AnalysisLink)
from pbsmrtpipe.engine import TaskManifestWorker
from pbsmrtpipe.pb_io import WorkflowLevelOptions


log = logging.getLogger(__name__)
slog = logging.getLogger('status.' + __name__)

# logging.basicConfig(level=logging.DEBUG)


class Constants(object):
    SHUTDOWN = "SHUTDOWN"


def _init_bg(bg, ep_d):
    """Resolving/Initializing BindingGraph with supplied EntryPoints"""

    # Help initialize graph/epoints
    B.resolve_entry_points(bg, ep_d)
    # Update Task-esque EntryBindingPoint
    B.resolve_entry_binding_points(bg)

    # initialization File node attributes
    for eid, path in ep_d.iteritems():
        B.resolve_entry_point(bg, eid, path)
        B.resolve_successor_binding_file_path(bg)

    return bg


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
    ntasks = len(bg.all_task_type_nodes())
    ncompleted_tasks = len(B.get_tasks_by_state(bg, TaskStates.SUCCESSFUL))
    return "Workflow status {n}/{t} completed/total tasks".format(t=ntasks, n=ncompleted_tasks)


def _get_dataset_uuid_or_create_uuid(path):
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
    """Read in the last N-lines of the stderr from a task

    This should be smarter to look for stacktraces and common errors.
    """
    lines = []
    try:
        with open(stderr_path, 'r') as f:
            outs = f.readlines()
        x = min(n, len(outs))
        lines = outs[-x:]
    except Exception as e:
        log.warn("Unable to extract stderr from {p} Error {e}".format(p=stderr_path, e=e.message))

    return lines


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


def __exe_workflow(global_registry, ep_d, bg, task_opts, workflow_opts, output_dir,
                   workers, shutdown_event, service_uri_or_none):
    """
    Core runner of a workflow.

    :type bg: BindingsGraph
    :type workflow_opts: WorkflowLevelOptions
    :type output_dir: str
    :type service_uri_or_none: str | None

    :param workers: {taskid:Worker}
    :return:

    The function is doing way too much. This is really terrible.
    """
    job_id = random.randint(100000, 999999)
    started_at = time.time()

    m_ = "Distributed" if workflow_opts.distributed_mode is not None else "Local"

    slog.info("starting to execute {m} workflow with assigned job_id {i}".format(i=job_id, m=m_))
    log.info("exe'ing workflow Cluster renderer {c}".format(c=global_registry.cluster_renderer))
    slog.info("Service URI: {i} {t}".format(i=service_uri_or_none, t=type(service_uri_or_none)))

    # Setup logger, job directory and initialize DS
    slog.info("creating job resources in {o}".format(o=output_dir))
    job_resources, ds = DU.job_resource_create_and_setup_logs(log, output_dir, bg, task_opts, workflow_opts, ep_d)
    slog.info("successfully created job resources.")

    # Some Pre-flight checks
    # Help initialize graph/epoints
    B.resolve_entry_points(bg, ep_d)
    # Update Task-esque EntryBindingPoint
    B.resolve_entry_binding_points(bg)

    # initialization File node attributes
    for eid, path in ep_d.iteritems():
        B.resolve_entry_point(bg, eid, path)
        B.resolve_successor_binding_file_path(bg)

    # Mark Chunkable tasks
    B.label_chunkable_tasks(bg, global_registry.chunk_operators)

    slog.info("validating binding graph")
    # Check the degree of the nodes
    B.validate_binding_graph_integrity(bg)
    slog.info("successfully validated binding graph.")

    # Add scattered
    # This will add new nodes to the graph if necessary
    B.apply_chunk_operator(bg, global_registry.chunk_operators, global_registry.tasks)

    # log.info(BU.to_binding_graph_summary(bg))

    # "global" file type id counter {str: int} that will be
    # used to generate ids
    file_type_id_to_count = {file_type.file_type_id: 0 for _, file_type in global_registry.file_types.iteritems()}

    # Create a closure to allow file types to assign unique id
    # this returns a func
    to_resolve_files_func = B.to_resolve_files(file_type_id_to_count)

    # Local vars
    max_total_nproc = workflow_opts.total_max_nproc
    max_nworkers = workflow_opts.max_nworkers
    max_nproc = workflow_opts.max_nproc
    max_nchunks = workflow_opts.max_nchunks

    q_out = multiprocessing.Queue()

    worker_sleep_time = 1
    # To store all the reports that are displayed in the analysis.html
    # {id:task-id, report_path:path/to/report.json}
    analysis_file_links = []

    # Flag for pipeline execution failure
    has_failed = False
    # Time to sleep between each step the execution loop
    # after the first 1 minute of exe, update the sleep time to 2 sec
    sleep_time = 1
    # Running total of current number of slots/cpu's used
    total_nproc = 0

    # Define a bunch of util funcs to try to make the main driver while loop
    # more understandable. Not the greatest model.

    def _to_run_time():
        return time.time() - started_at

    def write_analysis_report(analysis_file_links_):
        analysis_report_html = os.path.join(job_resources.html, 'analysis.html')
        R.write_analysis_link_report(analysis_file_links_, analysis_report_html)

    def update_analysis_file_links(task_id_, report_path_):
        analysis_link = AnalysisLink(task_id_, report_path_)
        log.info("updating report analysis file links {a}".format(a=analysis_link))
        analysis_file_links.append(analysis_link)
        write_analysis_report(analysis_file_links)

    # utils for getting the running func and worker type
    def _to_w(wid, task_id, manifest_path_):
        # the IO loading will forceful set this to None
        # if the cluster manager not defined or cluster_mode is False
        if global_registry.cluster_renderer is None:
            r_func = T.run_task_manifest
        else:
            r_func = T.run_task_manifest_on_cluster
        return TaskManifestWorker(q_out, shutdown_event, worker_sleep_time,
                                  r_func, task_id, manifest_path_, name=wid)

    def _to_local_w(wid, task_id, manifest_path_):
        return TaskManifestWorker(q_out, shutdown_event, worker_sleep_time,
                                  T.run_task_manifest, task_id, manifest_path_, name=wid)

    # factories for getting a Worker instance
    def _determine_worker_type(task_type):
        # resolve the specific worker type
        if task_type == TaskTypes.LOCAL or workflow_opts.distributed_mode is False:
            return _to_local_w
        else:
            return _to_w

    # Define a bunch of util funcs to try to make the main driver while loop
    # more understandable. Not the greatest model.
    def write_report_(bg_, current_state_, was_successful_):
        return DU.write_main_workflow_report(job_id, job_resources, workflow_opts,
                                             task_opts, bg_, current_state_, was_successful_, _to_run_time())

    def write_task_summary_report(bg_):
        task_summary_report = DU.to_task_summary_report(bg_)
        p = os.path.join(job_resources.html, 'task_summary.html')
        R.write_report_to_html(task_summary_report, p)

    def is_task_id_scatterable(task_id_):
        return _is_task_id_scatterable(global_registry.chunk_operators, task_id_)

    def get_scatter_task_id_from_task_id(task_id_):
        return _get_scatterable_task_id(global_registry.chunk_operators, task_id_)

    def services_log_update_progress(source_id_, level_, message_):
        if service_uri_or_none is not None:
            total_log_uri = "{u}/log".format(u=service_uri_or_none)
            WS.log_pbsmrtpipe_progress(total_log_uri, message_, level_, source_id_, ignore_errors=True)

    def services_add_datastore_file(datastore_file_):
        if service_uri_or_none is not None:
            total_ds_uri = "{u}/datastore".format(u=service_uri_or_none)
            WS.add_datastore_file(total_ds_uri, datastore_file_, ignore_errors=True)

    def _update_analysis_reports_and_datastore(tnode_, task_):
        for file_type_, path_ in zip(tnode_.meta_task.output_types, task_.output_files):
            source_id = "{t}-{f}".format(t=task_.task_id, f=file_type_.file_type_id)
            ds_uuid = _get_dataset_uuid_or_create_uuid(path_)
            ds_file_ = DataStoreFile(ds_uuid, source_id, file_type_.file_type_id, path_)
            ds.add(ds_file_)
            ds.write_update_json(job_resources.datastore_json)

            # Update Services
            services_add_datastore_file(ds_file_)

            dsr = DU.datastore_to_report(ds)
            R.write_report_to_html(dsr, os.path.join(job_resources.html, 'datastore.html'))
            if file_type_ == FileTypes.REPORT:
                T.write_task_report(job_resources, task_.task_id, path_, DU._get_images_in_dir(task_.output_dir))
                update_analysis_file_links(tnode_.idx, path_)

    def _log_task_failure_and_call_services(path_to_stderr, task_id_):
        """log the error messages extracted from stderr"""
        lines = _get_last_lines_of_stderr(20, path_to_stderr)
        for line_ in lines:
            log.error(line_.strip())
            # these already have newlines
            sys.stderr.write(line_)
            sys.stderr.write("\n")
        services_log_update_progress("pbsmrtpipe::{i}".format(i=task_id_), WS.LogLevels.ERROR, "\n".join(lines))

    def has_available_slots(n):
        if max_total_nproc is None:
            return True
        return total_nproc + n <= max_total_nproc

    # Misc setup
    write_report_(bg, TaskStates.CREATED, False)
    # write empty analysis reports
    write_analysis_report(analysis_file_links)

    # For book-keeping
    # task id -> tnode
    tid_to_tnode = {}
    # tnode -> Task instance
    tnode_to_task = {}

    try:
        log.debug("Starting execution loop... in process {p}".format(p=os.getpid()))
        BU.write_binding_graph_images(bg, job_resources.workflow)

        # After the initial startup, bump up the time to reduce resource usage
        # (since multiple instances will be launched from the services)
        if _to_run_time() > 300:
            sleep_time = 5

        while True:

            # This will add new nodes to the graph if necessary
            B.apply_chunk_operator(bg, global_registry.chunk_operators, global_registry.tasks)
            # B.write_binding_graph_images(bg, job_resources.workflow)
            # If a TaskScatteredBindingNode is completed successfully and
            # output chunk.json is resolved, read in the file and
            # generate the new chunked tasks. This mutates the graph
            # significantly.
            B.add_gather_to_completed_task_chunks(bg, global_registry.chunk_operators, global_registry.tasks)

            if not _are_workers_alive(workers):
                for tix_, w_ in workers.iteritems():
                    if not w_.is_alive():
                        log.warn("Worker {i} (pid {p}) is not alive for task {x}. Worker exit code {e}.".format(i=w_.name, p=w_.pid, e=w_.exitcode, x=tix_))
                        #w_.terminate()

            # Check if Any tasks are running or that there still runnable tasks
            is_completed = bg.is_workflow_complete()
            has_task_running = B.has_running_task(bg)

            if not is_completed:
                if not has_task_running:
                    if not B.has_task_in_states(bg, TaskStates.RUNNABLE_STATES()):
                        if not B.has_next_runnable_task(bg):
                            msg = "Unable to find runnable task or any tasks running and workflow is NOT completed."
                            log.error(msg)
                            log.error(BU.to_binding_graph_summary(bg))
                            services_log_update_progress("pbsmrtpipe", WS.LogLevels.ERROR, msg)
                            raise PipelineRuntimeError(msg)

            time.sleep(sleep_time)

            # log.debug("Sleeping for {s}".format(s=sleep_time))
            #log.debug("\n" + BU.to_binding_graph_summary(bg))
            # BU.to_binding_graph_task_summary(bg)

            # This should only be triggered after events. The main reason
            # to keep updating it was the html report is up to date with the
            # runtime
            write_report_(bg, TaskStates.RUNNING, is_completed)

            if is_completed:
                msg_ = "Workflow is completed. breaking out."
                log.info(msg_)
                services_log_update_progress("pbsmrtpipe", WS.LogLevels.INFO, msg_)
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
                    msg_ = "Task was successful {r}".format(r=result)
                    slog.info(msg_)

                    is_valid_or_emsg = B.validate_outputs_and_update_task_to_success(bg, tnode_, run_time_, bg.node[tnode_]['task'].output_files)

                    # Failed to find output files
                    if is_valid_or_emsg is not True:
                        B.update_task_state_to_failed(bg, tnode_, result.run_time_sec, result.error_message)
                        raise PipelineRuntimeError(is_valid_or_emsg)

                    services_log_update_progress("pbsmrtpipe::{i}".format(i=tid_), WS.LogLevels.INFO, msg_)
                    B.update_task_output_file_nodes(bg, tnode_, tnode_to_task[tnode_])
                    B.resolve_successor_binding_file_path(bg)

                    total_nproc -= task_.nproc
                    w_ = workers.pop(tid_)
                    _terminate_worker(w_)

                    # Update Analysis Reports and Register output files to Datastore
                    _update_analysis_reports_and_datastore(tnode_, task_)

                    BU.write_binding_graph_images(bg, job_resources.workflow)
                else:
                    # Process Non-Successful Task Result
                    B.update_task_state(bg, tnode_, state_)
                    msg_ = "Task was not successful {r}".format(r=result)
                    slog.error(msg_)
                    log.error(msg_ + "\n")
                    sys.stderr.write(msg_ + "\n")

                    stderr_ = os.path.join(task_.output_dir, "stderr")

                    _log_task_failure_and_call_services(stderr_, tid_)

                    # let the remaining running jobs continue
                    w_ = workers.pop(tid_)
                    _terminate_worker(w_)

                    total_nproc -= task_.nproc
                    has_failed = True

                    BU.write_binding_graph_images(bg, job_resources.workflow)

                _update_msg = _status(bg)
                log.info(_update_msg)
                slog.info(_update_msg)

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
                # Found a Runnable Task

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
                            B.add_scatter_task(bg, tnode, global_registry.tasks[scatterable_task_id])
                            bg.node[tnode]['was_chunked'] = True
                            BU.write_binding_graph_images(bg, job_resources.workflow)

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

                # Write "manifest" of the task to run, this included the Task id,
                # and cluster template.
                # this is similar to the Resolved Tool Contract
                manifest_path = os.path.join(task_dir, GlobalConstants.TASK_MANIFEST_JSON)
                if isinstance(tnode.meta_task, MetaStaticTask):
                    # write driver manifest, which calls the resolved-tool-contract.json
                    # there's too many layers of indirection here. Partly due to the pre-tool-contract era
                    # python defined tasks.
                    driver_manifest_path = os.path.join(task_dir, GlobalConstants.DRIVER_MANIFEST_JSON)
                    IO.write_driver_manifest(tnode.meta_task, task, task_opts, driver_manifest_path)

                DU.write_task_manifest(manifest_path, tid, task, tnode.meta_task.resource_types,
                                       GlobalConstants.TASK_MANIFEST_VERSION,
                                       tnode.meta_task.__module__, global_registry.cluster_renderer)

                # Create an instance of Worker
                worker_type = _determine_worker_type(tnode.meta_task.task_type)
                w = worker_type("worker-task-{i}".format(i=tid), tid, manifest_path)

                workers[tid] = w
                w.start()
                total_nproc += task.nproc
                log.debug("Starting worker {i} ({n} workers running, {m} total proc in use)".format(i=tid, n=len(workers), m=total_nproc))

                # Submit job to be run.
                B.update_task_state(bg, tnode, TaskStates.SUBMITTED)
                msg_ = "Updating task {t} to SUBMITTED".format(t=tid)
                log.debug(msg_)
                tid_to_tnode[tid] = tnode
                services_log_update_progress("pbsmrtpipe::{i}".format(i=tnode.idx), WS.LogLevels.INFO, msg_)
                #B.write_binding_graph_images(bg, job_resources.workflow)

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
                raise TypeError("Unsupported node type {t} of '{x}'".format(t=type(tnode), x=tnode))

            # Update state of any files
            B.resolve_successor_binding_file_path(bg)
            # Final call to write empty analysis reports
            write_analysis_report(analysis_file_links)

        # end of while loop
        _terminate_all_workers(workers.values(), shutdown_event)

        #B.write_binding_graph_images(bg, job_resources.workflow)

        if has_failed:
            log.debug("\n" + BU.to_binding_graph_summary(bg))

        was_successful = B.was_workflow_successful(bg)
        s_ = TaskStates.SUCCESSFUL if was_successful else TaskStates.FAILED
        write_report_(bg, s_, was_successful)

    except PipelineRuntimeKeyboardInterrupt:
        write_report_(bg, TaskStates.KILLED, False)
        write_task_summary_report(bg)
        BU.write_binding_graph_images(bg, job_resources.workflow)
        was_successful = False

    except Exception as e:
        # update workflow reports to failed
        write_report_(bg, TaskStates.FAILED, False)
        write_task_summary_report(bg)
        services_log_update_progress("pbsmrtpipe", WS.LogLevels.ERROR, "Error {e}".format(e=e))
        BU.write_binding_graph_images(bg, job_resources.workflow)
        raise

    finally:
        write_task_summary_report(bg)
        BU.write_binding_graph_images(bg, job_resources.workflow)

    return True if was_successful else False


def _write_final_results_message(state):
    if state is True:
        msg = " Successfully completed workflow."
        sys.stdout.write(msg + "\n")
    else:
        msg = " Failed to run workflow."
        sys.stderr.write(msg + "\n")


def _validate_entry_points_or_raise(entry_points_d):
    for entry_id, path in entry_points_d.iteritems():
        if not os.path.exists(path):
            raise IOError("Unable to find entry point {e} path {p}".format(e=entry_id, p=path))

    return True


def _load_io_for_workflow(registered_tasks, registered_pipelines, workflow_template_xml_or_pipeline,
                          entry_points_d, preset_xml, rc_preset_or_none, force_distribute=None):
    """
    Load and resolve input IO layer

    # Load Presets and Workflow Options. Resolve and Merge
    # The Order of loading is
    # - rc, workflow.xml, then preset.xml
    # force_distribute will attempt to override ALL settings (if cluster_manager is defined)

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
        # override distributed mode only if provided.
        if force_distribute is not None:
            workflow_level_opts.distributed_mode = True
    else:
        cluster_render = None

    return workflow_bindings, workflow_level_opts, topts, cluster_render


def _load_io_for_task(registered_tasks, entry_points_d, preset_xml, rc_preset_or_none, force_distribute=None):
    """Grungy loading of the IO and resolving values

    Returns a tuple of (WorkflowLevelOptions, TaskOptions, ClusterRender)
    """
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
        # override distributed mode
        if force_distribute is not None:
            if force_distribute is True:
                workflow_level_opts.distributed_mode = True
    else:
        cluster_render = None

    return workflow_level_opts, topts, cluster_render


def exe_workflow(global_registry, entry_points_d, bg, task_opts, workflow_level_opts, output_dir, service_uri):
    """This is the fundamental entry point to running a pbsmrtpipe workflow."""

    slog.info("Initializing Workflow")

    # Create workers container here so we can catch exceptions and shutdown
    # gracefully
    workers = {}
    manager = multiprocessing.Manager()
    shutdown_event = manager.Event()

    state = False
    try:
        state = __exe_workflow(global_registry, entry_points_d, bg, task_opts,
                               workflow_level_opts, output_dir,
                               workers, shutdown_event, service_uri)
    except Exception as e:
        if isinstance(e, KeyboardInterrupt):
            emsg = "received SIGINT. Attempting to abort gracefully."
        else:
            emsg = "Unexpected exception. shutting down."

        slog.error(emsg, exc_info=True)
        sys.stderr.write(emsg + "\n")
        state = False

    finally:
        _terminate_all_workers(workers.values(), shutdown_event)
        _write_final_results_message(state)

    return state


def workflow_exception_exitcode_handler(func):
    """Decorator to call core workflow/task run funcs

    Funcs should return True/False or 0/1.

    It will log the run time and handle exception handling and logging/stderr.

    """

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        started_at = time.time()

        state = False
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

        finally:
            print "Shutting down."
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
def run_pipeline(registered_pipelines_d, registered_file_types_d, registered_tasks_d,
                 chunk_operators, workflow_template_xml, entry_points_d,
                 output_dir, preset_xml, rc_preset_or_none, service_uri, force_distribute=None):
    """
    Entry point for running a pipeline

    :param workflow_template_xml: path to workflow xml
    :param entry_points_d:
    :param output_dir:
    :param preset_xml: path to preset xml (or None)
    :return: exit code

    :type registered_tasks_d: dict[str, pbsmrtpipe.pb_tasks.core.MetaTask]
    :type registered_file_types_d: dict[str, pbsmrtpipe.pb_tasks.core.FileType]
    :type workflow_template_xml: str
    :type output_dir: str
    :type preset_xml: str | None
    :type service_uri: str | None
    :type force_distribute: None | bool

    :rtype: int
    """
    log.debug(pprint.pformat(entry_points_d))

    workflow_bindings, workflow_level_opts, task_opts, cluster_render = _load_io_for_workflow(registered_tasks_d,
                                                                                              registered_pipelines_d,
                                                                                              workflow_template_xml,
                                                                                              entry_points_d, preset_xml,
                                                                                              rc_preset_or_none,
                                                                                              force_distribute=force_distribute)

    slog.info("building graph")
    bg = B.binding_strs_to_binding_graph(registered_tasks_d, workflow_bindings)
    slog.info("successfully loaded graph from bindings.")

    valid_chunk_operators = {}
    # Disabled chunk operators if necessary
    if workflow_level_opts.chunk_mode is False:
        slog.info("Chunk mode is False. Disabling {n} chunk operators.".format(n=len(chunk_operators)))
    else:
        # Validate chunk operators, or skip if malformed.
        for chunk_operator_id, chunk_operator in chunk_operators.iteritems():
            try:
                validate_operator(chunk_operator, registered_tasks_d)
                valid_chunk_operators[chunk_operator_id] = chunk_operator
            except MalformedChunkOperatorError as e:
                log.warn("Invalid chunk operator {i}. {m}".format(i=chunk_operator_id, m=e.message))

    # Container to hold all the resources
    global_registry = GlobalRegistry(registered_tasks_d,
                                     registered_file_types_d,
                                     valid_chunk_operators,
                                     cluster_render)

    return exe_workflow(global_registry, entry_points_d, bg, task_opts,
                        workflow_level_opts, output_dir, service_uri)


def _task_to_entry_point_ids(meta_task):
    """Generate entry points from a meta-task. $entry:e_0, $entry:e_1, ...

    This is used to automatically create pipeline entry points from the
    positional inputs of the task.
    """
    def _to_e(i_):
        return "{e}e_{i}".format(i=i_, e=GlobalConstants.ENTRY_PREFIX)
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
    """Validate the entry points are consistent with the MetaTask

    :raises: KeyError
    :rtype: bool
    """
    entry_point_ids = _task_to_entry_point_ids(meta_task)

    zs = zip(entry_point_ids, meta_task.input_types)
    for ep_id, in_type in zs:
        if ep_id not in entry_points_d:
            _d = dict(n=ep_id, i=in_type, v=entry_points_d.keys(), t=meta_task.task_id)
            raise KeyError("Task {t} Required Entry point '{n}' for input type {i} not supplied. Supplied values {v}".format(**_d))

    return True


@workflow_exception_exitcode_handler
def run_single_task(registered_file_types_d, registered_tasks_d, chunk_operators,
                    entry_points_d, task_id, output_dir, preset_xml, rc_preset_or_none,
                    service_config, force_distribute=None):
    """
    Entry Point for running a single task

    :param task_id:
    :param output_dir:
    :return:
    """

    print entry_points_d
    meta_task = registered_tasks_d.get(task_id, None)

    if meta_task is None:
        raise KeyError("Unable to find task id '{i}' in registered tasks. Use "
                       "'show-tasks' to get a list of registered tasks.".format(i=task_id))

    workflow_level_opts, task_opts, cluster_render = _load_io_for_task(registered_tasks_d, entry_points_d,
                                                                       preset_xml, rc_preset_or_none,
                                                                       force_distribute=force_distribute)

    slog.info("building bindings graph")
    binding_str = _task_to_binding_strings(meta_task)

    bg = B.binding_strs_to_binding_graph(registered_tasks_d, binding_str)
    slog.info("successfully bindings graph for task {i}".format(i=task_id))

    # Validate chunk operators
    valid_chunk_operators = {k: v for k, v in chunk_operators.iteritems() if validate_operator(v, registered_tasks_d)}
    # Container to hold all the resources
    global_registry = GlobalRegistry(valid_chunk_operators,
                                     registered_file_types_d,
                                     chunk_operators,
                                     cluster_render)

    return exe_workflow(global_registry, entry_points_d, bg, task_opts,
                        workflow_level_opts, output_dir, service_config)
