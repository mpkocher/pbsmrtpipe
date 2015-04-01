"""Process Engine for running jobs"""
import os
import sys
import time
import logging
import multiprocessing
import subprocess
import types
import platform
import tempfile
import shlex
import signal

from pbsmrtpipe.cluster import ClusterTemplateRender

log = logging.getLogger(__name__)
slog = logging.getLogger('status.' + __name__)


def backticks(cmd, merge_stderr=True):
    """
    Returns rcode, stdout, stderr
    """
    if merge_stderr:
        _stderr = subprocess.STDOUT
    else:
        _stderr = subprocess.PIPE

    # Setting shell = True is really badform, however, many of the tasks
    # generate general shell code (which needs to be removed).
    p = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=_stderr,
                         close_fds=True)

    started_at = time.time()

    # node_id = socket.getfqdn()
    node_id = platform.node()

    log.debug("Running on {s} with cmd '{c}'".format(s=node_id, c=cmd))

    out = [l[:-1] for l in p.stdout.readlines()]

    p.stdout.close()

    # need to allow process to terminate
    p.wait()

    run_time = time.time() - started_at

    errCode = p.returncode and p.returncode or 0

    if p.returncode > 0:
        errorMessage = os.linesep.join(out)
        output = []
    else:
        errorMessage = ''
        output = out

    if p.returncode == 0:
        log.debug("Successful output (Return code = 0) in {s:.2f} sec ({m:.2f} min) of {c}".format(c=cmd, s=run_time, m=run_time / 60.0))
    else:
        msg = "Return code {r} {e} of cmd {c}".format(r=p.returncode, e=errorMessage, c=cmd)
        log.error(msg)
        sys.stderr.write(msg)

    return errCode, output, errorMessage, run_time


def run_command(cmd, stdout_fh, stderr_fh, shell=True, time_out=None):
    """Run command


    :param time_out: (None, Int) Timeout in seconds.

    :return: (exit code, stdout, stderr, run_time_sec)

    """

    started_at = time.time()
    # Most of the current pacbio shell commands have aren't shlex-able
    if not shell:
        cmd = shlex.split(cmd)

    hostname = platform.node()
    slog.debug("calling cmd '{c}' on {h}".format(c=cmd, h=hostname))
    process = subprocess.Popen(cmd, stderr=stderr_fh, stdout=stdout_fh, shell=shell)

    # This needs a better dynamic model
    max_sleep_time = 5
    sleep_time = 0.1
    dt = 0.1

    process.poll()
    while process.returncode is None:
        process.poll()
        time.sleep(sleep_time)
        run_time = time.time() - started_at
        if time_out is not None:
            if run_time > time_out:
                log.info("Exceeded TIMEOUT of {t}. Killing cmd '{c}'".format(t=time_out, c=cmd))
                try:
                    # ask for forgiveness model
                    process.kill()
                except OSError:
                    pass
        if sleep_time < max_sleep_time:
            sleep_time += dt

    run_time = time.time() - started_at

    run_time = run_time
    returncode = process.returncode
    log.info("returncode is {r} in {s:.2f} sec.".format(r=process.returncode,
                                                         s=run_time))

    stdout, stderr = "", ""
    return returncode, stdout, stderr, run_time


def get_results_from_queue(queue):
    """
    Pull all the results from the Output queue used by the Workers
    """
    results = []
    while True:
        if queue.empty():
            break
        else:
            results.append(queue.get())
    return results


class EngineTask(object):
    # container object
    def __init__(self, task_id, script_path, stdout, stderr, nproc, sleep_time=1):
        self.task_id = task_id
        self.script_path = script_path
        self.stdout = stdout
        self.stderr = stderr
        self.nproc = nproc
        self.sleep_time = sleep_time

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.task_id, n=self.nproc)
        return "<{k} id:{i} >".format(**_d)


class EngineWorker(multiprocessing.Process):

    def __init__(self, out_q, shutdown_event, task_job_id, script_path, stdout, stderr, nproc, sleep_time=1):
        self.task_job_id = task_job_id
        #manifest path
        self.script_path = script_path
        # queue stdout/stderr abspath
        self.stderr = stderr
        self.stdout = stdout

        self.nproc = nproc

        self.shutdown_event = shutdown_event
        # results output queue
        self.out_queue = out_q
        multiprocessing.Process.__init__(self)
        self.sleep_time = sleep_time

    def run(self):

        # quick sanity check
        if self.shutdown_event.is_set():
            log.debug("Worker {c} {n} : Got event {e} Shutting down worker.".format(n=self.name, e=self.shutdown_event.is_set(), c=self.__class__.__name__))
            return

        stdout_h = open(self.stdout, 'w+')
        stderr_h = open(self.stderr, 'w+')
        p = subprocess.Popen(self.script_path, shell=True, stdin=subprocess.PIPE, stdout=stderr_h, stderr=stdout_h, close_fds=True, preexec_fn=os.setsid)
        # Loop until subprocess is completed, or self.event is set
        started_at = time.time()

        e_msg = "Job {u} failed ".format(u=self.task_job_id)
        while p.returncode is None:
            # update returncode
            p.poll()

            # Check for shutdown message from process pool
            if self.shutdown_event.is_set():
                # hard kill of subprocess call and all it's children processes
                print "Sending SIGTERM to process group {p}.".format(p=p.pid)
                os.killpg(p.pid, signal.SIGTERM)
                p.terminate()

                run_time = time.time() - started_at
                e_msg = "Worker {n} shutdown. Job {u} killed by shutdown event. Process ran for {s:.2f} sec.".format(n=self.name, u=self.task_job_id, s=run_time)
                slog.info(e_msg)
                time.sleep(self.sleep_time)
                break

            time.sleep(self.sleep_time)

        run_time = time.time() - started_at

        if p.returncode == 0:
            # overwrite default error message
            e_msg = "Job {u} completed return code 0 in {s:.2f} sec".format(u=self.task_job_id, s=run_time)
            outs = e_msg

        # Fundamental output data format:
        # -> task_urk, return code, elapsed run time, out/error message
        output = (self.task_job_id, p.returncode, run_time, e_msg)

        self.out_queue.put(output)

        msg = "Worker {s} {n}: completed run()".format(s=self.__class__.__name__, n=self.name)
        log.info(msg)
        print msg


class ClusterEngineWorker(multiprocessing.Process):

    def __init__(self, out_q, shutdown_event, task_job_id, script_path, stdout, stderr, nproc, cluster_renderer, sleep_time=1):
        self.task_job_id = task_job_id
        self.script_path = script_path
        self.stderr = stderr
        self.stdout = stdout
        self.nproc = nproc
        self.shutdown_event = shutdown_event
        # results output queue
        self.out_queue = out_q
        multiprocessing.Process.__init__(self)

        if not isinstance(cluster_renderer, ClusterTemplateRender):
            raise TypeError("cluster render must be of type {t}".format(t=ClusterTemplateRender))
        self.cluster_renderer = cluster_renderer

        self.sleep_time = sleep_time

    def run(self):
        # sanity check
        if not os.path.exists(self.script_path):
            msg = "Unable to run task {t}. Unable to find script '{s}'".format(t=self.task_job_id, s=self.script_path)
            slog.error(msg)
            sys.stderr.write(msg + "\n")
            return

        if self.shutdown_event.is_set():
            slog.debug("Worker {c} {n} : Got event {e} Shutting down worker.".format(n=self.name, e=self.shutdown_event, c=self.__class__.__name__))
            return

        d_ = dict(i=self.task_job_id, x=self.sleep_time, c=self.__class__.__name__, n=self.script_path, p=self.name)
        slog.info("Worker {p} {c} {n} attempting to run task job id {i} cmd:'{n}'".format(**d_))

        # Run job through the  Cluster service
        # Make new stdout, stderr for qsub output
        to_q = lambda x: x + ".cluster"
        stdout_h = open(to_q(self.stdout), 'w+')
        stderr_h = open(to_q(self.stderr), 'w+')

        # Necessary to write files first?
        with open(self.stdout, 'w+') as f:
            f.write("# stdout Running task_job_id {i}".format(i=self.task_job_id))
        with open(self.stderr, 'w+') as f:
            f.write("# stderr Running task_job_id {i}".format(i=self.task_job_id))

        # Not sure how to enable the selection of start or interactive
        template_name = 'interactive'
        cluster_cmd = self.cluster_renderer.render(template_name, self.script_path, self.task_job_id, stdout=self.stdout, stderr=self.stderr, nproc=self.nproc)
        stdout_h.write("Cluster command '{c}'".format(c=cluster_cmd))
        slog.debug(cluster_cmd)
        p = subprocess.Popen(cluster_cmd, shell=True, stdin=subprocess.PIPE, stdout=stderr_h, stderr=stdout_h, close_fds=True)

        # Loop until subprocess is completed, or self.event is set
        started_at = time.time()

        while p.returncode is None:
            p.poll()

            if self.shutdown_event.is_set():
                # This will only work if the QueueWorker is a Process (not a Thread)?
                p.terminate()
                # hard return
                run_time = time.time() - started_at
                output = (self.task_job_id, p.returncode, run_time, "Job Failed")
                self.out_queue.put(output)
                time.sleep(self.sleep_time)
                slog.info("Job id {i} -> subprocess ran for {x:.2f} sec.".format(x=run_time, i=self.task_job_id))

                # update the return code
                p.poll()
                break

            time.sleep(self.sleep_time)

        run_time = time.time() - started_at
        rcode, outs, err = p.returncode, "Job outs in {s:.2f} sec".format(s=run_time), "Job Error"

        # Fundamental output data
        output = (self.task_job_id, rcode, run_time, "Run by worker {n} Job {i} exit code {r}.".format(n=self.name, i=self.task_job_id, r=rcode))
        self.out_queue.put(output)

        time.sleep(self.sleep_time)

        slog.info("Worker {s} {n}: completed run()".format(s=self.__class__.__name__, n=self.name))


class ProcessPoolManager(multiprocessing.Process):

    def __init__(self, job_id, worker_shutdown_event, shutdown_event, in_q, out_q, max_workers, sleep_time=1, cluster_renderer=None):
        self.job_id = job_id
        self.max_workers = max_workers
        self.shutdown_event = shutdown_event
        self.worker_shutdown_event = worker_shutdown_event
        self.in_q = in_q
        self.out_q = out_q
        self.sleep_time = sleep_time
        assert isinstance(cluster_renderer, (types.NoneType, ClusterTemplateRender))
        self.cluster_renderer = cluster_renderer
        if self.cluster_renderer is None:
            self.worker_klass = EngineWorker
        else:
            self.worker_klass = ClusterEngineWorker
        multiprocessing.Process.__init__(self)

    def run(self):
        workers = {}
        if self.shutdown_event.is_set():
            log.info("Shutdown event is set. Shutting down Pool.")
            return

        while True:
            if self.shutdown_event.is_set():
                log.info("Got shutdown event.")
                self.worker_shutdown_event.set()
                # attempt to give time for workers to shutdown gracefully.
                time.sleep(self.sleep_time * 2)

                for worker, state in workers.iteritems():
                    log.info("Shutting down worker {w}".format(w=worker))
                    if worker.is_alive():
                        worker.terminate()

                break

            # delete old workers is they are done
            for worker in workers.keys():
                if not worker.is_alive():
                    log.info("Deleting worker {w}".format(w=worker))
                    del workers[worker]
                    # self.in_q.task_done()

            if not self.in_q.empty():
                while len(workers) < self.max_workers:
                    d = self.in_q.get()
                    log.info("Grabbed data {d} from in queue.".format(d=d))
                    task_job_id, script, stdout, stderr, nproc = d

                    if self.cluster_renderer is None:
                        w = EngineWorker(self.out_q, self.worker_shutdown_event, task_job_id, script, stdout, stderr, nproc, sleep_time=self.sleep_time)
                    else:
                        w = ClusterEngineWorker(self.out_q, self.worker_shutdown_event, task_job_id, script, stdout, stderr, nproc, self.cluster_renderer, sleep_time=self.sleep_time)

                    log.debug("Starting worker {w}".format(w=w))
                    w.start()
                    # maybe just use a set
                    workers[w] = 'started'
                    self.in_q.task_done()

            time.sleep(self.sleep_time)

        msg = "Exiting Pool.run()"
        print msg
        slog.info(msg)
