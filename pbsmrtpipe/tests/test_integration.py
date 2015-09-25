import os
import logging
import random
import shutil
import unittest
import functools

from nose.plugins.attrib import attr

import pbsmrtpipe.cluster as C
from pbsmrtpipe.engine import backticks, run_command

from base import (TEST_DATA_DIR, HAS_CLUSTER_QSUB, get_temp_cluster_dir, SLOW_ATTR, get_temp_dir, get_temp_file)

log = logging.getLogger(__name__)

_EXE = 'pbsmrtpipe'


class _TestIntegrationHelp(unittest.TestCase):
    CMD = "{e} pipeline --help".format(e=_EXE)

    def test_help(self):
        rcode, stdout, stderr, run_time = backticks(self.CMD)
        self.assertEqual(rcode, 0)


class TestIntegrationHelp(_TestIntegrationHelp):
    CMD = '{e} task --help'.format(e=_EXE)


@unittest.skipIf(not HAS_CLUSTER_QSUB, "Cluster is not accessible")
class TestHelloWorldCluster(unittest.TestCase):

    def test_hello_world_job(self):
        r = C.load_installed_cluster_templates_by_name('sge')
        log.debug(r)

        job_name = "int_job_hello"
        output_dir = get_temp_cluster_dir(job_name)

        cmd = "pbsmrtpipe --help"

        def _to_p(x_):
            return os.path.join(output_dir, x_)

        sh_script = _to_p('qsub_test.sh')

        with open(sh_script, 'w') as f:
            f.write(cmd + "\n")

        # qsub output
        stdout = _to_p('stdout')
        stderr = _to_p('stderr')

        for x in [stdout, stderr]:
            with open(x, 'w') as f:
                f.write("")

        log.info(sh_script)
        cmd = r.render("start", sh_script, 'test_job_01', stdout=stdout, stderr=stderr, nproc=1)
        log.debug("Running qsub command '{c}'".format(c=cmd))
        time_out = 60 * 5
        rcode, stdout, stderr, run_time = run_command(cmd, None, None, time_out=time_out)
        log.debug((rcode, stdout, stderr, run_time))

        if rcode != 0:
            log.info(stdout)
            log.error(stderr)
            log.error("Failed Integration Job {i} with exit code {r}".format(i=job_name, r=rcode))
            if os.path.exists(stderr):
                with open(stderr, 'r') as f:
                    log.error(f.read())
        else:
            try:
                shutil.rmtree(output_dir)
            except Exception as e:
                log.warn("Unable to cleanup testdir {o}. {m}".format(o=output_dir, m=e.message))

        self.assertEqual(rcode, 0, stderr)


def __to_cmd(pipeline_subparser, job_dir, workflow_xml_or_pipeline_id, preset_xml, ep_d):
    preset_xml_str = '' if preset_xml is None else ' --preset-xml {p}'.format(p=preset_xml)
    e = ['-e "{i}:{p}"'.format(i=i, p=p) for i, p in ep_d.iteritems()]
    _d = dict(e=' '.join(e), p=preset_xml_str, w=workflow_xml_or_pipeline_id, d=job_dir, s=pipeline_subparser)
    return "pbsmrtpipe {s} {w} --debug --output-dir={d} {p} {e}".format(**_d)

_to_pipeline_cmd = functools.partial(__to_cmd, "pipeline")
_to_pipeline_id_cmd = functools.partial(__to_cmd, "pipeline-id")


@attr(SLOW_ATTR)
class _TestDriverIntegrationBase(unittest.TestCase):
    JOB_NAME = "my_job"
    PRESET_XML = None
    WORKFLOW_XML = ''
    ENTRY_POINTS = {}
    TO_CMD_FUNC = _to_pipeline_cmd

    def _get_root_temp_dir(self):
        """Override me to set the shared tmp dir"""
        return get_temp_cluster_dir(self.JOB_NAME)

    def test_run(self):

        root_output_dir = self._get_root_temp_dir()
        i = random.randint(1, 10000)
        name = "{n}_{i}".format(n=self.JOB_NAME, i=i)
        output_dir = os.path.join(root_output_dir, name)
        os.mkdir(output_dir)

        ep_d = {ep_id: get_temp_file(suffix=name) for ep_id, name in self.ENTRY_POINTS.iteritems()}

        for ep_id, file_name in ep_d.iteritems():
            with open(file_name, 'w') as x:
                x.write("Mock data for {i} \n".format(i=ep_id))

        cmd = self.TO_CMD_FUNC(output_dir, self.WORKFLOW_XML, self.PRESET_XML, ep_d)

        stderr_path = os.path.join(output_dir, 'job.stderr')
        stdout_path = os.path.join(output_dir, 'job.stdout')
        log.debug(cmd)
        with open(stdout_path, 'w') as wo:
            with open(stderr_path, 'w') as we:
                rcode, stdout_results, stderr_results, run_time = run_command(cmd, wo, we)

        log.debug("Integration Job {i} state {s} in {t:.2f} sec.".format(i=self.JOB_NAME, s=rcode, t=run_time))

        if rcode != 0:
            log.error("Integration Job {i} failed.".format(i=self.JOB_NAME))
            log.error(stdout_results)
            log.error(stderr_results)
            if os.path.exists(stderr_path):
                with open(stderr_path, 'r') as f:
                    log.error(f.read())

        emsg = "Failed Integration Job {i} with exit code {r} in {d}. {w}".format(i=self.JOB_NAME, r=rcode, d=output_dir, w=self.WORKFLOW_XML)
        self.assertEqual(rcode, 0, emsg)


class TestHelloWorldWorkflow(_TestDriverIntegrationBase):
    JOB_NAME = 'hello_world'
    PRESET_XML = os.path.join(TEST_DATA_DIR, 'hello_world_preset.xml')
    WORKFLOW_XML = os.path.join(TEST_DATA_DIR, 'hello_world_workflow.xml')
    ENTRY_POINTS = {'e_01': "hello_entry_point.txt"}


@unittest.skipIf(not HAS_CLUSTER_QSUB, "No qsub exe found.")
class TestHelloWorldDistributed(TestHelloWorldWorkflow):
    JOB_NAME = 'hello_world_distributed'
    WORKFLOW_XML = os.path.join(TEST_DATA_DIR, 'hello_world_distributed_workflow.xml')

    def _get_root_temp_dir(self):
        return get_temp_cluster_dir(self.JOB_NAME)


class _TestDriverPipelineId(_TestDriverIntegrationBase):
    TO_CMD_FUNC = _to_pipeline_id_cmd
