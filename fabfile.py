"""Util for General testing of settings and cluster config"""
import os
import tempfile
from pprint import pformat

from fabric.api import task, local, settings

import pbsmrtpipe
from pbsmrtpipe.cluster_templates import CLUSTER_TEMPLATE_DIR
from pbsmrtpipe.cluster import (load_cluster_templates_from_dir, ClusterTemplateRender)
from pbsmrtpipe.driver import _validate_input_xml, _validate_rc
from pbsmrtpipe.legacy.input_xml import InputXml, parse_core_xml_files


PACKAGE_DIR = os.path.dirname(os.path.dirname(pbsmrtpipe.__file__))
TEST_DATA_DIR = os.path.join(PACKAGE_DIR, 'tests', 'data')


def _make_tempfile(suffix):
    t = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    t.close()
    return t.name


@task
def test_cluster_template(template_name):
    """Run an sample program using the specified cluster configuration

    :param: template_name (str)

    Example: 'sge_pacbio' will look for pbsmrtpipe/cluster_templates
    """
    dir_name = os.path.join(CLUSTER_TEMPLATE_DIR, template_name)
    if os.path.exists(dir_name):
        print "Loading cluster configuration {t}".format(t=dir_name)
    else:
        raise IOError("Unable to find {t}".format(t=template_name))

    #command = "python --version"
    command = "pbsmrtpipe --help"

    cluster_templates = load_cluster_templates_from_dir(dir_name)

    r = ClusterTemplateRender(cluster_templates)

    shell_script = _make_tempfile('_job.sh')

    with open(shell_script, 'w') as f:
        f.write(command + "\n")

    print "Writing command '{c}' to {f}".format(c=command, f=shell_script)

    template_type = 'interactive'
    # For SGE, the job id must begin with a character
    job_id = 'c12345'
    nproc = 2
    stdout = _make_tempfile('_file.stdout')
    stderr = _make_tempfile('_file.stderr')

    qsub_command = r.render(template_type, shell_script,
                            job_id=job_id, nproc=nproc,
                            stderr=stderr, stdout=stdout)

    print "Running command '{s}'".format(s=qsub_command)
    # write script to temp dir
    out = local(qsub_command, capture=True)
    print "Test qsub command exit code {r}".format(r=out.return_code)

    return True


def _to_pbsmrtpipe_command(input_xml, workflow_xml, output_dir, rc=None,
                           chunked=False, debug=True):
    exe = "pbsmrtpipe"
    to_debug = " --debug " if debug else " "
    to_rc = " --rc={r}".format(r=rc) if rc is not None else " "
    to_chunked = ' --chunked ' if chunked else ' '

    stdout = os.path.join(output_dir, 'pipe.stdout')
    stderr = os.path.join(output_dir, 'pipe.stderr')

    d = dict(e=exe, d=to_debug, r=to_rc, i=input_xml, w=workflow_xml,
             o=output_dir, so=stdout, se=stderr, c=to_chunked)

    return "{e} {c} {d} {r} {i} {w} {o} > {so} 2> {se}".format(**d)


@task
def job_test(input_xml, workflow_xml, rc=None, chunked=False, debug=True):
    """Run a test job using the workflow.xml files in tests/data.

    :param: name (str) name of workflow.xml file in tests/data/
    :param: rc (str, None) Path to rc file
    :param: chunked (bool) Run in 'chunked' mode. (split by movies).
    """

    input_xml = os.path.join(TEST_DATA_DIR, input_xml)
    workflow_xml = os.path.join(TEST_DATA_DIR, workflow_xml)

    if not os.path.exists(input_xml):
        msg = "Unable to find InputXml in {w}".format(w=workflow_xml)
        msg += "\n Valid workflow names :\n"
        msg += pformat(os.listdir(TEST_DATA_DIR))
        IOError(msg)

    _validate_input_xml(input_xml)

    if rc is not None:
        rc = _validate_rc(rc)

    if not os.path.exists(workflow_xml):
        msg = "Unable to find WorkflowXml in {x}".format(x=workflow_xml)
        msg += "\n Valid workflow names :\n"
        msg += pformat(os.listdir(TEST_DATA_DIR))
        raise IOError(msg)

    n, _ = os.path.splitext(os.path.basename(workflow_xml))
    output_dir = tempfile.mkdtemp(prefix="pbsmrtpipe_job_{n}_".format(n=n))
    print "Creating tempdir for job output {o}".format(o=output_dir)

    cmd = _to_pbsmrtpipe_command(input_xml, workflow_xml, output_dir, rc=rc,
                                 chunked=chunked, debug=debug)

    with settings(warn_only=True):
        out = local(cmd, capture=True)
        print "command exit code {r}".format(r=out.return_code)

        if out.return_code != 0:
            # try to read in log file
            log_file = os.path.join(output_dir, 'pipe.log')
            if os.path.exists(log_file):
                print "log output {f}.".format(f=log_file)
                with open(log_file, 'r') as f:
                    print f.read()
                print "Job {f} failed with return code {r}".format(r=out.return_code, f=output_dir)
            else:
                print "Unable to find log file {f}".format(f=log_file)


@task
def validate_input_xml(input_xml):
    """Validate the movies in an input.xml file"""
    f = _validate_input_xml(input_xml)
    i = InputXml.from_file(f)
    print i
    for movie in i.movies:
        print movie


@task
def validate_workflow(input_xml, workflow_xml):
    """Validate a workflow described by input.xml and workflow.xml files"""
    job_output_dir = tempfile.mkdtemp(suffix='test_validate_workflow_')
    ws, module_names = parse_core_xml_files(input_xml, workflow_xml, job_output_dir)
    print "Successfully loaded {n} modules from {w}".format(w=workflow_xml, n=len(module_names))
    for module_name in module_names:
        print module_name
