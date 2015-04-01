import os
import shutil
import logging

from fabric.api import local, task
from fabric.context_managers import lcd
from pbsmrtpipe.utils import setup_log

log = logging.getLogger(__name__)
_EXE = 'pbtestkit-runner'


def _parse_file(fofn):
    base_dir = os.path.dirname(fofn)

    def _to_p(p):
        if os.path.isabs(p):
            return p
        else:
            x = os.path.join(base_dir, p)
            return os.path.abspath(x)

    butler_cfgs = []
    with open(fofn, 'r') as f:
        for line in f:
            b = _to_p(line.strip())
            if not os.path.exists(b):
                raise IOError("Unable to find '{v}'".format(v=b))
            butler_cfgs.append(b)

    return butler_cfgs


def _to_cmd(exe, butler_cfg):
    return "{e} --debug {c}".format(e=exe, c=butler_cfg)


@task
def run_fofn(butler_cfg_fofn, log_file=None):
    """
    Run fofn of butler.cfg files
    :param butler_cfg_fofn:
    """
    # list of absolute file paths
    butler_cfgs = _parse_file(butler_cfg_fofn)

    if log_file is not None:
        setup_log(log, level=logging.DEBUG, file_name=log_file)

    for butler_cfg in butler_cfgs:
        cmd = _to_cmd(_EXE, butler_cfg)
        dir_name = os.path.dirname(butler_cfg)
        with lcd(dir_name):
            log.info(cmd)
            output = local(cmd)
            log.info(output)
            print output


@task
def cleaner():
    base_dir = os.path.dirname(__file__)

    n = 0
    c = 0
    for x in os.listdir(base_dir):
        n += 1
        p = os.path.join(base_dir, x, 'job_output')
        if os.path.exists(p):
            c += 1
            shutil.rmtree(p)

    print "Found {n} job dirs. Cleaned {x} job dirs.".format(n=n, x=c)
