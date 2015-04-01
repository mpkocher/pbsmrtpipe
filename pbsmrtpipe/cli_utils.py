import os
import functools
import time
import sys
import logging
import traceback


from pbcore.util.Process import backticks
from pbsmrtpipe.utils import setup_log

log = logging.getLogger(__name__)


def trigger_nfs_refresh(ff):
    """
    Central place for all NFS hackery

    Return whether a file or a dir ff exists or not.
    Call ls instead of python os.path.exists to eliminate NFS errors.

    Added try/catch black hole exception cases to help trigger an NFS refresh

    :rtype bool:
    """
    # try to trigger refresh for File case
    try:
        f = open(ff, 'r')
        f.close()
    except Exception:
        pass

    # try to trigger refresh for Directory case
    try:
        _ = os.stat(ff)
        _ = os.listdir(ff)
    except Exception:
        pass

    # Call externally
    # this is taken from Yuan
    cmd = "ls %s" % ff
    _, rcode, _ = backticks(cmd)

    return rcode == 0


def _trigger_nfs_refresh_and_ignore(ff):
    """

    :rtype str
    """
    _ = trigger_nfs_refresh(ff)
    return ff


def _validate_resource(func, resource):
    """Validate the existence of a file/dir"""
    # Attempt to trigger an NFS metadata refresh
    _ = trigger_nfs_refresh(resource)

    if func(resource):
        return os.path.abspath(resource)
    else:
        raise IOError("Unable to find resource '{f}'".format(f=resource))

validate_file = functools.partial(_validate_resource, os.path.isfile)
validate_dir = functools.partial(_validate_resource, os.path.isdir)
validate_output_dir = functools.partial(_validate_resource, os.path.isdir)


def validate_report(report_file_name):
    """
    Raise ValueError if report contains path seps
    """
    if not os.path.basename(report_file_name) == report_file_name:
        raise ValueError("Path separators are not allowed: {r}".format(r=report_file_name))
    return report_file_name


def args_executer(args):
    """


    :rtype int
    """
    try:
        return_code = args.func(args)
    except Exception as e:
        log.error(e, exc_info=True)
        traceback.print_exc(sys.stderr)
        if isinstance(e, IOError):
            return_code = 1
        else:
            return_code = 2

    return return_code


def main_runner(argv, parser, exe_runner_func, setup_log_func, alog):
    """
    Fundamental interface to commandline applications
    """
    started_at = time.time()
    args = parser.parse_args(argv)
    #log.debug(args)

    # setup log
    if hasattr(args, 'debug'):
        if args.debug:
            setup_log_func(alog, level=logging.DEBUG)
        else:
            alog.addHandler(logging.NullHandler())
    else:
        alog.addHandler(logging.NullHandler())

    log.debug(args)
    alog.info("Starting tool version {v}".format(v=parser.version))
    rcode = exe_runner_func(args)

    run_time = time.time() - started_at
    _d = dict(r=rcode, s=run_time)
    alog.info("exiting with return code {r} in {s:.2f} sec.".format(**_d))
    return rcode


def main_runner_default(argv, parser, alog):
    # this has a loose assumption that debug has been defined in the parser
    setup_log_func = setup_log
    return main_runner(argv, parser, args_executer, setup_log_func, alog)