"""
General func/tools used in pbsmrtpipe
"""
import os
import sys
import re
import time
import logging
import functools

from jinja2 import Environment, PackageLoader

from pbcore.util.Process import backticks
# for backward compatibility

from pbcommand.utils import setup_log, compose

from pbsmrtpipe.decos import ignored
from pbsmrtpipe.constants import SLOG_PREFIX

HTML_TEMPLATE_ENV = Environment(loader=PackageLoader('pbsmrtpipe', 'html_templates'))


log = logging.getLogger(__name__)
slog = logging.getLogger(SLOG_PREFIX + __name__)


def validate_type_or_raise(obj, klasses, msg=None):
    if not isinstance(obj, klasses):
        emsg = "{o} Got type {x}, expected type {y}.".format(o=obj, x=type(obj), y=klasses)
        if msg is not None:
            emsg = " ".join([emsg, msg])
        raise TypeError(emsg)
    return obj


def _validate_resource(func, resource):
    """Validate the existence of a file/dir"""
    if func(resource):
        return os.path.abspath(resource)
    else:
        raise IOError("Unable to find {f}".format(f=resource))

validate_file = functools.partial(_validate_resource, os.path.isfile)
validate_dir = functools.partial(_validate_resource, os.path.isdir)
validate_output_dir = functools.partial(_validate_resource, os.path.isdir)


def _nfs_exists_check(ff):
    """Return whether a file or a dir ff exists or not.
    Call ls instead of python os.path.exists to eliminate NFS errors.
    """
    # this is taken from Yuan
    cmd = "ls %s" % ff
    output, errCode, errMsg = backticks(cmd)
    return errCode == 0


def validate_fofn(fofn):
    """Validate existence of FOFN and files within the FOFN.

    :param fofn: (str) Path to File of file names.
    :raises: IOError if any file is not found.
    :return: (str) abspath of the input fofn
    """
    if os.path.isfile(fofn):
        file_names = fofn_to_files(os.path.abspath(fofn))
        log.debug("Found {n} files in FOFN {f}.".format(n=len(file_names), f=fofn))
        return os.path.abspath(fofn)
    else:
        raise IOError("Unable to find {f}".format(f=fofn))


def fofn_to_files(fofn):
    """Util func to convert a bas/bax fofn file to a list of bas/bax files."""
    if os.path.exists(fofn):
        with open(fofn, 'r') as f:
            bas_files = [line.strip() for line in f.readlines()]

        for bas_file in bas_files:
            if not os.path.isfile(bas_file):
                # try one more time to find the file by
                # performing an NFS refresh
                found = _nfs_exists_check(bas_file)
                if not found:
                    raise IOError("Unable to find Fofn file '{f}'".format(f=bas_file))

        if len(set(bas_files)) != len(bas_files):
            raise IOError("Detected duplicate files in fofn. {n} unique, {m} total files in {f}".format(f=fofn, m=len(bas_files), n=len(set(bas_files))))

        return bas_files
    else:
        raise IOError("Unable to find FOFN {f}".format(f=fofn))


def log_timing(func):
    """Simple deco to log the runtime of func"""
    started_at = time.time()

    def wrapper(*args, **kw):
        return func(*args, **kw)

    run_time = time.time() - started_at
    name = func.__name__
    log.info("Func {f} took {s:.2f} sec ({m:.2f} min)".format(f=name, s=run_time, m=run_time / 60.0))

    return wrapper


class StdOutStatusLogFilter(logging.Filter):

    def filter(self, record):
        return record.name.startswith(SLOG_PREFIX)


def is_verified(path, max_nfs_refresh=3):
    """Validate that a file exists. Force NFS refresh if necessary"""
    for i in xrange(max_nfs_refresh):
        with ignored(OSError):
            # Try to force an NFS refresh
            os.listdir(os.path.dirname(path))
            if os.path.exists(path):
                return True
            # time.sleep(0.25)

    return False


def get_default_logging_config_dict():
    """Returns a dict configuration of the logger."""
    d = {
        'version': 1,
        'disable_existing_loggers': False,  # this fixes the problem
        'formatters': {
            'standard': {
                'format': '[%(levelname)s] %(asctime)-15s [%(name)s %(funcName)s %(lineno)d] %(message)s'
            },
        },
        'handlers': {
            'default': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
                'stream': 'ext://sys.stdout'
            },
            "error_file_handler": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "ERROR",
                "formatter": "standard",
                "filename": "errors.log",
                "maxBytes": "10485760",
                "backupCount": "20",
                "encoding": "utf8"
            },
            "info_file_handler": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "formatter": "standard",
                "filename": "info.log",
                "maxBytes": "10485760",
                "backupCount": "20",
                "encoding": "utf8"
            }
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': 'DEBUG',
                'propagate': True
            }
        },
        'root': {
            'level': 'DEBUG',
            'handlers': ['default', 'error_file_handler', 'info_file_handler']
        }

    }
    return d
