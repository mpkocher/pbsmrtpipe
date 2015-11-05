"""
General func/tools used in pbsmrtpipe
"""
import os
import sys
import re
import time
import logging
import logging.config
import logging.handlers

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


def nfs_exists_check(ff):
    """
    Central place for all NFS hackery

    Return whether a file or a dir ff exists or not.
    Call listdir() instead of os.path.exists() to eliminate NFS errors.

    Added try/catch black hole exception cases to help trigger an NFS refresh

    :rtype bool:
    """
    try:
        # All we really need is opendir(), but listdir() is usually fast.
        os.listdir(os.path.dirname(os.path.realpath(ff)))
        # But is it a file or a directory? We do not know until it actually exists.
        if os.path.exists(ff):
            return True
        # Might be a directory, so refresh itself too.
        # Not sure this is necessary, since we already ran this on parent,
        # but it cannot hurt.
        os.listdir(os.path.realpath(ff))
        if os.path.exists(ff):
            return True
    except OSError:
        pass

    # The rest is probably unnecessary, but it cannot hurt.

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


def nfs_refresh(path, ntimes=3):
    # re-try
    while True:
        if nfs_exists_check(path):
            return True
        ntimes -= 1
        if ntimes <= 0:
            break
        time.sleep(1.0)
    log.warn("NFS refresh failed. unable to resolve {p}".format(p=path))
    return False


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
                found = nfs_exists_check(bas_file)
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


def get_default_logging_config_dict(master_log, master_level, pb_log, stdout_level):
    """Returns a dict configuration of the logger. """
    d = {
        'version': 1,
        'disable_existing_loggers': False,  # this fixes the problem
        'formatters': {
            'console': {
                'format': '[%(levelname)s] %(asctime)-15sZ %(message)s'
            },
            'standard': {
                'format': '[%(levelname)s] %(asctime)-15sZ [%(name)s] %(message)s'
            },
            'full': {
                'format': '[%(levelname)s] %(asctime)-15sZ [%(name)s %(funcName)s %(lineno)d] %(message)s'
            }
        },
        'filters': {
            "slog_filter": {
                '()': StdOutStatusLogFilter,
            }
        },
        'handlers': {
            'console': {
                'level': logging.getLevelName(stdout_level),
                'class': 'logging.StreamHandler',
                'formatter': 'console',
                'stream': 'ext://sys.stdout',
                'filters': ['slog_filter']
            },
            "debug_file_handler": {
                "class": 'logging.handlers.RotatingFileHandler',
                "level": logging.getLevelName(master_level),
                "formatter": "full",
                "filename": master_log,
                "maxBytes": "10485760",
                "backupCount": "20",
                "encoding": "utf8"
            },
            "info_file_handler": {
                "class": 'logging.handlers.RotatingFileHandler',
                "level": "INFO",
                "formatter": "standard",
                "filename": pb_log,
                "maxBytes": "10485760",
                "backupCount": "20",
                "encoding": "utf8",
                "filters": ['slog_filter']
            }
        },
        'loggers': {
            '': {
                'handlers': ['console', 'info_file_handler', 'debug_file_handler'],
                'level': 'DEBUG',
                'propagate': True
            }
        },
        'root': {
            'level': 'DEBUG',
            'handlers': ['console', 'debug_file_handler', 'info_file_handler']
        }

    }
    return d


def setup_internal_logs(master_log, master_level, pb_log, stdout_level):
    d = get_default_logging_config_dict(master_log, master_level, pb_log, stdout_level)
    logging.config.dictConfig(d)
    logging.Formatter.converter = time.gmtime
    return d

