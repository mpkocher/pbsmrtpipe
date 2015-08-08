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
from pbcommand.utils import setup_log

from pbsmrtpipe.decos import ignored
from pbsmrtpipe.constants import SLOG_PREFIX

HTML_TEMPLATE_ENV = Environment(loader=PackageLoader('pbsmrtpipe', 'html_templates'))


log = logging.getLogger(__name__)
slog = logging.getLogger(SLOG_PREFIX + __name__)


def compose(*funcs):
    """Functional composition

    [f, g, h] will be f(g(h(x)))
    """
    def compose_two(f, g):
        def c(x):
            return f(g(x))
        return c
    return functools.reduce(compose_two, funcs)


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


def p4_changelist_to_version(major_version, perforce_str):
    """
    Util to get the version from a perforce tags (KCS).

    :param major_version: (str, int, float)
    :param perforce_str: "$Change: 12345$"
    :return str: "1.1.1234"
    """
    rx = re.compile(r'Change: (\d+)')
    match = rx.search(perforce_str)
    if match is None:
        v = 'UnknownVersion'
    else:
        v = match.group(1)
    return '%s.%s' % (str(major_version), v)


def validate_movie_file_name(file_name):
    """Validate the movie file name

    m120201_042231_42129_c100275262550000001523007907041260_s1_p0.bas.h5
    m130715_185638_SMRT1_c000000062559900001500000112311501_s1_p0

    multipart files:

    m130306_023456_42129_c100422252550000001523053002121396_s1_p0.2.bax.h5
    m130306_023456_42129_c100422252550000001523053002121396_s1_p0.1.bax.h5
    m130306_023456_42129_c100422252550000001523053002121396_s1_p0.3.bax.h5
    """
    bas_ext = '.bas.h5'
    bax_ext = '.bax.h5'
    supported_exts = [bas_ext, bax_ext]

    if not (file_name.endswith(bas_ext) or file_name.endswith(bax_ext)):
        raise ValueError("Unsupported file extension for '{f}'. Supported extensions {s}".format(f=file_name, s=supported_exts))

    bas_rx = re.compile(r'm\d{6}_\d{6}_[A-Za-z0-9]*_c\d{32}[0-7]_s\d_p0')
    bax_rx = re.compile(r'm\d{6}_\d{6}_[A-Za-z0-9]*_c\d{32}[0-7]_s\d_p0\.[1-3]')
    rxs = {'.bas': bas_rx, '.bax': bax_rx}

    movie, ext = os.path.splitext(os.path.splitext(os.path.basename(file_name))[0])
    rx = rxs[ext]
    return rx.search(file_name) is not None


def validate_movie_name_deco(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        validate_movie_file_name(*args, **kwargs)
        return func(*args, **kwargs)

    return wrapper


def movie_to_cell(movie):
    """
    The movie file name has the 'cell' number encoded in the name

    m130715_185638_SMRT1_c000000062559900001500000112311501_s1_p0
                                                          ^
                                                          |
    The cell number is a number between 0-7

    :param movie: (str) movie file full path or basename
    :return: (str) movie cell number
    """
    return '_'.join(os.path.basename(movie).split('_')[:4])


def movie_to_set(movie):
    """
    This it not a python set!

    The movie file name has the 'set' number encoded in the name

    m130715_185638_SMRT1_c000000062559900001500000112311501_s1_p0
                                                             ^
                                                             |
    The set number is 1

    :param movie: (str) movie file full path or basename
    :return: (str) movie set
    """
    return movie.split('_')[4][1:]


def object_memory_profiler(objects, fileName):
    """Write Memory objects to dot file

    :param: list of objects to be profiled
    :param: fileName: filename to write graph to (e.g, name.dot)
    """
    log.info("Running in Object Memory Profiling Mode.")

    t0 = time.time()
    try:
        import objgraph

        objgraph.show_refs(objects, filename=fileName)
    except Exception as e:
        log.debug("Unable to run object Memory Profile due to {e}".format(e=e))

    run_time = time.time() - t0
    log.debug(
        "completed Memory Object profiling in {s:.2f} sec.".format(s=run_time))


class Singleton(type):

    """
    General Purpose singleton class

    Usage:

    >>> class MyClass(object):
    >>>     __metaclass__ = Singleton
    >>>     def __init__(self):
    >>>         self.name = 'name'

    """

    def __init__(cls, name, bases, dct):
        super(Singleton, cls).__init__(name, bases, dct)
        cls.instance = None

    def __call__(cls, *args, **kw):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args)
        return cls.instance


class StdOutStatusLogFilter(logging.Filter):

    def filter(self, record):
        return record.name.startswith(SLOG_PREFIX)


def which(exe_str):
    """walk the exe_str in PATH to get current exe_str.

    If path is found, the full path is returned. Else it returns None.
    """
    paths = os.environ.get('PATH', None)
    state = None

    if paths is None:
        # log warning
        msg = "PATH env var is not defined."
        log.error(msg)
        return state

    for path in paths.split(":"):
        exe_path = os.path.join(path, exe_str)
        # print exe_path
        if os.path.exists(exe_path):
            state = exe_path
            break

    return state


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
