"""Load Registered tasks, file types, pipelines or Chunk Operators.

Not Particularly thrilled with this model, howver, it is centralized.
"""

import os
import importlib
import logging
import sys
import functools

import pbsmrtpipe.models as M

log = logging.getLogger(__name__)

# Loading Caches
_REGISTERED_TASKS = None
# Task define in json files
_REGISTERED_STATIC_TASKS = None
_REGISTERED_FILE_TYPES = None
_REGISTERED_PIPELINES = None
_REGISTERED_OPERATORS = None


def load_all_installed_pb_tasks():
    return load_all_pb_tasks_from_python_module_name('pbsmrtpipe.pb_tasks')


def load_all_pb_tasks_from_python_module_name(name):
    """
    Load all python modules in pbsmrtpipe.task_module_di/

    All files with prepended with '_' are assumed to be internal to the package and
    are NOT loaded.
    """
    global _REGISTERED_TASKS  # cache-ing mechanism. this is not awesome

    if _REGISTERED_TASKS is None:

        m = importlib.import_module(name)

        # this is kinda gross
        d = os.path.dirname(m.__file__)

        for x in os.listdir(d):
            if x.endswith(".py"):
                # Ignore are files with _my_file.py. These are assumed to be
                # internal to the package
                if not x.startswith('_'):
                    b, _ = os.path.splitext(x)
                    m_name = ".".join([name, b])
                    try:
                        _ = importlib.import_module(m_name)
                    except ImportError:
                        msg = "Failed in dynamically import '{x}' -> '{m}'".format(x=x, m=m_name)
                        log.error(msg)
                        raise
                    except KeyError:
                        msg = "Duplicate task id. '{x}' -> '{m}'".format(x=x, m=m_name)
                        log.error(msg)
                        raise

        _REGISTERED_TASKS = M.REGISTERED_TASKS

    return _REGISTERED_TASKS



def _load_all_pb_static_tasks(registered_tasks_d, filter_filename_func, processing_func):

    import pbsmrtpipe.pb_io as IO

    m = importlib.import_module("pbsmrtpipe.pb_static_tasks")

    d = os.path.dirname(m.__file__)
    log.debug("Loading static meta tasks from {m}".format(m=d))

    for x in os.listdir(d):
        if filter_filename_func(x):
            json_file = os.path.join(d, x)
            try:
                meta_task = processing_func(json_file)
                log.info(meta_task)
                registered_tasks_d[meta_task.task_id] = meta_task
            except Exception as e:
                log.error("Failed loading Static Task from '{x}'".format(x=json_file))
                sys.stderr.write(e.message + "\n")
                log.error(e.message)
                raise

    return _REGISTERED_STATIC_TASKS


def load_all_pb_static_tasks():

    import pbsmrtpipe.pb_io as IO

    # this is gross.
    global _REGISTERED_STATIC_TASKS

    if _REGISTERED_STATIC_TASKS is None:
        _REGISTERED_STATIC_TASKS = {}

    def filter_by(name, path):
        return path.endswith(".json") and name in path

    filter_manifests = functools.partial(filter_by, "static_manifest")
    filter_contracts = functools.partial(filter_by, "tool_contract")

    rtasks = _load_all_pb_static_tasks(_REGISTERED_STATIC_TASKS, filter_manifests, IO.load_static_meta_task_from_file)
    rtasks = _load_all_pb_static_tasks(rtasks, filter_contracts, IO.tool_contract_to_meta_task_from_file)

    return rtasks


def load_xml_chunk_operators_from_python_module_name(name):
    import pbsmrtpipe.pb_io as IO
    m = importlib.import_module(name)
    d = os.path.dirname(m.__file__)

    operators = []
    for x in os.listdir(d):
        if x.endswith(".xml"):
            p = os.path.join(d, x)
            operator = IO.parse_operator_xml(p)
            operators.append(operator)

    log.debug("Loaded {o} operators from {n} -> {d}".format(o=len(operators), n=name, d=d))
    return {op.idx: op for op in operators}


def load_all_installed_chunk_operators():
    global _REGISTERED_OPERATORS
    if _REGISTERED_OPERATORS is None:
        _REGISTERED_OPERATORS = load_xml_chunk_operators_from_python_module_name("pbsmrtpipe.chunk_operators")

    return _REGISTERED_OPERATORS


def load_pipelines_from_python_module_name(name):
    # FIXME This is terrible form. Need to update the loading to be configurable to
    # dynamically load pipelines
    # it's a dict and sometimes is a list of registered resources
    # m = importlib.import_module(name)

    global _REGISTERED_PIPELINES

    if _REGISTERED_PIPELINES is None:
        import pbsmrtpipe.pb_pipelines
        import pbsmrtpipe.pb_pipelines_dev
        import pbsmrtpipe.pb_pipelines_sa3
        from pbsmrtpipe.models import REGISTERED_PIPELINES
        _REGISTERED_PIPELINES = REGISTERED_PIPELINES

    return _REGISTERED_PIPELINES


def load_all_installed_pipelines():
    # FIXME
    return load_pipelines_from_python_module_name("")


def load_all_registered_file_types():
    global _REGISTERED_FILE_TYPES
    if _REGISTERED_FILE_TYPES is None:
        from pbsmrtpipe.models import REGISTERED_FILE_TYPES
        _REGISTERED_FILE_TYPES = REGISTERED_FILE_TYPES
    return _REGISTERED_FILE_TYPES


def load_all():
    """
    Load all resources and return a tuple of (MetaTasks, FileTypes, ChunkOperators, Pipelines)

    :note: This will only be loaded once and cached.
    """
    meta_tasks = load_all_installed_pb_tasks()
    static_metatasks = load_all_pb_static_tasks()
    meta_tasks.update(static_metatasks)
    operators = load_all_installed_chunk_operators()
    pipelines = load_all_installed_pipelines()

    from pbsmrtpipe.core import REGISTERED_FILE_TYPES
    return meta_tasks, REGISTERED_FILE_TYPES, operators, pipelines



