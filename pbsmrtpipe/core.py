"""A lot of this file should be pushed into models.py or deleted"""
import inspect
import logging
import copy
import sys
import types
import functools

from pbcommand.models import (FileTypes, TaskTypes, SymbolTypes, ResourceTypes, FileType)
from pbsmrtpipe.exceptions import (MalformedMetaTaskError,
                                   MalformedPipelineError)

from pbsmrtpipe.constants import RX_VERSION, RX_CHUNK_KEY
from pbsmrtpipe.constants import (ENTRY_PREFIX,
                                  RX_BINDING_TASK,
                                  RX_VALID_BINDINGS,
                                  RX_ENTRY,
                                  RX_TASK_ID, RX_BINDING_PIPELINE_ENTRY,
                                  RX_BINDING_PIPELINE_TASK)

from pbsmrtpipe.models import (MetaTask, MetaScatterTask,
                               MetaGatherTask, Pipeline,
                               REGISTERED_FILE_TYPES,
                               REGISTERED_PIPELINES,
                               REGISTERED_TASKS)
from pbsmrtpipe.utils import validate_type_or_raise


log = logging.getLogger(__name__)

__all__ = ['MetaTaskBase', 'MetaScatterTaskBase', "MetaGatherTaskBase"]


def _binding_entry_points_to_tuple(bs):
    f = lambda x_: x_.startswith(ENTRY_PREFIX)
    eps = [(x, y) for x, y in bs if f(x)]
    bs = [(x, y) for x, y in bs if not f(x)]
    return eps, bs


def is_validate_binding_str(s):
    for rx in RX_VALID_BINDINGS:
        m = rx.match(s)
        if m is not None:
            return True
    m = ", ".join([x.pattern for x in RX_VALID_BINDINGS])
    raise ValueError("Binding str '{s}' is not valid. Must match {m}".format(s=s, m=m))


def does_pipeline_have_task_id(p, task_binding_str):
    for b_out, b_in in p.bindings:
        if task_binding_str in (b_out, b_in):
            return True
    return False


def _binding_str_match(rx, s):
    m = rx.match(s)
    return True if m is not None else False


def binding_str_is_entry_id(s):
    return _binding_str_match(RX_ENTRY, s)


def binding_str_is_pipeline_task_str(s):
    return _binding_str_match(RX_BINDING_PIPELINE_TASK, s)


def binding_str_is_task_id(s):
    return _binding_str_match(RX_BINDING_TASK, s)


def get_task_binding_str_from_pipeline_task_str(s):
    gs = RX_BINDING_PIPELINE_TASK.match(s).groups()
    return ".".join([gs[2], 'tasks', gs[3]]) + ":" + gs[4]


def get_pipeline_id_from_pipeline_task_str(s):
    gs = RX_BINDING_PIPELINE_TASK.match(s).groups()
    return ".".join([gs[0], 'pipelines', gs[1]])


def get_pipeline_id_from_pipeline_entry_str(s):
    gs = RX_BINDING_PIPELINE_ENTRY.match(s).groups()
    return ".".join([gs[0], 'pipelines', gs[1]])


def get_entry_label_from_pipeline_entry_str(s):
    gs = RX_BINDING_PIPELINE_ENTRY.match(s).groups()
    return "{e}:{i}".format(e=ENTRY_PREFIX, i=gs[2])


def parse_pipeline_id(s):
    m = RX_BINDING_PIPELINE_TASK.match(s)
    gs = m.groups()
    return ".".join([gs[0], 'pipelines', gs[1]])


def _load_existing_pipeline(p, p_existing):
    """Add existing tasks and entry points into Pipeline p"""
    # check if already loaded
    if p_existing.pipeline_id in p.parent_pipeline_ids:
        log.info("Skipping loading. Pipeline was already loaded")
        return

    log.debug("[Loading entry points] from {p} into {i}".format(p=p_existing.pipeline_id, i=p.pipeline_id))
    for e_out, b_in in p_existing.entry_bindings:
        p.entry_bindings.append((e_out, b_in))

    log.debug("[Loading bindings]")
    for b_out, b_in in p_existing.bindings:
        p.bindings.append((b_out, b_in))

    # add parent history
    for p_id in p_existing.parent_pipeline_ids:
        p.parent_pipeline_ids.append(p_id)

    p.parent_pipeline_ids.append(p_existing.pipeline_id)

    return p


def _load_existing_pipeline_or_raise(pipelines_d, p, p_existing_id):
    if p_existing_id not in pipelines_d.keys():
        raise ValueError("Pipeline '{i}' required pipeline '{o}' to be defined.".format(i=p.pipeline_id, o=p_existing_id))

    p_existing = pipelines_d[p_existing_id]
    _load_existing_pipeline(p, p_existing)


def load_pipeline_bindings(registered_pipeline_d, pipeline_id, display_name, description, bs):
    """
    Mutate the registered pipelines registry

    :param registered_pipeline_d:
    :param pipeline_id:
    :param bs: list of binding strings [(a, b), ]

    :return: mutated pipeline registry
    """
    # only use unique pairs
    bs = list({x for x in bs})

    log.debug("Processing pipeline {i}".format(i=pipeline_id))
    # str, [(in, out)] [(in, out)]
    pipeline = Pipeline(pipeline_id, display_name, description, [], [])

    for x in bs:
        validate_type_or_raise(x, (tuple, list))
        if len(x) != 2:
            raise TypeError("Binding Strings must be provided a 2-tuple of strings")

        b_out, b_in = x

        for x in (b_out, b_in):
            is_validate_binding_str(x)

        # Is it an Entry Point
        if binding_str_is_entry_id(b_out):
            # 3 cases, b_in is a
            # - task_id
            # - pipeline_id:entry_label (Rebound entry label)
            # - pipeline_id:task_id (Using the output of an existing task in the pipeline)
            # b_in could be a pipeline id or a task id

            if binding_str_is_pipeline_task_str(b_in):
                # print ("entry point -> pipeline", b_in)
                # Need to load existing pipeline
                # pipeline.entry_bindings.append((b_out, b_in))
                # print "(load pipeline) entry points need to load existing pipeline for tasks and entry points", b_in
                pass
            elif binding_str_is_task_id(b_in):
                # ($entry:e_01, "pbsmrtpipe.tasks.dev_task_01:0)
                pipeline.entry_bindings.append((b_out, b_in))
            elif _binding_str_match(RX_BINDING_PIPELINE_ENTRY, b_in):
                # ($entry:e_01, pbsmrtpipe.pipelines.pipeline_id_1:$entry:e_02)
                pi_id = get_pipeline_id_from_pipeline_entry_str(b_in)
                e_label = get_entry_label_from_pipeline_entry_str(b_in)
                _load_existing_pipeline_or_raise(registered_pipeline_d, pipeline, pi_id)
                log.info("entry points -> pipeline:$entry format '{n}'".format(n=b_in))
                log.debug("(re-bind) entry points need to load exiting pipeline for tasks and entry points")
            else:
                raise MalformedPipelineError("Unsupported value {b}".format(b=b_in))

        # is regular task -> task bindings
        elif binding_str_is_task_id(b_out):
            # simplest case
            # print ("task -> task binding", b_out, b_in)
            pipeline.bindings.append((b_out, b_in))
        elif _binding_str_match(RX_BINDING_PIPELINE_TASK, b_out):
            # pbsmrtpipe.pipelines.dev_01:pbsmrtpipe.tasks.dev_hello_world:0
            # needs to load existing pipeline bindings and entry points
            # then create a new binding of ("pbsmrtpipe.tasks.dev_hello_world:0", b_in)
            task_binding_str = get_task_binding_str_from_pipeline_task_str(b_out)

            pl_id = get_pipeline_id_from_pipeline_task_str(b_out)
            _load_existing_pipeline_or_raise(registered_pipeline_d, pipeline, pl_id)

            pipeline.bindings.append((task_binding_str, b_in))
            # print ("pipeline task binding", b_out, b_in)
        else:
            raise MalformedPipelineError("Unhandled binding case '{o}' -> '{i}'".format(o=b_out, i=b_in))

        log.info("registering pipeline {i}".format(i=pipeline.pipeline_id))
        registered_pipeline_d[pipeline.pipeline_id] = pipeline

    return registered_pipeline_d


def register_pipeline(pipeline_id, display_name):

    def deco_wrapper(func):

        if pipeline_id in REGISTERED_PIPELINES:
            log.warn("'{i}' has already been registered.".format(i=pipeline_id))

        bs = func()
        load_pipeline_bindings(REGISTERED_PIPELINES, pipeline_id, display_name, func.__doc__, bs)

        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return deco_wrapper


def to_list_if_necessary(tuple_or_s):
    if isinstance(tuple_or_s, tuple):
        return list(tuple_or_s)
    return tuple_or_s


def _register_or_raise(meta_task):
    if meta_task.task_id in REGISTERED_TASKS:
        msg = "Task id {i} is already registered.".format(i=meta_task.task_id)
        log.warn(msg)
        # raise KeyError("Task id {i} is already registered.".format(i=meta_task.task_id))
    else:
        REGISTERED_TASKS[meta_task.task_id] = meta_task
        log.debug("Successfully registered task_id {i} {t}".format(i=meta_task.task_id, t=meta_task))

    return meta_task


def __validate_output_file_names(output_types, output_file_names):
    errors = []
    if isinstance(output_file_names, (list, tuple)):
        for x in output_file_names:
            if isinstance(x, (list, tuple)):
                if len(x) == 2:
                    pass
                else:
                    errors.append("Malformed output file name {x}. Expected 2-tuple (str, str).".format(x=x))
        if len(output_file_names) == len(output_types):
            # this is only branch where the outputs and override outputs file names are valid
            return True
        else:
            errors.append("Malformed output file names. Expected {n}. Got {m}".format(n=output_types, m=output_file_names))
    else:
        errors.append("Malformed output file name. Expected [(str, str)]. Got {m}".format(m=output_file_names))

    log.error("\n".join(errors))
    raise ValueError("\n".join(errors))


def _validate_output_file_names(output_types, output_files_names_or_none):
    if output_files_names_or_none is not None:
        __validate_output_file_names(output_types, output_files_names_or_none)
        out_names = output_files_names_or_none
    else:
        out_names = []

    return out_names


def _validate_output_file_names_or_raise(output_types, output_file_names_or_none, task_id):
    try:
        return _validate_output_file_names(output_types, output_file_names_or_none)
    except Exception:
        msg = "task id {i} Output file types ({n}) and override output file names ({m}) supplied are NOT compatible ".format(n=len(output_types), m=len(output_file_names_or_none), i=task_id)
        log.error(msg)
        raise


def __validate_provided_file_types(file_types):
    def _is_valid(x):
        if not isinstance(x, FileType):
            raise TypeError("Invalid FileType. Got {t}".format(t=type(x)))

    for i in file_types:
        _is_valid(i)

    return file_types


def validate_provided_file_types(file_type_or_file_types):
    if isinstance(file_type_or_file_types, FileType):
        return [file_type_or_file_types]

    return __validate_provided_file_types(file_type_or_file_types)


def __is_schema(d):
    try:
        p = d['property']
        keys = p.keys()
        oid = keys[0]
        if len(keys) != 1:
            raise IndexError
        _ = d['property'][oid]['default']
        return True
    except (KeyError, IndexError):
        return False


def _is_schema_list(value):
    if isinstance(value, dict):
        if value:
            __is_schema(value)
            return value
        else:
            # empty
            return value

    if isinstance(value, (list, tuple)):
        return all(__is_schema(x) for x in value)

    return False


def _validate_mutable_files(mutable_files_or_none, input_types, output_types):
    """
    1. Valid well-formed string
    2. Valid that indices are pointed
    3. Valid the input/output types are the same

    :param mutable_files_or_none: [("$input.0, "$output.0"), ]
    :param input_types:
    :param output_types:
    :return:

    """
    if mutable_files_or_none is None:
        return ()

    for mutable_file in mutable_files_or_none:

        if len(mutable_file) != 2:
            raise ValueError("Malformed mutable file name {n}. Expected tuple of (str, str)".format(n=mutable_file))

        in_f, out_f = mutable_file
        if not in_f.startswith("$inputs.") or not out_f.startswith("$outputs.0"):
            raise ValueError("Malformed mutable file ({i}, {f})".format(i=in_f, f=out_f))

        ix = int(in_f.split("$inputs.")[-1])
        ox = int(out_f.split("$outputs.")[-1])
        ixt = input_types[ix]
        oxt = output_types[ox]
        if ixt != oxt:
            log.warn("Mutable types are different. {i} {t}  -> {o} {f}".format(i=ix, t=ixt, o=ox, f=oxt))

    # we got here, everything is fine
    return mutable_files_or_none


def _validate_func_with_n_args(nargs, func):
    p = inspect.getargspec(func)
    if len(p.args) != nargs:
        raise ValueError("Expected func {x} to have {n} args. Got {y}".format(x=func.__name__, n=nargs, y=len(p.args)))
    return func


def _validate_chunk_only_input_type(in_out_types):
    if len(in_out_types) == 1:
        if in_out_types[0] == FileTypes.CHUNK:
            return True
    raise ValueError("Expected chunk file type in {i}".format(i=in_out_types))


def _get_class_attr_or_raise(class_name, attr, d):
    if attr not in d:
        raise MalformedMetaTaskError("MetaTask class '{c}' is missing required class var '{n}'".format(c=class_name, n=attr))
    else:
        return d[attr]


def _raise_malformed_task_attr(msg):
    def _wrapper(m=None):
        msg_ = msg + " " + m if m is not None else msg
        raise MalformedMetaTaskError(msg_)
    return _wrapper


def validate_task_type(x):

    _raise = _raise_malformed_task_attr("IS_DISTRIBUTED must be a DI List or primitive value {x}.".format(x=bool))

    if isinstance(x, bool):
        return x
    else:
        _raise("Incompatible type. Expected bool")

    return x


def _validate_in_out_types(x):

    _raise = _raise_malformed_task_attr("In/Out types must be defined as list of (FileTypes.FILE, label, description) or a single value FileTypes.FILE")
    processed_in_out_types = []

    if isinstance(x, (list, tuple)):
        for i in x:
            _raise_type = lambda: _raise("Expected FileType. Got {t}".format(t=type(i)))
            # Support the new and old format
            if isinstance(i, (list, tuple)):
                if len(i) == 3:
                    if isinstance(i[0], FileType):
                        processed_in_out_types.append(i[0])
                    else:
                        _raise_type()
                else:
                    _raise_type()
            elif isinstance(i, FileType):
                processed_in_out_types.append(i)
            else:
                _raise_type()

    else:
        _raise("Got type {t}".format(t=type(x)))

    return processed_in_out_types


def _validate_chunk_in_out_type(msg):
    def _f(x):
        _raise = _raise_malformed_task_attr("{m} Got {x}".format(x=x, m=msg))
        x = _validate_in_out_types(x)
        if isinstance(x, (list, tuple)):
            if len(x) == 1:
                if x[0] is FileTypes.CHUNK:
                    return x
        _raise()

    return _f


def _validate_scatter_output_types(x):
    _f = _validate_chunk_in_out_type("Scatter outputs type must be ONLY one chunk file type")
    return _f(x)


def _validate_gather_input_types(x):
    _f = _validate_chunk_in_out_type("Gather input type must be ONLY one chunk file type")
    return _f(x)


def _validate_gather_output_types(x):
    _raise = _raise_malformed_task_attr("Gather output types must be a single file type. Got {x}".format(x=x))
    if isinstance(x, (list, tuple)):
        # Only one output is allowed
        if len(x) == 1:
            # old format
            if isinstance(x, FileType):
                return [x]
            # New Format [(FileType, label, desc)]
            if isinstance(x[0], (list, tuple)):
                if isinstance(x[0][0], FileType):
                    return [x[0][0]]
    _raise()


def _validate_schema_options(x):
    _raise = _raise_malformed_task_attr("Schema options must be provided as DI list or dict. Got type {t}.".format(t=type(x)))

    # Standard form
    if isinstance(x, dict):
        if x:
            is_valid = _is_schema_list(x)
            if is_valid:
                return x
        else:
            # emtpy dict
            return x
    elif isinstance(x, (list, tuple)):
        x = to_list_if_necessary(x)
        if not __is_schema(x[0]):
            _raise("When task options are provided as DI model list, the first item must be the schema options for the Task.")
        return x

    # All other cases fail
    _raise()


def _validate_nproc(x):

    msg = "NPROC ('{x}') must be a DI LIST or primitive int value, or {s}".format(x=x, s=SymbolTypes.MAX_NPROC)
    _raise = _raise_malformed_task_attr(msg)
    if isinstance(x, int):
        if x > 0:
            return x
        _raise("NPROC must be > 0")
    elif isinstance(x, str):
        if x == SymbolTypes.MAX_NPROC:
            return x
        else:
            _raise("")
    elif isinstance(x, (tuple, list)):
        # Validate DI
        return to_list_if_necessary(x)
    else:
        _raise("Got type (t)").format(t=type(x))

    return x


def _validate_task_id(x):
    if isinstance(x, str):
        if RX_TASK_ID.match(x):
            return x
    else:
        raise MalformedMetaTaskError("Task id '{n}' must match {p}".format(p=RX_TASK_ID.pattern, n=x))


def _validate_resource_types(x):
    if x is None:
        return ()

    _raise = _raise_malformed_task_attr("Resource types must be a list with valid values {x}.".format(x=ResourceTypes.ALL()))

    if isinstance(x, (list, tuple)):
        for i in x:
            if i not in ResourceTypes.ALL():
                _raise("Invalid resource value '{x}'".format(x=i))
        return x
    else:
        _raise("Invalid type {x}".format(x=x))


def _validate_to_cmd_func(f):
    _validate_func_with_n_args(5, f)
    return f


def _validate_version(x):
    m = RX_VERSION.match(x)
    if m is None:
        _raise_malformed_task_attr("Version '{v}' should match {p}".format(v=x, p=RX_VERSION.pattern))
    return x


def _validate_nchunks(x):
    if isinstance(x, str):
        if x == SymbolTypes.MAX_NCHUNKS:
            return x
    if isinstance(x, int):
        return x
    if isinstance(x, (list, tuple)):
        if isinstance(list(x)[-1], types.FunctionType):
            # Add more validation here
            return x

    msg = "Chunk only supports int or {x}, or DI model list".format(x=SymbolTypes.MAX_NCHUNKS)
    _raise_malformed_task_attr(msg)


def _validate_chunk_keys(chunk_keys):
    if isinstance(chunk_keys, (list, tuple)):
        if not chunk_keys:
            _raise_malformed_task_attr("CHUNK_KEYS can NOT be empty.")
        for chunk_key in chunk_keys:
            if RX_CHUNK_KEY.match(chunk_key) is None:
                _raise_malformed_task_attr("CHUNK_KEYS '{x}' must match pattern {p}".format(x=chunk_key, p=RX_CHUNK_KEY))
        return chunk_keys

    _raise_malformed_task_attr("CHUNK_KEYS '{m}' is malformed".format(m=chunk_keys))


def _metaklass_to_metatask(klass, cls, name, parents, dct, validate_input_types_func, validate_output_types_func, validate_to_cmd_func):
    """
    This looks kinda crazy, but I am just trying to avoid metaclass
    inheritance at all costs. This should be easier to understand and will
    avoid code duplication in Scatter, Gather metaclasses.

    Validating the input, output files and number of args in the to-cmd func
    is the only item that needs to be explicitly passed in.

    """

    def _to_v(attr_name):
        return _get_class_attr_or_raise(name, attr_name, dct)

    def _to_value(attr_name, validation_func):
        return validation_func(_to_v(attr_name))

    task_id = _to_value("TASK_ID", _validate_task_id)
    # this is the display name
    display_name = _to_v("NAME")
    desc = cls.__doc__

    input_types = _to_value("INPUT_TYPES", validate_input_types_func)
    output_types = _to_value("OUTPUT_TYPES", validate_output_types_func)
    schema_opts = _to_value("SCHEMA_OPTIONS", _validate_schema_options)
    task_type = _to_value("IS_DISTRIBUTED", validate_task_type)
    nproc = _to_value("NPROC", _validate_nproc)
    version = _to_value("VERSION", _validate_version)
    resource_types = _validate_resource_types(dct.get("RESOURCE_TYPES", None))

    output_file_names = _validate_output_file_names_or_raise(output_types, dct.get('OUTPUT_FILE_NAMES', None), task_id)
    mutable_files = _validate_mutable_files(dct.get("MUTABLE_FILES", None), input_types, output_types)

    # the static method Class is a bit awkward to use. There's a bit of hackery going on here.
    # to_cmd_staticmethod = _to_v('to_cmd')
    # log.info(("Static method object", to_cmd_staticmethod, type(to_cmd_staticmethod)))
    # can't use isinstance(x, staticmethod) for some reason
    # if type(to_cmd_staticmethod) != 'staticmethod':
    #     raise MalformedMetaTaskError("Task class '{x}' to_cmd must be defined as a staticmethod.".format(x=name))

    obj = super(klass, cls).__new__(cls, name, parents, dct)
    to_cmd_func = getattr(obj, 'to_cmd')
    to_cmd_func = validate_to_cmd_func(to_cmd_func)

    return task_id, task_type, input_types, output_types, schema_opts, nproc, resource_types, to_cmd_func, output_file_names, mutable_files, desc, display_name, version


class _MetaKlassTask(type):

    def __new__(cls, name, parents, dct):

        if name is not 'MetaTaskBase':
            try:
                _validate_to_cmd = functools.partial(_validate_func_with_n_args, 5)
                # See above comments try to rationalize this craziness
                task_id, task_type, input_types, output_types, schema_opts, nproc, resource_types, to_cmd_func, output_file_names, mutable_files, desc, display_name, version = _metaklass_to_metatask(_MetaKlassTask, cls, name, parents, dct, _validate_in_out_types, _validate_in_out_types, _validate_to_cmd)
                meta_task = MetaTask(task_id, task_type, input_types, output_types, schema_opts, nproc, resource_types, to_cmd_func, output_file_names, mutable_files, desc, display_name, version=version)

                _register_or_raise(meta_task)

            except Exception as e:
                msg = "Failed to load Task class {c}. {m}".format(c=name, m=e.message)
                log.error(msg)
                sys.stderr.write(msg + "\n")
                raise

        return super(_MetaKlassTask, cls).__new__(cls, name, parents, dct)


class MetaTaskBase(object):
    __metaclass__ = _MetaKlassTask

    # this is really just for autocomplete to work
    TASK_ID = None
    NAME = None
    VERSION = None

    IS_DISTRIBUTED = None
    INPUT_TYPES = None
    OUTPUT_TYPES = None

    SCHEMA_OPTIONS = None
    NPROC = None

    # Optional
    OUTPUT_FILE_NAMES = None
    RESOURCE_TYPES = None


class _MetaScatterKlassTask(type):

    def __new__(cls, name, parents, dct):

        def _to_v(attr_name):
            return _get_class_attr_or_raise(name, attr_name, dct)

        def _to_value(attr_name, validation_func):
            return validation_func(_to_v(attr_name))

        if name not in ('_MetaScatterKlassTask', 'MetaScatterTaskBase'):
            try:
                _validate_to_cmd = functools.partial(_validate_func_with_n_args, 6)
                task_id, task_type, input_types, output_types, schema_opts, nproc, resource_types, to_cmd_func, output_file_names, mutable_files, desc, display_name, version = _metaklass_to_metatask(_MetaScatterKlassTask, cls, name, parents, dct, _validate_in_out_types, _validate_scatter_output_types, _validate_to_cmd)

                nchunks = _to_value("NCHUNKS", _validate_nchunks)
                chunk_keys = _to_value("CHUNK_KEYS", _validate_chunk_keys)
                to_cmd_func = _validate_func_with_n_args(6, to_cmd_func)

                meta_task = MetaScatterTask(task_id, task_type, input_types, output_types, schema_opts, nproc, resource_types, to_cmd_func, nchunks, chunk_keys, output_file_names, mutable_files, desc, display_name, version=version)

                _register_or_raise(meta_task)

            except Exception as e:
                msg = "Failed to load Task class {c}. {m}".format(c=name, m=e.message)
                log.error(msg)
                sys.stderr.write(msg + "\n")
                raise

        return super(_MetaScatterKlassTask, cls).__new__(cls, name, parents, dct)


class MetaScatterTaskBase(object):

    """This is the new model that all python defined tasks should use"""
    __metaclass__ = _MetaScatterKlassTask

    # this is really just for autocomplete to work
    TASK_ID = None
    NAME = None
    VERSION = None

    IS_DISTRIBUTED = True
    INPUT_TYPES = None
    OUTPUT_TYPES = None

    SCHEMA_OPTIONS = None
    NPROC = None

    # Optional
    OUTPUT_FILE_NAMES = None
    RESOURCE_TYPES = None

    NCHUNKS = None
    CHUNK_KEYS = None


class _MetaGatherKlassTask(type):

    def __new__(cls, name, parents, dct):

        def _to_v(attr_name):
            return _get_class_attr_or_raise(name, attr_name, dct)

        def _to_value(attr_name, validation_func):
            return validation_func(_to_v(attr_name))

        if name not in ('_MetaScatterKlassTask', 'MetaScatterTaskBase', '_MetaGatherKlassTask', 'MetaGatherTaskBase'):
            try:
                _validate_to_cmd = functools.partial(_validate_func_with_n_args, 5)
                task_id, task_type, input_types, output_types, schema_opts, nproc, resource_types, to_cmd_func, output_file_names, mutable_files, desc, display_name, version = _metaklass_to_metatask(_MetaGatherKlassTask, cls, name, parents, dct, _validate_gather_input_types, _validate_gather_output_types, _validate_to_cmd)

                # chunk_keys = _to_value("CHUNK_KEYS", _validate_chunk_keys)

                meta_task = MetaGatherTask(task_id, task_type, input_types, output_types, schema_opts, nproc, resource_types, to_cmd_func, output_file_names, mutable_files, desc, display_name, version=version)

                _register_or_raise(meta_task)

            except Exception as e:
                msg = "Failed to load Task class {c}. {m}".format(c=name, m=e.message)
                log.error(msg)
                sys.stderr.write(msg + "\n")
                raise

        return super(_MetaGatherKlassTask, cls).__new__(cls, name, parents, dct)


class MetaGatherTaskBase(object):

    """This is the new model that all python defined tasks should use"""
    __metaclass__ = _MetaGatherKlassTask

    TASK_ID = None
    NAME = None
    VERSION = None

    IS_DISTRIBUTED = True
    INPUT_TYPES = None
    OUTPUT_TYPES = None

    SCHEMA_OPTIONS = None
    NPROC = None

    # Optional
    OUTPUT_FILE_NAMES = None
    RESOURCE_TYPES = None
