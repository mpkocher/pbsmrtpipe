"""A lot of this file should be pushed into models.py or deleted"""
import inspect
import logging
import copy
import os
import sys
import types
import functools

from pbcommand.models import (FileTypes, TaskTypes, SymbolTypes, ResourceTypes, FileType)
from pbcommand.models.common import REGISTERED_FILE_TYPES
from pbsmrtpipe.exceptions import (MalformedMetaTaskError,
                                   MalformedPipelineError)

from pbsmrtpipe.constants import (ENTRY_PREFIX,
                                  RX_BINDING_TASK,
                                  RX_VALID_BINDINGS,
                                  RX_ENTRY,
                                  RX_TASK_ID, RX_BINDING_PIPELINE_ENTRY,
                                  RX_BINDING_PIPELINE_TASK)

from pbsmrtpipe.models import Pipeline, REGISTERED_PIPELINES

from pbsmrtpipe.utils import validate_type_or_raise


log = logging.getLogger(__name__)


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
        p.entry_bindings.add((e_out, b_in))

    log.debug("[Loading bindings]")
    for b_out, b_in in p_existing.bindings:
        p.bindings.add((b_out, b_in))

    # add parent history
    for p_id in p_existing.parent_pipeline_ids:
        p.parent_pipeline_ids.append(p_id)

    p.parent_pipeline_ids.append(p_existing.pipeline_id)

    return p


def _load_existing_pipeline_or_raise(pipelines_d, p, p_existing_id):
    if p_existing_id not in pipelines_d.keys():
        raise KeyError("Pipeline '{i}' required pipeline '{o}' to be defined.".format(i=p.pipeline_id, o=p_existing_id))

    p_existing = pipelines_d[p_existing_id]
    _load_existing_pipeline(p, p_existing)


def load_pipeline_bindings(registered_pipeline_d, pipeline_id, display_name, version, description, bs, tags, task_options):
    """
    Mutate the registered pipelines registry

    :param registered_pipeline_d:
    :param pipeline_id:
    :param bs: list of binding strings [(a, b), ]

    :return: mutated pipeline registry
    """
    # only use unique pairs
    bs = list({x for x in bs})

    log.debug("Processing pipeline  {i}".format(i=pipeline_id))
    # str, [(in, out)] [(in, out)]
    pipeline = Pipeline(pipeline_id, display_name, version, description, [], [], tags=tags, task_options=task_options)

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
                pipeline.entry_bindings.add((b_out, b_in))
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
            pipeline.bindings.add((b_out, b_in))
        elif _binding_str_match(RX_BINDING_PIPELINE_TASK, b_out):
            # pbsmrtpipe.pipelines.dev_01:pbsmrtpipe.tasks.dev_hello_world:0
            # needs to load existing pipeline bindings and entry points
            # then create a new binding of ("pbsmrtpipe.tasks.dev_hello_world:0", b_in)
            task_binding_str = get_task_binding_str_from_pipeline_task_str(b_out)

            pl_id = get_pipeline_id_from_pipeline_task_str(b_out)
            _load_existing_pipeline_or_raise(registered_pipeline_d, pipeline, pl_id)

            pipeline.bindings.add((task_binding_str, b_in))
            # print ("pipeline task binding", b_out, b_in)
        else:
            raise MalformedPipelineError("Unhandled binding case '{o}' -> '{i}'".format(o=b_out, i=b_in))

        if pipeline.pipeline_id not in registered_pipeline_d:
            log.debug("registering pipeline {i}".format(i=pipeline.pipeline_id))

        registered_pipeline_d[pipeline.pipeline_id] = pipeline

    return registered_pipeline_d


def register_pipeline(pipeline_id, display_name, version, tags=(), task_options=None):

    def deco_wrapper(func):

        if pipeline_id in REGISTERED_PIPELINES:
            log.warn("'{i}' has already been registered.".format(i=pipeline_id))

        bs = func()
        load_pipeline_bindings(REGISTERED_PIPELINES, pipeline_id, display_name, version, func.__doc__, bs, tags, task_options=task_options)

        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return deco_wrapper


class PipelineRegistry(object):
    """
    Container for Defining Pipeline Templates that can be loaded by
    other python modules
    """
    def __init__(self, namespace, pipelines=None):
        """

        :param namespace: Pipeline Namespace
        :param pipelines: List of Pipelines. This allows the registry to
        refererence pipelines already loaded by pbsmrtpipe

        :type pipelines: list[Pipeline]
        """
        self.namespace = namespace
        # {id:Pipeline}
        self._pipelines = {}
        if pipelines is not None:
            self._pipelines = {p.idx: p for p in pipelines}
        self.original_pipeline_ids = {i for i in self._pipelines.keys()}

    @property
    def pipelines(self):
        # Only emit the pipelines that were specifically added to this registry
        return {i: p for i, p in self._pipelines.items() if i not in self.original_pipeline_ids}

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, n=self.namespace, p=len(self.pipelines))
        return "<{k} ns:{n} pipelines:{p} >".format(**_d)

    def __call__(self, relative_pipeline_id, name, version, tags=(), task_options=None):
        """Register a pipeline by relative id"""
        def _w(func):
            desc = func.__doc__
            t_options = {} if task_options is None else copy.deepcopy(task_options)
            bs = func()
            pipeline_id = ".".join([self.namespace, 'pipelines', relative_pipeline_id])
            # pass in the *ALL* the pipelines to the new pipelines can reference
            # previously loaded pipelines
            load_pipeline_bindings(self._pipelines, pipeline_id, name, version, desc, bs, tags=tags, task_options=t_options)
            return bs

        return _w


def registry_runner(registry_, rtasks, output_dir, emit_xml=False):
    # this will emit the PTs to an output dir

    from pbsmrtpipe.pb_io import pipeline_to_xml, write_pipeline_templates_to_json

    d = os.path.abspath(os.path.expanduser(output_dir))
    r = registry_

    log.info("Writing Pipeline Templates to {o}".format(o=output_dir))
    print "Emitting pipelines to output dir {d}".format(d=d)

    # write_pipeline_templates_to_avro(r.pipelines.values(), rtasks, d)
    write_pipeline_templates_to_json(r.pipelines.values(), rtasks, d)

    for p in r.pipelines.values():
        if emit_xml:
            file_name = p.idx + "_pipeline.xml"
            path = os.path.join(output_dir, file_name)
            xml = pipeline_to_xml(p)
            with open(path, 'w') as f:
                f.write(str(xml))
            log.info("writing pipeline {x}".format(x=path))

    log.info("Successfully wrote {n} pipelines to {d}".format(n=len(r.pipelines), d=d))
    return 0