from collections import namedtuple
import logging
import json
import os
import collections
import pbsmrtpipe

from pbsmrtpipe.constants import (to_workflow_option_ns,
                                  RESOLVED_TOOL_CONTRACT_JSON)
from pbsmrtpipe.exceptions import (MalformedChunkOperatorError)


# legacy. imports into this module.
from pbcommand.models import FileType
from pbcommand.models.common import REGISTERED_FILE_TYPES

log = logging.getLogger(__name__)


# Global Registry. These should only be accessed via pbsmrtpipe.loader !!!
REGISTERED_TASKS = {}

REGISTERED_PIPELINES = {}

REGISTERED_CHUNK_OPERATORS = {}

REGISTERED_CLUSTER_RENDERERS = {}

__all__ = ['Constants', 'TaskTypes', 'SymbolTypes',
           'ResourceTypes', 'FileTypes',
           'MetaTask', 'Task', 'ToolContractMetaTask',
           'ScatterTask',
           'GatherTask',
           'RunnableTask',
           'DataStoreFile', 'DataStore',
           'Pipeline', "PipelineChunk", 'ChunkOperator']


class GlobalRegistry(object):

    """Global Registry of Immutable resources

    All are dicts, except for cluster_render
    """

    def __init__(self, tasks, file_types, chunk_operators, cluster_renderer):
        """

        :param tasks:
        :type tasks: dict[str, MetaTask]

        :param file_types:
        :type file_types: dict[str, FileType]

        :param chunk_operators:
        :type chunk_operators: dict[str,ChunkOperator]

        :param cluster_renderer:
        :type cluster_renderer:  ClusterTemplateRender | None

        :return:
        """
        self.tasks = tasks
        self.file_types = file_types
        self.chunk_operators = chunk_operators
        self.cluster_renderer = cluster_renderer

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  n=len(self.tasks),
                  f=len(self.file_types),
                  o=len(self.chunk_operators))
        return "<{k} tasks:{n} file-types:{f} operators:{o}>".format(*_d)


def datetime_to_string(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S')


class Constants(object):
    CHUNK_KEY_PREFIX = "$chunk."

TaskResult = namedtuple('TaskResult', "task_id state error_message run_time_sec")

_JOB_ATTRS = ['root', 'workflow', 'html', 'logs', 'tasks', 'css', 'js', 'images', 'datastore_json', 'entry_points_json']
JobResources = namedtuple("JobResources", _JOB_ATTRS)


class TaskStates(object):
    # Task Has been created
    CREATED = 'created'
    # Options have been resolved
    READY = 'ready'
    # Task was submitted to the computing resources
    SUBMITTED = 'submitted'
    # Task Is running. Need to clarify what 'running' means in the
    # cluster env. It could still be waiting in the queue
    RUNNING = 'running'
    SUCCESSFUL = 'successful'
    FAILED = 'failed'
    # Killed by sigint from the user
    KILLED = 'killed'
    # Not sure this is the best way to handle this
    # Scattered means the chunking has been applied and the new
    # chunked tasks were created.
    SCATTERED = 'scattered'

    @classmethod
    def ALL_STATES(cls):
        return (cls.CREATED, cls.READY, cls.SUBMITTED, cls.RUNNING,
                cls.SUCCESSFUL, cls.FAILED, cls.SCATTERED, cls.KILLED)

    @classmethod
    def COMPLETED_STATES(cls):
        return cls.SUCCESSFUL, cls.FAILED, cls.KILLED, cls.SCATTERED

    @classmethod
    def RUNNABLE_STATES(cls):
        return cls.CREATED, cls.READY

    @classmethod
    def FAILURE_STATES(cls):
        return cls.FAILED, cls.KILLED


class MetaTask(object):

    def __init__(self, task_id, is_distributed,
                 input_types, output_types,
                 option_schemas,
                 nproc,
                 resource_types, cmd_func, output_file_names, mutable_files, description, display_name, version=None):
        """These may be specified as the DI version"""
        self.task_id = task_id
        self.input_types = input_types
        self.output_types = output_types
        self.resource_types = resource_types
        self.option_schemas = option_schemas
        self.nproc = nproc
        self.is_distributed = is_distributed
        self.cmd_func = cmd_func
        self.output_file_names = output_file_names
        self.mutable_files = mutable_files
        self.description = description
        self.display_name = display_name
        self.version = version if version is not None else "UNKNOWN"

    def __eq__(self, other):
        # need to rethink this.
        if isinstance(other, self.__class__):
            if self.task_id == other.task_id:
                if len(self.input_types) == len(other.input_types):
                    if len(self.output_file_names) == len(self.output_file_names):
                        return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        v = "v{v}".format(v=self.version) if self.version is not None else ""
        _d = dict(k=self.__class__.__name__,
                  i=self.task_id,
                  p=len(self.input_types),
                  o=len(self.output_types),
                  r=len(self.resource_types),
                  v=v)
        return "<{k} id:{i} {v} inputs:{p} outputs:{o} resources:{r} >".format(**_d)

    def summary(self):
        outs = ["{k} summary id:{i}".format(i=self.task_id, k=self.__class__.__name__)]
        sep = '-' * 20

        def _sep():
            outs.append(sep)

        def _to_io_str(attr_name, description):
            attr = getattr(self, attr_name)
            outs.append(" {x} ({n})".format(n=len(attr), x=description))
            _sep()
            for i, io_type in enumerate(attr):
                outs.append(" ".join([str(i).rjust(3), str(io_type)]))

        if self.description:
            _sep()
            outs.append("Description:")
            outs.append(self.description)

        _sep()
        _to_io_str('input_types', "Input Types")
        _sep()
        _to_io_str('output_types', "Output Types")

        def to_f_(s):
            return str(s).ljust(20)

        def _to_di_str(attr_name, description):
            attr = getattr(self, attr_name)
            desc = to_f_(description)
            if isinstance(attr, (str, int)):
                outs.append(" {x}: {v}".format(x=desc, v=attr))
            else:
                outs.append(" {x}: DI list (n) items".format(x=desc, n=len(attr)))

        _sep()
        _to_di_str("is_distributed", "Is Distributed")
        _to_di_str("nproc", "nproc")

        if isinstance(self.option_schemas, dict):
            outs.append(" : ".join([to_f_(" Number of Options"), str(len(self.option_schemas))]))
        elif isinstance(self.option_schemas, (list, tuple)):
            if self.option_schemas:
                _to_di_str("Number of Options", len(self.option_schemas[0]))
        else:
            # should never get here
            log.warn("Malformed task options {o}".format(o=self.option_schemas))

        if self.resource_types:
            outs.append(" Resources Types: {r}".format(r=self.resource_types))

        if self.mutable_files:
            outs.append(" Mutable Files: {m}".format(m=self.mutable_files))

        _sep()
        if self.output_file_names:
            outs.append(" Override Output files names ({n})".format(n=len(self.output_file_names)))
            xs = zip(self.output_types, self.output_file_names)
            for i, x in enumerate(xs):
                type_, name_ext_ = x
                name_ = ".".join(name_ext_)
                outs.append(" {i}: {t} -> {x} ".format(i=str(i).rjust(3), x=name_, t=type_))

        _sep()
        return "\n".join(outs)

    def to_cmd(self, input_files, output_files, resolved_opts, nproc, resource_types):
        """

        Quite a bit of validation here to help debugging.
        """
        validations = [("Input types", self.input_types, input_files),
                       ("Output types", self.output_types, output_files),
                       ("Resource types", self.resource_types, resource_types)]

        for m, k, v in validations:
            if len(k) != len(v):
                _d = dict(c=self.__class__.__name__,
                          n=len(k), i=len(v), v=v, d=self.task_id, m=m)
                raise IndexError("{c} {d}. Incompatible with defined {m}. Expected {n} values. Got '{i}'. {v}".format(**_d))

        # - should validate resolved options against schema
        # this can be the DI model, or the raw di

        schemas = self.option_schemas
        if isinstance(self.option_schemas, (list, tuple)):
            # assume the first value is a dict of the opts
            schemas = self.option_schemas[0]

        for k, v in schemas.iteritems():
            if k not in resolved_opts:
                raise KeyError("Expected resolved option with id '{k}'. Got {d}. Options are not resolved. {o}".format(k=k, d=resolved_opts, o=self.option_schemas))

        if not isinstance(nproc, int):
            raise TypeError("nproc expected int, got type {t}".format(t=type(nproc)))

        return self.cmd_func(input_files, output_files, resolved_opts, nproc, resource_types)


class MetaScatterTask(MetaTask):

    def __init__(self, task_id, is_distributed, input_types, output_types, opt_schema, nproc, resource_types, cmd_func, chunk_di, chunk_keys, output_file_names, mutable_files, description, display_name, version=None):
        super(MetaScatterTask, self).__init__(task_id, is_distributed, input_types, output_types, opt_schema, nproc, resource_types, cmd_func, output_file_names, mutable_files, description, display_name, version=version)
        # this can be a primitive value or a DI model list
        self.chunk_di = chunk_di
        self.chunk_keys = chunk_keys

    def to_cmd(self, input_files, output_files, resolved_opts, nproc, resource_types, nchunks):
        return self.cmd_func(input_files, output_files, resolved_opts, nproc, resource_types, nchunks)


class MetaGatherTask(MetaTask):
    # FIXME. This needs to override the to_cmd so the chunk_key can be passed in
    # This will make the {Fasta|Fastq|Gff}Gather generic.
    pass


class Task(object):
    # FIXME. This needs to be consolidated with the ResolvedToolContract and Runnable Task data-models

    def __init__(self, task_id, is_distributed, input_files, output_files, resolved_options, nproc, resources, cmd, output_dir):
        self.task_id = task_id
        # List of strings
        self.input_files = input_files
        # List of Strings
        self.output_files = output_files
        # [{"resource_type":"type-id", "path": "/path/to/resource"}, ...]
        self.resources = resources
        # dict
        self.resolved_options = resolved_options
        # int
        self.nproc = nproc
        #
        self.is_distributed = is_distributed
        # Command list of strings or string
        self.cmds = cmd if isinstance(cmd, (list, tuple)) else [cmd]

        # Task output dir
        self.output_dir = output_dir

    @property
    def stderr(self):
        return os.path.join(self.output_dir, 'stderr')

    @property
    def stdout(self):
        return os.path.join(self.output_dir, 'stdout')

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.task_id,
                  p=len(self.input_files),
                  o=len(self.output_files),
                  r=len(self.resources),
                  n=self.nproc)
        # changing this so to_dot works
        return "{k} id {i} inputs {p} outputs {o} resources {r} nproc {n} ".format(**_d)

    def to_dict(self):
        return dict(task_id=self.task_id,
                    input_files=self.input_files,
                    output_files=self.output_files,
                    resources=self.resources, nproc=self.nproc,
                    options=self.resolved_options,
                    cmds=self.cmds,
                    is_distributed=self.is_distributed,
                    output_dir=self.output_dir)

    @staticmethod
    def from_d(d):
        return Task(d['task_id'], d['is_distributed'],
                    d['input_files'], d['output_files'],
                    d['options'], d['nproc'],
                    d['resources'], d['cmds'], d['output_dir'])


class ScatterTask(Task):

    def __init__(self, task_id, task_type, input_files, output_files, resolved_opts, nproc, resources, cmd, nchunks, output_dir, chunk_keys):
        super(ScatterTask, self).__init__(task_id, task_type, input_files, output_files, resolved_opts, nproc, resources, cmd, output_dir)
        self.nchunks = nchunks
        self.chunk_keys = chunk_keys

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.task_id,
                  p=len(self.input_files),
                  o=len(self.output_files),
                  r=len(self.resources),
                  n=self.nproc,
                  c=self.nchunks, x=self.chunk_keys)
        return "<{k} id:{i} inputs:{p} outputs:{o} resources:{r} nproc:{n} nchunks:{c} keys:{x} >".format(**_d)


class GatherTask(Task):
    pass


class RunnableTask(object):

    """Container for task-manifest.json"""

    def __init__(self, task, cluster, envs=None):
        """

        :type cluster: ClusterTemplateRender | None
        :type task: Task
        """

        self.task = task
        self.cluster = cluster
        self.envs = {} if envs is None else envs

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.task.task_id,
                  n=len(self.task.cmds),
                  t=self.task.is_distributed,
                  m=len(self.task.resources))
        return "<{k} {i} task type {t} ncommands {n} nresources {m} >".format(**_d)

    @staticmethod
    def from_manifest_json(path):
        with open(path, 'r') as r:
            d = json.loads(r.read())

        return RunnableTask.from_d(d)

    @staticmethod
    def from_d(d):

        # fixme
        from pbsmrtpipe.cluster import ClusterTemplateRender, ClusterTemplate

        if d['cluster']:
            tmplates = [ClusterTemplate(k, v) for k, v in d['cluster'].iteritems()]
            c = ClusterTemplateRender(tmplates)
        else:
            c = None

        task = Task.from_d(d['task'])
        return RunnableTask(task, c, d['env'])

    def to_dict(self):
        t = self.task.to_dict()
        if self.cluster:
            cr = {name: str(t) for name, t in self.cluster.cluster_templates.iteritems()}
        else:
            cr = None

        return dict(id=self.task.task_id,
                    task=t, env={},
                    cluster=cr,
                    version=pbsmrtpipe.get_version(),
                    resource_types=self.task.resources)


class Pipeline(object):

    def __init__(self, idx, display_name, description, bindings, entry_bindings, parent_pipeline_ids=None):
        self.idx = idx
        self.display_name = display_name
        self.description = description
        # List of [(a, b), ...]
        self.bindings = bindings
        # List of [(a, b), ...]
        self.entry_bindings = entry_bindings
        if parent_pipeline_ids is None:
            self.parent_pipeline_ids = []
        else:
            self.parent_pipeline_ids = parent_pipeline_ids

    @property
    def pipeline_id(self):
        return self.idx

    @property
    def all_bindings(self):
        return self.bindings + self.entry_bindings

    def __repr__(self):
        ek = [eid for eid, _ in self.entry_bindings]
        e = " ".join(ek)
        _d = dict(k=self.__class__.__name__, i=self.idx,
                  d=self.display_name, b=len(self.bindings), e=e)
        return "<{k} id={i} nbindings={b} entry bindings={e} >".format(**_d)

    def summary(self):
        def _printer(xs):
            for a, b in xs:
                print a, '->', b
        print "Summary", self.pipeline_id
        print "[EntryPoints]"
        _printer(self.entry_bindings)
        print "[Bindings]"
        _printer(self.bindings)
        print "[Parents]", self.parent_pipeline_ids


ScatterChunk = namedtuple("ScatterChunk", "chunk_key task_input")
# task id to scatter, scatter task id
Scatter = namedtuple("Scatter", "task_id scatter_task_id chunks")

GatherChunk = namedtuple("GatherChunk", "gather_task_id chunk_key task_input")
Gather = namedtuple("Gather", "chunks")

ChunkOperator = namedtuple("ChunkOperator", "idx scatter gather")

SmrtAnalysisComponent = namedtuple("SmrtAnalysisComponent", "build version name")
SmrtAnalysisSystem = namedtuple("SmrtAnalysisSystem", "build version")


def validate_operator(op, registered_tasks):
    """

    :type op: ChunkOperator
    :param op:
    :return:
    """

    def _raise_msg(m):
        MalformedChunkOperatorError("Operator {o} malformed. {m}".format(o=op.idx, m=m))

    def _get_task_or_raise(task_id_):
        if task_id_ not in registered_tasks:
            _raise_msg("Unable to find task id {i}".format(o=op.idx, i=task_id_))
        return registered_tasks[task_id_]

    # Validate Make sure all chunked task id is found
    _get_task_or_raise(op.scatter.task_id)
    _get_task_or_raise(op.scatter.scatter_task_id)

    for gather_chunk in op.gather.chunks:
        _get_task_or_raise(gather_chunk.gather_task_id)

    # validate input types of chunked tasks and scatter task are the same
    ctask = registered_tasks[op.scatter.task_id]
    stask = registered_tasks[op.scatter.scatter_task_id]

    if not isinstance(stask, MetaScatterTask):
        _raise_msg("Scatter tasks must be of type {x}".format(x=MetaScatterTask))

    if len(ctask.input_types) != len(stask.input_types):
        _raise_msg("Scatter Tasks incompatible input types. Chunked task {t} Scatter Task {s}".format(t=ctask.input_types, s=stask.input_types))

    # Validate Chunk task an Scatter Task have the same input types
    for i, input_type in enumerate(ctask.input_types):
        stask_input_type = stask.input_types[i]
        if input_type != stask_input_type:
            _raise_msg("Incompatible input types. Task {i} Expected {t}. Got {s}".format(i=ctask.task_id, t=input_type, s=stask_input_type))

    _gchunks = {c.task_input: c for c in op.gather.chunks}
    # validate that all the gather chunk tasks are bound to
    for i, input_type in enumerate(ctask.input_types):
        task_input = ".".join([ctask.task_id, str(i)])
        if task_input not in _gchunks:
            _raise_msg("task {t} input {i} is not bound in Gather chunks {c}".format(t=ctask.task_id, i=i, c=_gchunks.keys()))
        else:
            gchunk = _gchunks[task_input]
            log.debug("Workflow will map {i} using {c}".format(i=task_input, c=gchunk))

    return True


class WorkflowLevelOptions(collections.Sized):

    ATTR_TO_ID = {'chunk_mode': to_workflow_option_ns('chunk_mode'),
                  'max_nchunks': to_workflow_option_ns('max_nchunks'),
                  'max_nproc': to_workflow_option_ns('max_nproc'),
                  'total_max_nproc': to_workflow_option_ns("max_total_nproc"),
                  'max_nworkers': to_workflow_option_ns('max_nworkers'),
                  "distributed_mode": to_workflow_option_ns("distributed_mode"),
                  "cluster_manager_path": to_workflow_option_ns("cluster_manager"),
                  "tmp_dir": to_workflow_option_ns("tmp_dir"),
                  "progress_status_url": to_workflow_option_ns("progress_status_url"),
                  "exit_on_failure": to_workflow_option_ns("exit_on_failure")}

    def __init__(self, chunk_mode, max_nchunks, max_nproc, total_max_nproc, max_nworkers,
                 distributed_mode, cluster_manager_path, tmp_dir,
                 progress_status_url, exit_on_failure):
        """ Container for the known workflow options"""
        self.chunk_mode = chunk_mode
        self.max_nchunks = max_nchunks
        self.max_nproc = max_nproc
        self.total_max_nproc = total_max_nproc
        self.max_nworkers = max_nworkers
        self.distributed_mode = distributed_mode
        # This can be given as an abspath to a dir,
        # or "pbsmrtpipe.cluster_templates.sge"
        self.cluster_manager_path = cluster_manager_path
        self.tmp_dir = tmp_dir
        self.progress_status_url = progress_status_url
        self.exit_on_failure = exit_on_failure

    @staticmethod
    def from_defaults():
        return WorkflowLevelOptions.from_id_dict({})

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, h=self.max_nchunks,
                  n=self.max_nproc,
                  w=self.max_nworkers, c=self.cluster_manager_path)
        return "<{k} chunk:{h} nproc:{n} workers:{w} cluster:{c}>".format(**_d)

    def __len__(self):
        return len(self.to_dict())

    @staticmethod
    def from_id_dict(d):
        """
        Create an instance from a id dict of options (pbsmrtpipe.options.x:value}
        """
        from pbsmrtpipe.pb_io import REGISTERED_WORKFLOW_OPTIONS
        import pbsmrtpipe.schema_opt_utils as OP

        adict = {}

        for opt_id, schema in REGISTERED_WORKFLOW_OPTIONS.iteritems():
            if opt_id in d:
                v = d[opt_id]
                OP.validate_value(schema, {opt_id: v})
                adict[opt_id] = v
            else:
                value = OP.get_default_from_schema(schema)
                d[opt_id] = value

        # build map to instance var names
        adict = {k: d[v] for k, v in WorkflowLevelOptions.ATTR_TO_ID.iteritems()}

        return WorkflowLevelOptions(**adict)

    def to_dict(self):
        return {v: getattr(self, k) for k, v in self.ATTR_TO_ID.iteritems()}


AnalysisLink = namedtuple("AnalysisLink", "name path")


class ToolContractMetaTask(MetaTask):

    def __init__(self, tool_contract, task_id, is_distributed, input_types, output_types, options_schema,
                 nproc, resource_types, output_file_names, mutable_files, description, display_name, version="NA", driver=None):
        """

        :type driver: ToolDriver
        :type tool_contract: pbcommand.models.ToolContract

        """
        # this is naughty and terrible. to_cmd should not be here!!!
        super(ToolContractMetaTask, self).__init__(task_id, is_distributed, input_types, output_types, options_schema,
                                             nproc, resource_types, "NA", output_file_names, mutable_files, description, display_name, version=version)
        # Adding in a bit of duplication here. Once everything uses TC, then
        # then the entire system can dramatically be simplify
        self.tool_contract = tool_contract
        # Driver
        self.driver = driver

    def to_cmd(self, input_files, output_files, resolved_opts, nproc, resource_types):
        """ Write the driver.exe driver-manifest.json

        For TC/RTC era this is an extra layer that should go away.
        """
        # get the job dir from the resolved value of the first output file,
        # this should probably be accessed via ResourceType.JobDir
        output_dir = os.path.dirname(output_files[0])
        p = os.path.join(output_dir, RESOLVED_TOOL_CONTRACT_JSON)
        return "{d} {m}".format(d=self.driver.driver_exe, m=p)
