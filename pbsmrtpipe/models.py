from collections import namedtuple
import logging
import json
import os
import datetime
import collections
import warnings
import uuid

import jsonschema

from pbsmrtpipe.constants import (to_constant_ns,
                                  to_file_ns, to_workflow_option_ns,
                                  DATASTORE_VERSION, DRIVER_MANIFEST_JSON,
                                  RX_CHUNK_KEY, to_ds_ns)
from pbsmrtpipe.exceptions import (MalformedOperatorError, MalformedChunkKeyError)


log = logging.getLogger(__name__)


# Global Registry. These should only be accessed via pbsmrtpipe.loader !!!
REGISTERED_TASKS = {}

REGISTERED_FILE_TYPES = {}

REGISTERED_PIPELINES = {}

REGISTERED_CHUNK_OPERATORS = {}

REGISTERED_CLUSTER_RENDERERS = {}

__all__ = ['Constants', 'TaskTypes', 'SymbolTypes',
           'ResourceTypes', 'FileTypes',
           'MetaTask', 'Task', 'MetaStaticTask',
           'ScatterTask',
           'GatherTask',
           'RunnableTask',
           'DataStoreFile', 'DataStore',
           'Pipeline', "PipelineChunk", 'ChunkOperator']


class Constants(object):
    CHUNK_KEY_PREFIX = "$chunk."


class TaskStates(object):
    CREATED = 'created'
    READY = 'ready'
    SUBMITTED = 'submitted'
    RUNNING = 'running'
    SUCCESSFUL = 'successful'
    FAILED = 'failed'
    # Killed by sigint from the user
    KILLED = 'killed'
    # Not sure this is the best way to handle this
    SCATTERED = 'scattered'

    @classmethod
    def ALL_STATES(cls):
        return (cls.CREATED, cls.READY, cls.SUBMITTED, cls.RUNNING,
                cls.SUCCESSFUL, cls.FAILED, cls.SCATTERED, cls.KILLED)

    @classmethod
    def COMPLETED_STATES(cls):
        return cls.SUCCESSFUL, cls.FAILED, cls.KILLED


class TaskTypes(object):
    # perhaps this should have it's own namespace
    # pbsmrtpipe.task_types.
    LOCAL = to_constant_ns('local_task')
    DISTRIBUTED = to_constant_ns('distributed_task')


class SymbolTypes(object):
    MAX_NPROC = '$max_nproc'
    MAX_NCHUNKS = '$max_nchunks'
    TASK_TYPE = '$task_type'
    RESOLVED_OPTS = '$ropts'
    SCHEMA_OPTS = '$opts_schema'
    OPTS = '$opts'
    NCHUNKS = '$nchunks'
    NPROC = '$nproc'


class ResourceTypes(object):
    TMP_DIR = '$tmpdir'
    TMP_FILE = '$tmpfile'
    LOG_FILE = '$logfile'
    # tasks can write output to this directory
    OUTPUT_DIR = '$outputdir'
    # Not sure this is a good idea
    #TASK_DIR = '$taskdir'

    @classmethod
    def ALL(cls):
        return cls.TMP_DIR, cls.TMP_FILE, cls.LOG_FILE, cls.OUTPUT_DIR

    @classmethod
    def is_tmp_resource(cls, name):
        return name in (cls.TMP_FILE, cls.TMP_DIR)

    @classmethod
    def is_valid(cls, attr_name):
        return attr_name in cls.ALL()


class _RegisteredFileType(type):
    def __init__(cls, name, bases, dct):
        super(_RegisteredFileType, cls).__init__(name, bases, dct)

    def __call__(cls, *args, **kwargs):
        if len(args) != 4:
            log.error(args)
            raise ValueError("Incorrect initialization for {c}".format(c=cls.__name__))

        file_type_id, base_name, file_ext, mime_type = args
        file_type = REGISTERED_FILE_TYPES.get(file_type_id, None)

        if file_type is None:
            file_type = super(_RegisteredFileType, cls).__call__(*args)
            log.debug("Registering file type '{i}'".format(i=file_type_id))
            REGISTERED_FILE_TYPES[file_type_id] = file_type
        else:
            # print warning if base name, ext, mime type aren't the same
            attrs_names = [('base_name', base_name),
                           ('ext', file_ext),
                           ('mime_type', mime_type)]

            for attrs_name, value in attrs_names:
                v = getattr(file_type, attrs_name)
                if v != value:
                    _msg = "Attempting to register a file with a different '{x}' -> {v} (expected {y})".format(x=attrs_name, v=v, y=value)
                    log.warn(_msg)
                    warnings.warn(_msg)

        return file_type


class FileType(object):
    __metaclass__ = _RegisteredFileType

    def __init__(self, file_type_id, base_name, ext, mime_type):
        self.file_type_id = file_type_id
        self.base_name = base_name
        self.ext = ext
        self.mime_type = mime_type

        if file_type_id not in REGISTERED_FILE_TYPES:
            REGISTERED_FILE_TYPES[file_type_id] = self

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self.file_type_id == other.file_type_id:
                if self.base_name == other.base_name:
                    if self.ext == other.ext:
                        return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.file_type_id,
                  b=self.base_name, e=self.ext)
        return "<{k} id={i} name={b}.{e} >".format(**_d)


class FileTypes(object):
    # generic Txt file
    TXT = FileType(to_file_ns('txt'), 'file', 'txt', 'text/plain')

    # THIS NEEDS TO BE CONSISTENT with scala code. When the datastore
    # is written to disk the file type id's might be translated to
    # the DataSet style file type ids.
    REPORT = FileType(to_file_ns('JsonReport'), "report", "json", 'application/json')
    CHUNK = FileType(to_file_ns("CHUNK"), "chunk", "json", 'application/json')

    FASTA = FileType(to_file_ns('Fasta'), "file", "fasta", 'text/plain')
    FASTQ = FileType(to_file_ns('Fastq'), "file", "fastq", 'text/plain')

    # Not sure this should be a special File Type?
    INPUT_XML = FileType(to_file_ns('input_xml'), "input", "xml", 'application/xml')
    FOFN = FileType(to_file_ns("generic_fofn"), "generic", "fofn", 'text/plain')
    MOVIE_FOFN = FileType(to_file_ns('movie_fofn'), "movie", "fofn", 'text/plain')
    RGN_FOFN = FileType(to_file_ns('rgn_fofn'), "region", "fofn", 'text/plain')

    ALIGNMENT_CMP_H5 = FileType(to_file_ns('alignment_cmp_h5'), "alignments", "cmp.h5", 'application/octet-stream')
    # I am not sure this should be a first class file
    BLASR_M4 = FileType(to_file_ns('blasr_file'), 'blasr', 'm4', 'text/plain')
    BAM = FileType(to_file_ns('bam'), "alignments", "bam", 'application/octet-stream')
    BAMBAI = FileType(to_file_ns('bam_bai'), "alignments", "bam.bai", 'application/octet-stream')
    BED = FileType(to_file_ns('bed'), "file", "bed", 'text/plain')
    SAM = FileType(to_file_ns('sam'), "alignments", "sam", 'application/octet-stream')
    VCF = FileType(to_file_ns('vcf'), "file", "vcf", 'text/plain')
    GFF = FileType(to_file_ns('gff'), "file", "gff", 'text/plain')
    CSV = FileType(to_file_ns('csv'), "file", "csv", 'text/csv')
    XML = FileType(to_file_ns('xml'), "file", "xml", 'application/xml')

    # DataSet Types
    DS_SUBREADS = FileType(to_ds_ns("HdfSubreadSet"), "file", "h5.subreads.xml", "application/xml")
    DS_SUBREADS_H5 = FileType(to_ds_ns("SubreadSet"), "file", "subreads.xml", "application/xml")
    DS_REF = FileType(to_file_ns("ReferenceSet"), "file", "reference.dataset.xml", "application/xml")
    DS_BAM = FileType(to_file_ns("AlignmentSet"), "file", "aligned", "application/xml")

    RS_MOVIE_XML = FileType(to_file_ns("rs_movie_metadata"), "file", "rs_movie.metadata.xml", "application/xml")

    # this needs to not be a directory
    REF_ENTRY_XML = FileType(to_file_ns('reference_info_xml'), "reference.info.xml", "xml", 'application/xml')


class JsonSchemaOption(object):
    def __init__(self, schema):
        _ = jsonschema.Draft4Validator(schema)
        self.schema = schema

    @property
    def option_id(self):
        return self.schema['properties'].keys()[0]

    @property
    def default_value(self):
        return self.schema['properties'][self.option_id]['default']

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.option_id,
                  d=self.default_value)
        return "<{k} id:{i} default:{d} >".format(**_d)


class MetaTask(object):

    def __init__(self, task_id, task_type,
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
        self.task_type = task_type
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
        _to_di_str("task_type", "Task Type")
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

    def __init__(self, task_id, task_type, input_types, output_types, opt_schema, nproc, resource_types, cmd_func, chunk_di, chunk_keys, output_file_names, mutable_files, description, display_name, version=None):
        super(MetaScatterTask, self).__init__(task_id, task_type, input_types, output_types, opt_schema, nproc, resource_types, cmd_func, output_file_names, mutable_files, description, display_name, version=version)
        # this can be a primitive value or a DI model list
        self.chunk_di = chunk_di
        self.chunk_keys = chunk_keys

    def to_cmd(self, input_files, output_files, resolved_opts, nproc, resource_types, nchunks):
        return self.cmd_func(input_files, output_files, resolved_opts, nproc, resource_types, nchunks)


class MetaGatherTask(MetaTask):
    pass


class Task(object):

    def __init__(self, task_id, task_type, input_files, output_files, resolved_options, nproc, resources, cmd, output_dir):
        self.task_id = task_id
        # List of strings
        self.input_files = input_files
        # List of Strings
        self.output_files = output_files
        # List of Strings
        self.resources = resources
        # dict
        self.resolved_options = resolved_options
        # int
        self.nproc = nproc
        #
        self.task_type = task_type
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
    def __init__(self, task_id, task_type, input_files, output_files, ropts, nproc, resources, cmds, cluster, envs):
        self.task_id = task_id
        self.task_type = task_type
        self.input_files = input_files
        self.output_files = output_files
        self.nproc = nproc
        # list of {"resource_type": "$tmpdir", "path": "/path/to/resource"}
        self.resources = resources
        self.resolved_options = ropts
        self.cmds = cmds

        # {template: "", args:{}}
        self.cluster = cluster
        # list
        self.envs = envs

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.task_id,
                  n=len(self.cmds),
                  t=self.task_type,
                  m=len(self.resources))
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

        def _f(x):
            return d['task'][x]

        if d['cluster']:
            tmplates = [ClusterTemplate(k, v) for k, v in d['cluster'].iteritems()]
            c = ClusterTemplateRender(tmplates)
        else:
            c = None

        return RunnableTask(d['id'],
                            _f('task_type'),
                            _f('input_files'),
                            _f('output_files'),
                            _f('resolved_options'),
                            _f('nproc'),
                            d['resource_types'],
                            _f('cmds'), c, d['env'])


class DataStoreFile(object):
    def __init__(self, file_id, type_id, path):
        # adding this for consistency. In the scala code, the unique id must be
        # a uuid format
        self.uuid = uuid.uuid4()
        # this must globally unique. This is used to provide context to where
        # the file originated from (i.e., the tool author
        self.file_id = file_id
        # Consistent with a value in FileTypes
        self.file_type_id = type_id
        self.path = path
        self.file_size = os.path.getsize(path)
        self.created_at = datetime.datetime.fromtimestamp(os.path.getctime(path))
        self.modified_at = datetime.datetime.fromtimestamp(os.path.getmtime(path))

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.file_id,
                  t=self.file_type_id,
                  p=os.path.basename(self.path))
        return "<{k} {i} type:{t} filename:{p} >".format(**_d)

    def to_dict(self):
        return dict(sourceId=self.file_id,
                    uniqueId=str(self.uuid),
                    fileTypeId=self.file_type_id,
                    path=self.path,
                    fileSize=self.file_size,
                    createdAt=str(self.created_at),
                    modifiedAt=str(self.modified_at))

_JOB_ATTRS = ['root', 'workflow', 'html', 'logs', 'tasks', 'css', 'js', 'images', 'datastore_json', 'entry_points_json']
JobResources = namedtuple("JobResources", _JOB_ATTRS)


class DataStore(object):
    version = DATASTORE_VERSION

    def __init__(self, ds_files, created_at=None):
        """

        :type ds_files: list[DataStoreFile]
        """
        self.files = {f.file_id: f for f in ds_files}
        self.created_at = datetime.datetime.now() if created_at is None else created_at
        self.updated_at = datetime.datetime.now()

    def add(self, ds_file):
        if isinstance(ds_file, DataStoreFile):
            self.files[ds_file.file_id] = ds_file
            self.updated_at = datetime.datetime.now()
        else:
            raise TypeError("DataStoreFile expected. Got type {t} for {d}".format(t=type(ds_file), d=ds_file))

    def to_dict(self):
        fs = [f.to_dict() for i, f in self.files.iteritems()]
        _d = dict(version=self.version,
                  createdAt=str(self.created_at),
                  updatedAt=str(self.updated_at), files=fs)
        return _d

    def _write_json(self, file_name, permission):
        with open(file_name, permission) as f:
            s = json.dumps(self.to_dict(), indent=4, sort_keys=True)
            f.write(s)

    def write_json(self, file_name):
        # if the file exists is should raise?
        self._write_json(file_name, 'w')

    def write_update_json(self, file_name):
        """Overwrite Datastore with current state"""
        self._write_json(file_name, 'w+')


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
        MalformedOperatorError("Operator {o} malformed. {m}".format(o=op.idx, m=m))

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


def _is_chunk_key(k):
    return k.startswith(Constants.CHUNK_KEY_PREFIX)


class PipelineChunk(object):
    def __init__(self, chunk_id, **kwargs):
        """

        kwargs is a key-value store. keys that begin "$chunk." are considered
        to be semantically understood by workflow and can be "routed" to
        chunked task inputs.

        Values that don't begin with "$chunk." are considered metadata.


        :param chunk_id: Chunk id
        :type chunk_id: str

        """
        if RX_CHUNK_KEY.match(chunk_id) is not None:
            raise MalformedChunkKeyError("'{c}'".format(c=chunk_id))

        self.chunk_id = chunk_id
        # loose key-value pair
        self._datum = kwargs

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.chunk_id, c=",".join(self.chunk_keys))
        return "<{k} id='{i}' chunk keys={c} >".format(**_d)

    @property
    def chunk_d(self):
        return {k: v for k, v in self._datum.iteritems() if _is_chunk_key(k)}

    @property
    def chunk_keys(self):
        return self.chunk_d.keys()

    @property
    def chunk_metadata(self):
        return {k: v for k, v in self._datum.iteritems() if not _is_chunk_key(k)}

    def to_dict(self):
        return {'chunk_id': self.chunk_id, 'chunk': self._datum}


class MetaStaticTask(MetaTask):
    def __init__(self, task_id, task_type, input_types, output_types, options_schema,
                 nproc, resource_types, output_file_names, mutable_files, description, display_name, version="NA", driver=None):
        """

        :type driver: DriverExe
        :param task_id:
        :param task_type:
        :param input_types:
        :param output_types:
        :param options_schema:
        :param nproc:
        :param resource_types:
        :param output_file_names:
        :param mutable_files:
        :param description:
        :param version:
        :param driver:
        :return:
        """
        # this is naughty and terrible. to_cmd should not be here!!!
        super(MetaStaticTask, self).__init__(task_id, task_type, input_types, output_types, options_schema,
                                             nproc, resource_types, "NA", output_file_names, mutable_files, description, display_name, version=version)
        # Driver
        self.driver = driver

    def to_cmd(self, input_files, output_files, resolved_opts, nproc, resource_types):
        """ Write the driver.exe driver-manifest.json"""
        return "{d} {m}".format(d=self.driver.driver_exe, m=DRIVER_MANIFEST_JSON)


class DriverExe(object):
    def __init__(self, driver_exe, env=None):
        """

        :param driver_exe: Path to the driver
        :param env: path to env to be sourced before it's run?
        :return:
        """
        self.driver_exe = driver_exe
        self.env = env

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, e=self.driver_exe)
        return "<{k} driver:{e} >".format(**_d)


class DriverTask(object):
    def __init__(self, task_id, task_type, input_files, output_files, options, nproc, resources):
        self.task_id = task_id
        self.task_type = task_type
        self.input_files = input_files
        self.output_files = output_files
        self.options = options
        self.nproc = nproc
        self.resources = resources

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.task_id, t=self.task_type)
        return "<{k} id:{i} >".format(**_d)


class DriverManifest(object):
    def __init__(self, task, driver):
        """

        :type task: DriverTask
        :type driver: DriverExe

        :param task:
        :param driver:
        :return:
        """

        # keep this a dicts for now
        # this really isn't needed quite yet
        # self.meta_task = meta_task
        self.task = task
        self.driver = driver
