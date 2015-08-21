import os
import sys
import functools
import logging
from collections import namedtuple
from xml.etree.cElementTree import ElementTree
import collections
import json
import itertools
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader

import jsonschema
from pbcommand.resolver import (ToolContractError,
                                resolve_tool_contract,
                                resolve_scatter_tool_contract,
                                resolve_gather_tool_contract)

from pbcommand.models import (PipelineChunk,
                              ToolContractTask,
                              GatherToolContractTask,
                              ScatterToolContractTask)

from pbcommand.models.common import REGISTERED_FILE_TYPES
from pbcommand.pb_io.tool_contract_io import (load_tool_contract_from)

from xmlbuilder import XMLBuilder

from pbsmrtpipe.validators import validate_provided_file_types, validate_task_type
from pbsmrtpipe.exceptions import (PipelineTemplateIdNotFoundError,
                                   MalformedBindingStrError)
import pbsmrtpipe.schema_opt_utils as OP
from pbsmrtpipe.schema_opt_utils import crude_coerce_type_from_str
import pbsmrtpipe.cluster as C
from pbsmrtpipe.models import (SmrtAnalysisComponent, SmrtAnalysisSystem,
                               ChunkOperator, Gather,
                               GatherChunk, ScatterChunk, Scatter,
                               ToolContractMetaTask,
                               ScatterToolContractMetaTask,
                               GatherToolContractMetaTask, PacBioOption,
                               PipelineBinding, IOBinding)
from pbsmrtpipe.constants import (ENV_PRESET, SEYMOUR_HOME)
import pbsmrtpipe.constants as GlobalConstants
from pbsmrtpipe.schemas import PT_SCHEMA

log = logging.getLogger(__name__)
slog = logging.getLogger('status.' + __name__)


BuilderRecord = namedtuple("BuilderRecord", ['bindings', 'task_options', 'workflow_options'])


class PresetRecord(object):

    def __init__(self, task_options, workflow_options):
        # this is a list of tuples (task_id, value)
        self.task_options = task_options
        self.workflow_options = workflow_options

    def __repr__(self):
        _d = dict(k=self.__class__.__name__)
        return "<{k} >".format(**_d)

    def to_workflow_level_opt(self):
        d = dict(self.workflow_options)
        wopts = WorkflowLevelOptions.from_id_dict(d)
        return wopts


def _to_wopt_id(s):
    """Workflow Level Options"""
    from pbsmrtpipe.constants import to_workflow_option_ns
    return to_workflow_option_ns(s)


class Constants(object):
    TEMPLATE = 'import-template'
    TEMPLATE_ID = 'id'
    ENTRY_POINT = 'entry-point'
    ENTRY_POINTS = 'entry-points'
    TASK_OPTIONS = 'task-options'
    WORKFLOW_OPTIONS = 'options'
    BINDINGS = 'bindings'
    BINDING = 'binding'
    VALUE = 'value'
    PARAM = 'option'
    ID = 'id'
    WORKFLOW_ROOT = 'pipeline'
    WORKFLOW_TEMPLATE_ROOT = 'pipeline-template'
    WORKFLOW_TEMPLATE_PRESET_ROOT = 'pipeline-preset-template'


REGISTERED_WORKFLOW_OPTIONS = {}
# {option_id: [validate_func, ..]}
OPTION_VALIDATORS = collections.defaultdict(list)


def _parse_task_from_binding_str(s):
    """

    Task id from task binding format from a simple format (no instance id)

    pbsmrtpipe.tasks.input_xml_to_fofn:0

    """
    m = GlobalConstants.RX_BINDING_TASK.match(s)
    if m is None:
        raise MalformedBindingStrError("Binding '{b}' expected to match {x}.'".format(b=s, x=GlobalConstants.RX_BINDING_TASK.pattern))

    namespace_, task_id_, in_out_index = m.groups()
    task_id = ".".join([namespace_, 'tasks', task_id_])
    return task_id, int(in_out_index)


def _parse_task_from_advanced_binding_str(b):
    """

    Raw form455
    pbsmrtpipe.tasks.task_id.0

    Advanced form to specific multiple instances of task

    pbsmrtpipe.tasks.input_xml_to_fofn:1:0

    task_id:instance_id:in_out_index

    :rtype: int

    """
    m = GlobalConstants.RX_BINDING_TASK_ADVANCED.match(b)
    if m is None:
        raise MalformedBindingStrError("Binding '{b}' expected to match {x}.'".format(b=b, x=GlobalConstants.RX_BINDING_TASK_ADVANCED.pattern))
    else:
        namespace_, task_id_, instance_id, in_out_index = m.groups()
        task_id = ".".join([namespace_, 'tasks', task_id_])

    return task_id, int(instance_id), int(in_out_index)


def binding_str_to_task_id_and_instance_id(s):
    """Returns a task type id, instance id, in-out positional index

    :raises: MalformedBindingStrError
    """
    try:
        task_id, instance_id, in_out_index = _parse_task_from_advanced_binding_str(s)
    except MalformedBindingStrError:
        task_id, in_out_index = _parse_task_from_binding_str(s)
        instance_id = 0

    return task_id, instance_id, in_out_index


def strip_entry_prefix(b):
    if b.startswith(GlobalConstants.ENTRY_PREFIX):
        return b.split(GlobalConstants.ENTRY_PREFIX)[1]
    return b


def register_workflow_option(func):
    """Register workflow option to global registry"""

    s = func()
    _ = jsonschema.Draft4Validator(s)
    oid = s['properties'].keys()[0]
    REGISTERED_WORKFLOW_OPTIONS[oid] = s

    return func


def register_validation_func(option_id):
    def wrapper(func):
        OPTION_VALIDATORS[option_id].append(func)
    return wrapper


@register_validation_func(OP.to_opt_id('tmp_dir'))
def validator(value):
    if os.path.isdir(value):
        return value
    raise ValueError("Option id '{i}' invalid. Unable to find {v}".format(v=value))


@register_workflow_option
def _to_max_chunks_option():
    return OP.to_option_schema(_to_wopt_id("max_nchunks"), "integer", "Max Number of Chunks",
                               "Max Number of chunks that a file will be scattered into", 10)


@register_workflow_option
def _to_max_nproc_option():
    return OP.to_option_schema(_to_wopt_id("max_nproc"), "integer",
                               "Maximum Total Number of Processors Per Task",
                               "Maximum number of Processors per Task.", 16)


@register_workflow_option
def _to_max_nproc_option():
    return OP.to_option_schema(_to_wopt_id("max_total_nproc"), ("integer", "null"),
                               "Maximum Total Number of Processors",
                               "Maximum Total number of Processors/Slots the workflow engine will use (null means there is no limit).", None)


@register_workflow_option
def _get_workflow_option_schema():
    return OP.to_option_schema(_to_wopt_id("max_nworkers"), "integer",
                               "Max Number of Workers",
                               "Max Number of concurrently running tasks. (Note:  max_nproc will restrict the number of workers if max_nworkers * max_nproc > max_total_nproc)", 16)


@register_workflow_option
def _get_chunked_mode_schema():
    return OP.to_option_schema(_to_wopt_id("chunk_mode"), "boolean",
                               "Chunked File Mode",
                               "Enable file splitting (chunking) mode", False)


@register_workflow_option
def _get_distributed_mode_schema():
    return OP.to_option_schema(_to_wopt_id("distributed_mode"), "boolean",
                               "Distributed File Mode",
                               "Enable Distributed mode to submit jobs to the cluster. (Must provide 'cluster_manager' path to cluster templates)", True)


@register_workflow_option
def _get_cluster_manager_schema():
    return OP.to_option_schema(_to_wopt_id("cluster_manager"), ("string", "null"),
                               "Cluster Template Path",
                               "Path to Cluster template files directory. The directory must contain 'start.tmpl', 'interactive.tmpl' and 'kill.tmpl' "
                               "Or path to python module (e.g., 'pbsmrtpipe.cluster_templates.sge')", "pbsmrtpipe.cluster_templates.sge_pacbio")


@register_workflow_option
def _get_node_tmp_dir_schema():
    return OP.to_option_schema(_to_wopt_id("tmp_dir"), ("string", "null"), "Temp directory",
                               "Temporary directory (/tmp) on the execution node. If running in distributed mode, "
                               "the tmp directory must be on the head node too.", "/tmp")


@register_workflow_option
def _get_process_url_schema():
    return OP.to_option_schema(_to_wopt_id("progress_status_url"), ("string", "null"),
                               "Status Progress URL", "Post status progress updates to URL.", None)


@register_workflow_option
def _get_exit_on_failure():
    return OP.to_option_schema(_to_wopt_id("exit_on_failure"), "boolean", "Exit On Failure",
                               "Immediately exit if a task fails (Instead of trying to run as many tasks as possible before exiting.", False)


@register_workflow_option
def _get_exit_on_failure():
    return OP.to_option_schema(_to_wopt_id("debug_mode"), "boolean", "Enable Debug Mode",
                               "Debug will emit debug messages to Stdout and set the level in the master log to DEBUG.", False)



def validate_or_modify_workflow_level_options(wopts):
    """
    This will adjust or modify intra-option dependencies.

    :type wopts: WorkflowLevelOptions
    :param wopts:
    :return:
    """
    # Check if tmp dir
    if not os.path.isdir(wopts.tmp_dir):
        raise IOError("Unable to find tmp dir '{t}'".format(t=wopts.tmp_dir))

    # Set distributed mode to false if cluster_manager is not provided
    if wopts.distributed_mode:
        if isinstance(wopts.cluster_manager_path, str):
            try:
                _ = C.load_cluster_templates(wopts.cluster_manager_path)
                slog.info("Successfully loaded cluster manager from {p}".format(p=wopts.cluster_manager_path))
                # if we got here the templates are loaded successfully
            except Exception:
                slog.error("Failed to load cluster templates from '{x}'".format(x=wopts.cluster_manager_path))
                raise
        else:
            slog.warn("cluster_manager not provided. Settings distribute mode to False")
            wopts.distributed_mode = False
    else:
        slog.warn("distribute_mode is False, Disabling cluster manager, running in LOCAL ONLY mode.")
        wopts.cluster_manager_path = None

    return wopts


class WorkflowLevelOptions(collections.Sized):

    ATTR_TO_ID = {'chunk_mode': _to_wopt_id('chunk_mode'),
                  'max_nchunks': _to_wopt_id('max_nchunks'),
                  'max_nproc': _to_wopt_id('max_nproc'),
                  'total_max_nproc': _to_wopt_id("max_total_nproc"),
                  'max_nworkers': _to_wopt_id('max_nworkers'),
                  "distributed_mode": _to_wopt_id("distributed_mode"),
                  "cluster_manager_path": _to_wopt_id("cluster_manager"),
                  "tmp_dir": _to_wopt_id("tmp_dir"),
                  "progress_status_url": _to_wopt_id("progress_status_url"),
                  "exit_on_failure": _to_wopt_id("exit_on_failure"),
                  "debug_mode": _to_wopt_id("debug_mode")
                  }

    def __init__(self, chunk_mode, max_nchunks, max_nproc, total_max_nproc, max_nworkers,
                 distributed_mode, cluster_manager_path, tmp_dir,
                 progress_status_url, exit_on_failure, debug_mode):
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
        self.debug_mode = debug_mode

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


def _has_valid_root_tag(root):
    return root.tag == Constants.WORKFLOW_ROOT


def _has_valid_option_id_format(option_id):
    return True


def _has_valid_task_id_format(task_id):
    return True


def __get_children_from_node(root, name):
    return root.findall(name)


def _node_has_children(root, name):
    xs = __get_children_from_node(root, name)
    return len(xs) != 0


def _root_has_one_child(root, name):
    xs = __get_children_from_node(root, name)
    return len(xs) == 1


def _has_template_node(root):
    return _node_has_children(root, Constants.TEMPLATE)


def _has_entry_points(root):
    return _node_has_children(root, Constants.ENTRY_POINTS)


def _has_bindings(root):
    return _node_has_children(root, Constants.BINDINGS)


def _has_entry_points_and_bindings(root):
    return _has_bindings(root) and _has_entry_points(root)


def __parse_options(child_name, root):
    options = []

    opts = root.findall(child_name)

    if opts:
        n = opts[0]
        for x in n.findall(Constants.PARAM):
            i = x.attrib[Constants.ID]
            vs = x.findall(Constants.VALUE)
            assert len(vs) == 1
            v = vs[0]
            value = v.text

            options.append((i, value))

    return options


def _parse_template_id(root):
    xs = root.findall(Constants.TEMPLATE)
    assert len(xs) == 1
    i = xs[0].attrib[Constants.TEMPLATE_ID]
    return i


parse_task_options = functools.partial(__parse_options, Constants.TASK_OPTIONS)
parse_workflow_options = functools.partial(__parse_options, Constants.WORKFLOW_OPTIONS)


def _raw_option_with_schema(option_id, raw_value, schema):

    option_id = option_id.strip()

    schema_option_id = schema['properties'].keys()[0]

    if option_id == schema_option_id:
        types_ = schema['properties'][option_id]['type']
        coerced_value = crude_coerce_type_from_str(raw_value, types_)
        _ = jsonschema.validate(schema, {option_id: coerced_value})
        value = coerced_value
    else:
        raise KeyError("Incompatible option id '{o}' and schema id '{i}'".format(o=option_id, i=schema_option_id))

    return value


def validate_raw_task_option(registered_tasks, option_id, raw_value):
    opts = {}
    for m in registered_tasks.values():
        if m.option_schemas:
            opts.update(m.option_schemas)

    if option_id in opts:
        value = _raw_option_with_schema(option_id, raw_value, opts[option_id])
    else:
        log.warn("Unknown option '{i}'. Ignoring".format(i=option_id))
        value = None

    return value


def validate_raw_task_options(registered_tasks, raw_opts_d):
    """
    Validates that the raw (CLI/XML) provided values are compatible with
    the json/schemas of all the tasks
    """
    opts = {}
    for option_id, raw_value, in raw_opts_d.iteritems():
        value = validate_raw_task_option(registered_tasks, option_id, raw_value)
        opts[option_id] = value

    return opts


def validate_workflow_options(d):
    """

    1. warn if an option provided in not a valid workflow option.
    2. try to coerce raw string values if possible

    Return a list of tuples [(id, value)] to be consistent with the existing API
    """
    for option_id in d:
        if option_id not in REGISTERED_WORKFLOW_OPTIONS:
            msg = "Unknown option. Ignoring workflow option '{i}'.".format(i=option_id)
            sys.stderr.write(msg + "\n")
            log.warn(msg)

    wopts = []
    for option_id, schema in REGISTERED_WORKFLOW_OPTIONS.iteritems():
        if option_id in d:
            raw_value = d[option_id]
            types_ = schema['properties'][option_id]['type']
            coerced_value = crude_coerce_type_from_str(raw_value, types_)
            _ = jsonschema.validate(schema, {option_id: coerced_value})
            wopts.append((option_id, coerced_value))
        else:
            # grab default
            value = OP.get_default_from_schema(schema)
            wopts.append((option_id, value))

    return wopts


def parse_entry_points(r):
    entry_points = []
    ens = r.findall(Constants.ENTRY_POINTS)
    enps = ens[0].findall(Constants.ENTRY_POINT)

    for n in enps:
        entry_points.append((n.attrib['id'], n.attrib['in']))

    return entry_points


def parse_bindings(r):
    bs = []
    bxs = r.findall(Constants.BINDINGS)
    bx = bxs[0]
    xs = bx.findall(Constants.BINDING)
    for n in xs:
        bs.append((n.attrib["out"], n.attrib["in"]))

    return bs


def __parse_template_id_to_bindings(root, registered_pipelines):
    template_id = _parse_template_id(root)

    if template_id not in registered_pipelines:
        raise PipelineTemplateIdNotFoundError("Unable to find Pipeline template '{i}' in {n} registered pipelines".format(i=template_id, n=len(registered_pipelines)))
    else:
        pipeline = registered_pipelines[template_id]

    return pipeline.all_bindings


def __parse_explicit_bindings(root, registered_pipelines):
    # fixme the registered pipelines are necessary to keep the interface
    bindings = parse_bindings(root)
    epoints = parse_entry_points(root)
    return bindings + epoints


def __parse_pipeline_template_xml(binding_func, file_name, registered_pipelines):

    t = ElementTree(file=file_name)
    r = t.getroot()

    bindings = binding_func(r, registered_pipelines)
    task_options = parse_task_options(r)
    wopts_tlist = parse_workflow_options(r)
    wopts = dict(wopts_tlist)
    workflow_options = validate_workflow_options(wopts)

    return BuilderRecord(bindings, task_options, workflow_options)

_parse_pipeline_template_xml_with_template_id = functools.partial(__parse_pipeline_template_xml, __parse_template_id_to_bindings)
_parse_pipeline_template = functools.partial(__parse_pipeline_template_xml, __parse_explicit_bindings)


def parse_pipeline_preset_xml(file_name):
    if not os.path.exists(file_name):
        raise IOError("Unable to find preset in {f}".format(f=file_name))

    t = ElementTree(file=file_name)
    r = t.getroot()
    task_options = parse_task_options(r)
    wopts_tlist = parse_workflow_options(r)
    wopts = dict(wopts_tlist)
    workflow_options = validate_workflow_options(wopts)
    return PresetRecord(task_options, workflow_options)


def parse_pipeline_template_xml(file_name, registered_pipelines):
    """

    :param file_name:
    :rtype: BuilderRecord
    """

    t = ElementTree(file=file_name)
    r = t.getroot()

    if _has_template_node(r):
        # parse template
        b = _parse_pipeline_template_xml_with_template_id(file_name, registered_pipelines)
    elif _has_entry_points_and_bindings(r):
        # Parse explicitly provided bindings and entry points
        b = _parse_pipeline_template(file_name, registered_pipelines)
    else:
        raise ValueError("Unable to find Workflow template id, or explicit bindings and entry points in {f}".format(f=file_name))

    return b


def load_preset_from_env(env_name=None):
    """
    Load the Preset from ENV variable

    """
    if env_name is None:
        env_name = ENV_PRESET

    p = os.environ.get(env_name, None)

    if p is not None:
        if os.path.isfile(p):
            preset_record = parse_pipeline_preset_xml(os.path.abspath(p))
            return preset_record
        else:
            log.warn("Unable to load RC preset from {x}".format(x=p))

    log.debug("Unable to find preset.xml from ENV '{e}'".format(e=env_name))
    return None


def schema_options_to_xml(option_type_name, schema_options_d):
    """Option type name is the task-option or option"""

    x = XMLBuilder(Constants.WORKFLOW_TEMPLATE_PRESET_ROOT)

    # Need to do this getattr to get around how the API works
    with getattr(x, option_type_name):
        for option_id, schema in schema_options_d.iteritems():
            default_value = schema['properties'][option_id]['default']
            if default_value is not None:
                with x.option(id=option_id):
                    default_value = schema['properties'][option_id]['default']
                    x.value(str(default_value))

    return x


def schema_task_options_to_xml(schema_options_d):
    return schema_options_to_xml(Constants.TASK_OPTIONS, schema_options_d)


def write_schema_task_options_to_xml(schema_options_d, output_file):
    xml = schema_task_options_to_xml(schema_options_d)
    with open(output_file, 'w') as w:
        w.write(str(xml))
    return 0


def schema_workflow_options_to_xml(schema_options_d):
    return schema_options_to_xml(Constants.WORKFLOW_OPTIONS, schema_options_d)


def pipeline_to_xml(p):
    """ Convert a Pipeline to XML

    :type p: Pipeline
    :param p:
    :return:
    """
    root = XMLBuilder(Constants.WORKFLOW_TEMPLATE_ROOT, id=p.idx)
    with getattr(root, Constants.ENTRY_POINTS):
        for eid, bid in p.entry_bindings:
            _d = {"id": eid, "in": bid}
            getattr(root, Constants.ENTRY_POINT)(**_d)
    with getattr(root, Constants.BINDINGS):
        for bout, bin_ in p.bindings:
            _d = {"out": bout, "in": bin_}
            getattr(root, Constants.BINDING)(**_d)

    return root


def _get_file_type_id(rtasks, task_type_id, input_index):
    return rtasks[task_type_id].input_types[input_index]


def _to_task_id_and_index(binding):
    s = binding.split(":")
    return s[0], int(s[1])


def sanity_entry_point(e_raw):
    return e_raw.split("$entry:")[-1]


def _pipeline_to_task_options(rtasks, p):
    bs = itertools.chain(*p.all_bindings)
    task_ids = [_to_task_id_and_index(b) for b in bs if not b.startswith("$entry:")]
    tids = [x for x, y in task_ids]
    rtsks = [rtasks[tid] for tid in tids]
    options = []
    for task in rtsks:
        if task.option_schemas:
            for k, v in task.option_schemas.iteritems():
                options.append(v)
    return options


def _option_jschema_to_pb_option(opt_jschema_d):
    """Convert from JsonSchema option to PacBioOption"""
    opt_id = opt_jschema_d['required'][0]
    name = opt_jschema_d['properties'][opt_id]['title']
    default = opt_jschema_d['properties'][opt_id]['default']
    desc = opt_jschema_d['properties'][opt_id]['description']
    pb_opt = PacBioOption(opt_id, name, default, desc)
    return pb_opt


def _to_entry_bindings(rtasks, a, b):
    entry_id = sanity_entry_point(a)
    task_id, t_in = _to_task_id_and_index(b)
    file_type = _get_file_type_id(rtasks, task_id, t_in)
    etype = file_type.file_type_id
    name = "Entry Name: {i}".format(i=file_type.file_type_id)
    return dict(file_type_id=etype, id=entry_id, name=name)


def _to_pipeline_binding(s):
    task_id, index, instance_id = binding_str_to_task_id_and_instance_id(s)
    return IOBinding(task_id, index, instance_id)


def pipeline_template_to_dict(pipeline, rtasks):
    """
    Convert and write the pipeline template to avro compatible dict

    :type pipeline: Pipeline
    """
    version = "0.1.2"
    options = []
    task_pboptions = []
    joptions = _pipeline_to_task_options(rtasks, pipeline)

    for jtopt in joptions:
            try:
                pbopt = _option_jschema_to_pb_option(jtopt)
                task_pboptions.append(pbopt)
            except Exception as e:
                sys.stderr.write("Failed to convert {p}\n".format(p=jtopt))
                raise e

    entry_points = [_to_entry_bindings(rtasks, bs[0], bs[1]) for bs in pipeline.entry_bindings]
    tags = ["sa3"]
    bindings = [PipelineBinding(_to_pipeline_binding(b_out),  _to_pipeline_binding(b_in)) for b_out, b_in in pipeline.bindings]

    desc = "Pipeline {i} " if pipeline.description is None else pipeline.description

    return dict(id=pipeline.pipeline_id,
                name=pipeline.display_name,
                version=version,
                entry_points=entry_points,
                bindings=[b.to_dict() for b in bindings],
                tags=tags,
                options=options,
                task_options=[x.to_dict() for x in task_pboptions],
                description=desc)


def write_pipeline_template_to_avro(pipeline, rtasks_d, output_file):

    d = pipeline_template_to_dict(pipeline, rtasks_d)
    f = open(output_file, 'w')
    with DataFileWriter(f, DatumWriter(), PT_SCHEMA) as writer:
        writer.append(d)

    return d


def load_pipeline_template_from_avro(path):
    f = open(path, 'r')
    with DataFileReader(f, DatumReader()) as reader:
        p = reader.next()

    return p


def write_pipeline_templates_to_avro(pipelines, rtasks_d, output_dir):
    output_files = []
    for pipeline in pipelines:
        name = pipeline.pipeline_id + "_pipeline_template.avro"
        file_name = os.path.join(output_dir, name)
        write_pipeline_template_to_avro(pipeline, rtasks_d, file_name)
        output_files.append(file_name)

    return output_files


def get_smrtanalysis_components(root):
    cs = [x.findall("component") for x in root.findall("components")][0]
    attrs = "build version name".split()

    def get_attrs(ce):
        return [ce.get(a) for a in attrs]

    return [SmrtAnalysisComponent(*get_attrs(c)) for c in cs]


def get_smrtanalysis_system(root_xml):
    cr = root_xml.findall("components")[0]

    def _get_value(name):
        return cr.get(name)
    return SmrtAnalysisSystem(_get_value("build"), _get_value("version"))


def get_smrtanalysis_system_and_components(file_name):
    et = ElementTree(file=file_name)
    r = et.getroot()
    return get_smrtanalysis_system(r), get_smrtanalysis_components(r)


def get_smrtanalysis_system_and_components_from_env():
    """Helper method to grab the resources from SMRTAnalysis config.xml"""
    path = os.environ[SEYMOUR_HOME]
    config_xml = os.path.join(path, "etc", "config.xml")
    return get_smrtanalysis_system_and_components(config_xml)


def write_env_to_json(json_file):

    # not completely sure why this has to be done. json.dumps(os.environ) will
    # raise a Serialization error
    d = {k: v for k, v in os.environ.iteritems()}

    with open(json_file, 'w') as f:
        f.write(json.dumps(d, sort_keys=True, indent=4))

    return True


def write_pipeline_chunks(chunks, output_json_file, comment):

    _d = dict(nchunks=len(chunks), _version="0.1.0",
              chunks=[c.to_dict() for c in chunks])

    if comment is not None:
        _d['_comment'] = comment

    with open(output_json_file, 'w') as f:
        f.write(json.dumps(_d, indent=4))

    log.debug("Write {n} chunks to {o}".format(n=len(chunks), o=output_json_file))


def load_pipeline_chunks_from_json(path):
    """Returns a list of Pipeline Chunks


    :rtype: list[PipelineChunk]
    """

    try:
        with open(path, 'r') as f:
            d = json.loads(f.read())

        chunks = []
        for cs in d['chunks']:
            chunk_id = cs['chunk_id']
            chunk_datum = cs['chunk']
            c = PipelineChunk(chunk_id, **chunk_datum)
            chunks.append(c)
    except Exception:
        msg = "Unable to load pipeline chunks from {f}".format(f=path)
        slog.error(msg)
        sys.stderr.write(msg + "\n")
        raise

    return chunks


def parse_operator_xml(f):

    et = ElementTree(file=f)
    r = et.getroot()

    def _get_value_from_first_element(r_, e_name):
        return r_.findall(e_name)[0].text

    operator_id = r.attrib['id']
    task_id = _get_value_from_first_element(r, 'task-id')

    s = r.findall('scatter')[0]
    scatter_task_id = _get_value_from_first_element(s, 'scatter-task-id')

    sgs = s.findall('chunks')[0]
    schunks = [ScatterChunk(x.attrib['out'], x.attrib['in']) for x in sgs.findall('chunk')]
    scatter = Scatter(task_id, scatter_task_id, schunks)

    gs = r.findall('gather')[0].findall('chunks')[0].findall('chunk')

    def _to_c(x):
        return _get_value_from_first_element(x, 'gather-task-id'), _get_value_from_first_element(x, 'chunk-key'), _get_value_from_first_element(x, 'task-output')

    gchunks = [GatherChunk(*_to_c(x)) for x in gs]

    gather = Gather(gchunks)
    return ChunkOperator(operator_id, scatter, gather)


def _to_meta_task(tc, task_type, input_types, output_types, schema_option_d):
    output_file_names = []
    mutable_files = []
    return ToolContractMetaTask(tc,
                                tc.task.task_id,
                                task_type,
                                input_types,
                                output_types,
                                schema_option_d,
                                tc.task.nproc,
                                tc.task.resources,
                                output_file_names,
                                mutable_files,
                                tc.task.description,
                                tc.task.name,
                                version=tc.task.version)


def _to_meta_scatter_task(tc, task_type, input_types, output_types,
                          schema_option_d, max_nchunks, chunk_keys):
    output_file_names = []
    mutable_files = []
    return ScatterToolContractMetaTask(tc,
                                       tc.task.task_id,
                                       task_type,
                                       input_types,
                                       output_types,
                                       schema_option_d,
                                       tc.task.nproc,
                                       tc.task.resources,
                                       output_file_names,
                                       mutable_files,
                                       tc.task.description,
                                       tc.task.name,
                                       max_nchunks, chunk_keys,
                                       version=tc.task.version)


def _to_meta_gather_task(tc, task_type, input_types, output_types, schema_option_d):
    output_file_names = []
    mutable_files = []
    return GatherToolContractMetaTask(tc,
                                      tc.task.task_id,
                                      task_type,
                                      input_types,
                                      output_types,
                                      schema_option_d,
                                      tc.task.nproc,
                                      tc.task.resources,
                                      output_file_names,
                                      mutable_files,
                                      tc.task.description,
                                      tc.task.name,
                                      version=tc.task.version)


def tool_contract_to_meta_task(tc, max_nchunks):
    """Shim layer to load tool contracts and convert them to MetaTask type

    """
    # there needs to be special attention here. This is side stepping all the
    # validation layers used in the rest of the code.

    def _get_ft(x_):
        return REGISTERED_FILE_TYPES[x_]

    schema_option_d = {opt['required'][0]: opt for opt in tc.task.options}

    # resolve strings to FileType instances
    input_types = validate_provided_file_types([_get_ft(x.file_type_id) for x in tc.task.input_file_types])
    output_types = validate_provided_file_types([_get_ft(x.file_type_id) for x in tc.task.output_file_types])

    #
    task_type = validate_task_type(tc.task.is_distributed)

    if isinstance(tc.task, ScatterToolContractTask):
        meta_task = _to_meta_scatter_task(tc, task_type, input_types, output_types, schema_option_d, max_nchunks, 'chunk-key')
    elif isinstance(tc.task, GatherToolContractTask):
        meta_task = _to_meta_gather_task(tc, task_type, input_types, output_types, schema_option_d)
    elif isinstance(tc.task, ToolContractTask):
        meta_task = _to_meta_task(tc, task_type, input_types, output_types, schema_option_d)
    else:
        raise TypeError("Unsupported Type {t} {x}".format(x=tc.task, t=type(tc.task)))

    return meta_task


def tool_contract_to_meta_task_from_file(path):
    """Loads a tool contract from a path and converts it to a StaticMetaTask"""
    tc = load_tool_contract_from(path)
    # FIXME
    max_chunks = 5
    return tool_contract_to_meta_task(tc, max_chunks)


def write_tool_contract(tc, path):
    """:type tc: pbcommand.models.ToolContract"""

    with open(path, 'w') as f:
        f.write(json.dumps(tc.to_dict(), sort_keys=True, indent=4))

    return tc


def static_meta_task_to_rtc(static_meta_task, task, task_options, task_dir, tmp_dir, max_nproc):
    """

    Shim layer to converts a static metatask to ResolvedToolContract

    :type static_meta_task: ToolContractMetaTask
    :type task: MetaTask
    :param static_meta_task:
    :return: dict representation of driver manifest
    """
    rtc = resolve_tool_contract(static_meta_task.tool_contract, task.input_files, task_dir, tmp_dir, max_nproc, task_options)
    # this is a hack because the 'resolving' is done in meta_task_to_task
    # for python defined tasks. This code path will be deleted shortly
    rtc.task.output_files = task.output_files
    rtc.task.is_distributed = static_meta_task.is_distributed
    rtc.task.nproc = task.nproc
    task.resources = task.resources

    return rtc


def static_scatter_meta_task_to_rtc(static_meta_task, task, task_options, task_dir, tmp_dir, max_nproc, max_nchunks, chunk_keys):
    rtc = resolve_scatter_tool_contract(static_meta_task.tool_contract, task.input_files, task_dir, tmp_dir, max_nproc, task_options, max_nchunks, chunk_keys)
    # See the above comment for this
    rtc.task.output_files = task.output_files
    rtc.task.is_distributed = static_meta_task.is_distributed
    rtc.task.nproc = task.nproc
    task.resources = task.resources

    return rtc


def static_gather_meta_task_to_rtc(static_meta_task, task, task_options, task_dir, tmp_dir, max_nproc, chunk_key):

    rtc = resolve_gather_tool_contract(static_meta_task.tool_contract, task.input_files, task_dir, tmp_dir, max_nproc, task_options, chunk_key)
    # See the above comment for this
    rtc.task.output_files = task.output_files
    rtc.task.is_distributed = static_meta_task.is_distributed
    rtc.task.nproc = task.nproc
    task.resources = task.resources

    return rtc
