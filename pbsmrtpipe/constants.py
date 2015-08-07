import functools
# This is where all the rdf imports should be. Other modules should import them
# from here because of the plugin registry.
import re
import rdflib
rdflib.plugin.register('sparql', rdflib.query.Processor,
                       'rdfextras.sparql.processor', 'Processor')
rdflib.plugin.register('sparql', rdflib.query.Result,
                       'rdfextras.sparql.query', 'SPARQLQueryResult')
from rdflib import Namespace
from rdflib import ConjunctiveGraph as RdfGraph
from rdflib import URIRef, Literal

__all__ = ['LegacyConstants', 'Namespace', 'RdfGraph', 'URIRef', 'Literal']


ENV_PRESET = 'PB_SMRTPIPE_XML_PRESET'

SMRT_RDF_NAMESPACE = Namespace("pype://v0.1/")

DEEP_DEBUG = False

# Global Env vars that are necessary to do *anything*. This is stupid.
SEYMOUR_HOME = 'SEYMOUR_HOME'

SLOG_PREFIX = 'status.'

DATASTORE_VERSION = "0.2.1"
CHUNK_API_VERSION = "0.1.0"

ENTRY_PREFIX = "$entry:"

# Generic Id
RX_TASK_ID = re.compile(r'^([A-z0-9_]*)\.tasks\.([A-z0-9_]*)$')
RX_PIPELINE_ID = re.compile(r'^([A-z0-9_]*)\.pipelines\.([A-z0-9_\.]*)$')
RX_FILE_TYPES_ID = re.compile(r'^([A-z0-9_]*)\.files\.([A-z0-9_\.]*)$')
RX_TASK_OPTION_ID = re.compile(r'^([A-z0-9_]*)\.task_options\.([A-z0-9_\.]*)')

# Bindings format
# Only Task includes the :0
RX_BINDING_TASK = re.compile(r'^([A-z0-9_]*)\.tasks\.([A-z0-9_]*):(\d*)$')
# Advanced bindings referencing an instance of a task
# {namespace}:tasks:{instance-id}:{in_out_index}
RX_BINDING_TASK_ADVANCED = re.compile(r'^([A-z0-9_]*)\.tasks\.([A-z0-9_]*):(\d*):(\d*)$')

# {pipeline_id}:{task_id}:{instance_id}
RX_BINDING_PIPELINE_TASK = re.compile(r'^([A-z0-9_]*).pipelines.([A-z0-9_]*):([A-z0-9_]*).tasks.(\w*):([0-9]*)$')
# {pipeline_id}:$entry:{entry_label}
RX_BINDING_PIPELINE_ENTRY = re.compile(r'^([A-z0-9_]*).pipelines.([A-z0-9_]*):\$entry:([A-z0-9_]*)$')
# Only Entry points
RX_ENTRY = re.compile(r'^\$entry:([A-z0-9_]*)$')
# to be consistent with the new naming scheme
RX_BINDING_ENTRY = re.compile(r'^\$entry:([A-z0-9_]*)$')

RX_VALID_BINDINGS = (RX_BINDING_PIPELINE_TASK, RX_BINDING_PIPELINE_ENTRY, RX_BINDING_TASK, RX_BINDING_ENTRY)

# This should really use a semantic version lib
RX_VERSION = re.compile(r'(\d*).(\d*).(\d*)')

# Chunk Key $chunk.my_label_id
RX_CHUNK_KEY = re.compile(r'^\$chunk\.([A-z0-9_]*)')
RX_CHUNK_ID = re.compile(r'(^[A-z0-9_]*)')


TASK_MANIFEST_JSON = 'task-manifest.json'
RUNNABLE_TASK_JSON = "runnable-task.json"
TASK_MANIFEST_VERSION = '0.3.0'

RESOLVED_TOOL_CONTRACT_JSON = "resolved-tool-contract.json"
TOOL_CONTRACT_JSON = "tool-contract.json"

#
MAX_NCHUNKS = 24


class PacBioNamespaces(object):
    # File Types
    #PBSMRTPIPE_FILE_PREFIX = 'pbsmrtpipe.files'
    # NEW File Type Identifier style Prefix
    NEW_PBSMRTPIPE_FILE_PREFIX = "PacBio.FileTypes"
    # New DataSet Identifier Prefix
    DATASET_FILE_PREFIX = "PacBio.DataSet"
    # Task Ids
    PBSMRTPIPE_TASK_PREFIX = 'pbsmrtpipe.tasks'
    # Task Options
    PBSMRTPIPE_TASK_OPTS_PREFIX = 'pbsmrtpipe.task_options'
    # Workflow Level Options
    PBSMRTPIPE_OPTS_PREFIX = 'pbsmrtpipe.options'
    # Constants
    PBSMRTPIPE_CONSTANTS_PREFIX = 'pbsmrtpipe.constants'
    # Pipelines
    PBSMRTPIPE_PIPELINES = "pbsmrtpipe.pipelines"


def __to_type(prefix, name):
    return ".".join([prefix, name])

to_constant_ns = functools.partial(__to_type, PacBioNamespaces.PBSMRTPIPE_CONSTANTS_PREFIX)
to_file_ns = functools.partial(__to_type, PacBioNamespaces.NEW_PBSMRTPIPE_FILE_PREFIX)
to_ds_ns = functools.partial(__to_type, PacBioNamespaces.DATASET_FILE_PREFIX)
to_task_option_ns = functools.partial(__to_type, PacBioNamespaces.PBSMRTPIPE_TASK_OPTS_PREFIX)
to_task_ns = functools.partial(__to_type, PacBioNamespaces.PBSMRTPIPE_TASK_PREFIX)
to_workflow_option_ns = functools.partial(__to_type, PacBioNamespaces.PBSMRTPIPE_OPTS_PREFIX)
to_pipeline_ns = functools.partial(__to_type, PacBioNamespaces.PBSMRTPIPE_PIPELINES)
