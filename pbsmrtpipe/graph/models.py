import logging
import json
import datetime

from pbcommand.models.common import FileType

from pbsmrtpipe.models import (TaskStates, MetaStaticTask,
                               MetaTask, MetaScatterTask)
from pbsmrtpipe.pb_io import strip_entry_prefix
from pbsmrtpipe.utils import validate_type_or_raise

log = logging.getLogger(__name__)


class DotShapeConstants(object):
    """Commonly used shapes so I don't have to keep looking this up"""
    ELLIPSE = "ellipse"
    RECTANGLE = "rectangle"
    OCTAGON = "octagon"
    TRIPLE_OCTAGON = 'tripleoctagon'
    # Use for scatter task
    TRIANGLE_UP = 'triangle'
    # Use for gather
    TRIANGLE_DOWN = 'invtriangle'

    DIAMOND = 'diamond'
    PARALLELOGRAM = 'parallelogram'


class DotColorConstants(object):
    """Colors so I don't have to keep looking this up

    http://www.graphviz.org/doc/info/colors.html
    """
    AQUA = "aquamarine"
    AQUA_DARK = 'aquamarine3'
    CYAN = 'cyan'
    RED = "red"
    BLUE = 'blue'
    ORANGE = 'orange'
    WHITE = 'azure'
    PURPLE = 'mediumpurple'
    PURPLE_DARK = 'mediumpurple4'
    GREY = 'grey'


class DotStyleConstants(object):
    DOTTED = 'dotted'
    FILLED = 'filled'


class Constants(object):
    TASK_FAILED_COLOR = DotColorConstants.RED


class ConstantsNodes(object):
    FILE_ATTR_IS_RESOLVED = 'is_resolved'
    FILE_ATTR_PATH = 'path'
    FILE_ATTR_RESOLVED_AT = 'resolved_at'

    TASK_ATTR_STATE = 'state'
    TASK_ATTR_NPROC = 'nproc'
    TASK_ATTR_ROPTS = 'ropts'
    TASK_ATTR_CMDS = 'cmds'
    TASK_ATTR_EMESSAGE = "error_message"
    TASK_ATTR_RUN_TIME = 'run_time'
    TASK_ATTR_UPDATED_AT = 'updated_at'
    TASK_ATTR_CREATED_AT = 'created_at'

    # On the chunk-able task, store metadata about the o
    TASK_ATTR_IS_CHUNKABLE = 'is_chunkable'
    # Was the chunked applied to the scattered chunk
    TASK_ATTR_WAS_CHUNKED = 'was_chunked'
    TASK_ATTR_COMPANION_CHUNK_TASK_ID = "companion_chunk_task_id"
    TASK_ATTR_IS_CHUNK_RUNNING = 'is_chunk_scatter_running'
    TASK_ATTR_OPERATOR_ID = 'operator_id'


class _NodeLike(object):
    """Base Graph Node type"""
    NODE_ATTRS = {}


class _NodeEqualityMixin(object):

    def __repr__(self):
        return ''.join(['<', str(self), '>'])

    def __str__(self):
        return "{k}_{i}".format(k=self.__class__.__name__, i=self.idx)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self.idx == other.idx:
                return True
        return False

    def __ne__(self, other):
        return not self == other


class _DotAbleMixin(object):
    DOT_SHAPE = DotShapeConstants.ELLIPSE
    DOT_COLOR = DotColorConstants.WHITE


class _FileLike(_NodeLike):
    # Attributes initialized at the graph level
    NODE_ATTRS = {ConstantsNodes.FILE_ATTR_IS_RESOLVED: False,
                  ConstantsNodes.FILE_ATTR_PATH: None,
                  ConstantsNodes.FILE_ATTR_RESOLVED_AT: None}


class _TaskLike(_NodeLike):
    # Attributes initialized at the graph level
    NODE_ATTRS = {ConstantsNodes.TASK_ATTR_STATE: TaskStates.CREATED,
                  ConstantsNodes.TASK_ATTR_ROPTS: {},
                  ConstantsNodes.TASK_ATTR_NPROC: 1,
                  ConstantsNodes.TASK_ATTR_CMDS: [],
                  ConstantsNodes.TASK_ATTR_RUN_TIME: None,
                  ConstantsNodes.TASK_ATTR_CREATED_AT: lambda : datetime.datetime.now(),
                  ConstantsNodes.TASK_ATTR_UPDATED_AT: lambda : datetime.datetime.now(),
                  ConstantsNodes.TASK_ATTR_EMESSAGE: None,
                  ConstantsNodes.TASK_ATTR_IS_CHUNKABLE: False,
                  ConstantsNodes.TASK_ATTR_IS_CHUNK_RUNNING: False,
                  ConstantsNodes.TASK_ATTR_WAS_CHUNKED: False}


class _ChunkLike(object):
    # Must have self.chunk_id
    pass


class EntryPointNode(_NodeEqualityMixin, _DotAbleMixin, _TaskLike):
    # this is like a Task
    # This abstraction needs to be deleted
    NODE_ATTRS = {'is_resolved': False, 'path': None}

    DOT_COLOR = DotColorConstants.PURPLE
    DOT_SHAPE = DotShapeConstants.DIAMOND

    def __init__(self, idx, file_klass):
        """

        :type idx: str
        :type file_klass: FileType

        :param idx:
        :param file_klass:
        :return:
        """
        self.idx = idx
        # FileType instance
        self.file_klass = file_klass

    def __str__(self):
        _d = dict(k=self.__class__.__name__, i=self.idx, f=self.file_klass.file_type_id)
        return "{k} {i} {f}".format(**_d)


class TaskBindingNode(_NodeEqualityMixin, _DotAbleMixin, _TaskLike):
    """ Standard base Task Node """
    DOT_COLOR = DotColorConstants.AQUA
    DOT_SHAPE = DotShapeConstants.OCTAGON

    def __init__(self, meta_task, instance_id):
        """

        :type meta_task: MetaTask
        :type instance_id: int

        :param meta_task:
        :param instance_id:
        """

        self.meta_task = validate_type_or_raise(meta_task, (MetaTask, MetaStaticTask))
        self.instance_id = validate_type_or_raise(instance_id, int)

    @property
    def url(self):
        # for backwards compatible with the driver
        return str(self)

    @property
    def idx(self):
        return self.meta_task.task_id

    def __str__(self):
        _d = dict(k=self.__class__.__name__, i=self.idx, n=self.instance_id)
        return "{k} {i}-{n}".format(**_d)


class TaskChunkedBindingNode(TaskBindingNode):
    """Chunked "instances" of a Task node, must have chunk_id"""

    DOT_SHAPE = DotShapeConstants.TRIPLE_OCTAGON
    DOT_COLOR = DotColorConstants.AQUA_DARK

    def __init__(self, meta_task, instance_id, chunk_id):
        super(TaskChunkedBindingNode, self).__init__(meta_task, instance_id)
        self.chunk_id = chunk_id

    def __str__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.idx, n=self.instance_id, c=self.chunk_id)
        return "{k}-{c} {i}-{n}".format(**_d)


class TaskScatterBindingNode(TaskBindingNode):
    """Scattered Task that produces a chunk.json output file type

    This will have a 'companion' task that shares the same input file type signature
    """

    DOT_SHAPE = DotShapeConstants.OCTAGON
    DOT_COLOR = DotColorConstants.ORANGE

    def __init__(self, scatter_meta_task, original_task_id, instance_id):
        validate_type_or_raise(scatter_meta_task, MetaScatterTask)
        super(TaskScatterBindingNode, self).__init__(scatter_meta_task, instance_id)
        # Keep track of the original task that was chunked
        self.original_task_id = original_task_id


class TaskGatherBindingNode(TaskBindingNode):
    """Gathered Task node. Consumes a gathered chunk.json and emits a single
    file type
    """
    DOT_SHAPE = DotShapeConstants.OCTAGON
    DOT_COLOR = DotColorConstants.GREY

    def __init__(self, meta_task, instance_id, chunk_key):
        super(TaskGatherBindingNode, self).__init__(meta_task, instance_id)
        # Keep track of the chunk_key that was passed to the exe.
        # Perhaps this should be in the meta task instance?
        self.chunk_key = chunk_key


class _BindingFileNode(_NodeEqualityMixin, _DotAbleMixin, _FileLike):
    # Grab from meta task
    ATTR_NAME = "input_types"
    # Used as a label in dot
    DIRECTION = "IN"

    DOT_COLOR = DotColorConstants.WHITE
    DOT_SHAPE = DotShapeConstants.ELLIPSE

    def __init__(self, meta_task, instance_id, index, file_type_instance):
        """

        :type meta_task: MetaTask
        :type instance_id: int
        :type index: int
        :type file_type_instance: FileType
        """

        # Not sure this is a great idea
        self.meta_task = validate_type_or_raise(meta_task, MetaTask)

        # task index (int)
        self.instance_id = validate_type_or_raise(instance_id, int)

        # positional index of input/output
        self.index = validate_type_or_raise(index, int)

        # this is a little odd. The input/output type are not necessarily identical
        self.file_klass = validate_type_or_raise(file_type_instance, FileType)

    @property
    def task_instance_id(self):
        # this is the {file klass}-{Instance id}
        types_ = getattr(self.meta_task, self.__class__.ATTR_NAME)
        file_type = types_[self.index]
        return "-".join([file_type.file_type_id, str(self.instance_id)])

    @property
    def idx(self):
        # the fundamental id used in the graph
        return "{n}.{i}".format(n=self.task_instance_id, i=self.index)

    def __str__(self):
        _d = dict(k=self.__class__.__name__,
                  d=self.__class__.DIRECTION,
                  i=self.idx,
                  f=self.file_klass.file_type_id,
                  n=self.meta_task.task_id,
                  m=self.instance_id)
        return "{k} {n}-{m} {i}".format(**_d)


class BindingInFileNode(_BindingFileNode):
    DIRECTION = "in"
    # Grab from meta task
    ATTR_NAME = "input_types"

    DOT_SHAPE = DotShapeConstants.ELLIPSE


class BindingChunkInFileNode(BindingInFileNode):
    def __init__(self, meta_task, instance_id, index, file_type_instance, chunk_id):
        super(BindingChunkInFileNode, self).__init__(meta_task, instance_id, index, file_type_instance)
        self.chunk_id = chunk_id


class BindingOutFileNode(_BindingFileNode):
    DIRECTION = "out"
    ATTR_NAME = "output_types"

    DOT_SHAPE = DotShapeConstants.OCTAGON


class BindingChunkOutFileNode(BindingOutFileNode):
    def __init__(self, meta_task, instance_id, index, file_type_instance, chunk_id):
        super(BindingChunkOutFileNode, self).__init__(meta_task, instance_id, index, file_type_instance)
        self.chunk_id = chunk_id


class EntryOutBindingFileNode(_NodeEqualityMixin, _DotAbleMixin, _FileLike):
    DOT_SHAPE = DotShapeConstants.RECTANGLE
    DOT_COLOR = DotColorConstants.WHITE

    def __init__(self, entry_id, file_klass):
        self.entry_id = strip_entry_prefix(entry_id)
        # FileType instance
        self.file_klass = file_klass
        self.instance_id = 0
        self.index = 0
        self.direction = 'out'

    def __str__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.entry_id,
                  f=self.file_klass.file_type_id)
        return "{k} {f} {i}".format(**_d)

    @property
    def idx(self):
        _d = dict(n=self.entry_id, i=self.index)
        return "{n}.{i}".format(**_d)


VALID_FILE_NODE_ClASSES = (BindingInFileNode, BindingOutFileNode, EntryOutBindingFileNode)
VALID_TASK_NODE_CLASSES = (TaskBindingNode, EntryPointNode)