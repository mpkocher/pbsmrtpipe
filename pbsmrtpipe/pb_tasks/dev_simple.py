import logging

from pbsmrtpipe.core import MetaTaskBase
from pbsmrtpipe.models import FileTypes, TaskTypes, ResourceTypes, SymbolTypes
import pbsmrtpipe.schema_opt_utils as OP

log = logging.getLogger(__name__)


def _get_opts():
    # Util func to create an task option id 'pbsmrtpipe.task_options.dev.hello_message'
    oid = OP.to_opt_id('dev.hello_message')
    return {oid: OP.to_option_schema(oid, "string", 'Hello Message', "Hello Message for dev Task.", "Default Message")}


class DevSimpleHelloWorldTask(MetaTaskBase):

    """My long Task Description goes here

    This task cats the output of input file into the output file.
    """
    TASK_ID = 'pbsmrtpipe.tasks.dev_simple_hello_world'
    # Task Display Name
    NAME = "Dev Simple Hello World"
    # Semantic style version
    VERSION = "1.0.0"

    # Task types can be local, or submitted to the cluster (TaskTypes.DISTRIBUTED)
    TASK_TYPE = TaskTypes.LOCAL

    # list of (FileType, binding label, description)
    INPUT_TYPES = [(FileTypes.TXT, "txt", "Text File")]
    OUTPUT_TYPES = [(FileTypes.TXT, "txt", "Text File with data")]

    # Override the default output file names. Must provide a value for each
    # output type.
    OUTPUT_FILE_NAMES = [("my_custom_file_name", "txt")]

    # dict of {option_id:schema}
    SCHEMA_OPTIONS = _get_opts()
    # int, or "$max_nproc" (i.e, SymbolTypes.MAX_NPROC)
    NPROC = 1

    # Accessible Resource types. The resources will be created and deleted
    # by the workflow
    RESOURCE_TYPES = (ResourceTypes.TMP_FILE, ResourceTypes.TMP_DIR)

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        cmds = []
        cmds.append("cat {i} > {o}".format(i=input_files[0], o=output_files[0]))
        cmds.append('echo "{m}" >> {o}'.format(m=ropts[OP.to_opt_id('dev.hello_message')], o=output_files[0]))
        return cmds
