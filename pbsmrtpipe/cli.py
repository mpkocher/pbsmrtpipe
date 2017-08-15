import copy
import json
import os
import pprint
import sys
import logging
import urlparse
from collections import OrderedDict

from pbcommand.utils import setup_log, compose
from pbcommand.cli.utils import main_runner, args_executer, subparser_builder
from pbcommand.common_options import add_log_debug_option
from pbcommand.cli import get_default_argparser
from pbcommand.models.common import BaseChoiceType #FIXME this should be exposed from models
from pbcommand.validators import validate_file
from pbsmrtpipe.core import binding_str_is_entry_id
from pbsmrtpipe.tools.diagnostics import (run_diagnostics,
                                          run_simple_diagnostics)

import pbsmrtpipe
import pbsmrtpipe.tools.utils as TU
from pbsmrtpipe.exceptions import MalformedEntryStrError
from pbsmrtpipe.models import MetaTask, MetaScatterTask, MetaGatherTask
import pbsmrtpipe.pb_io as IO
import pbsmrtpipe.driver as D
from pbsmrtpipe.utils import StdOutStatusLogFilter

from pbsmrtpipe.constants import (ENV_PRESET, ENTRY_PREFIX, RX_ENTRY, ENV_TC_DIR,
                                  ENV_CHK_OPT_DIR)

log = logging.getLogger()
slog = logging.getLogger('status.' + __file__)


def _validate_preset_xml(path):

    _, _, _, pipelines = __dynamically_load_all()

    if os.path.exists(path):
        _ = IO.parse_pipeline_template_xml(os.path.abspath(path), pipelines)
        return os.path.abspath(path)
    raise IOError("Unable to find preset '{f}'".format(f=path))


def add_log_file_options(p):
    p.add_argument('--log-file', type=str, default=None, help="Path to log file")
    p.add_argument('--log-file', type=str, default=None, help="Path to log file")
    return p

LOG_LEVELS = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
LOG_LEVELS_D = {attr: getattr(logging, attr) for attr in LOG_LEVELS}


def add_log_level_option(p):
    p.add_argument('--log-level',
                   default='INFO',
                   choices=LOG_LEVELS, help="Log LEVEL")
    return p


def _add_template_id_option(p):
    p.add_argument('template_id', type=str, help="Show details of Pipeline Template.")
    return p


def _add_task_id_option(p, help_msg="Show details of registered Task by id."):
    p.add_argument('task_id', type=str, help=help_msg)
    return p


def _add_task_id_run_option(p):
    m = "Run a single task by Tool Contract Id (e.g., pbcommand.tasks.dev_txt_hello). Use 'pbsmrtpipe show-tasks' to get a complete list of registered tasks. "
    return _add_task_id_option(p, help_msg=m)


def _validate_dir_or_create(p):
    x = os.path.abspath(os.path.expanduser(p))
    if os.path.exists(x):
        return x
    else:
        os.mkdir(x)
        return x


def _add_output_dir_option(p):
    p.add_argument('-o', '--output-dir', default=os.getcwd(),
                   type=_validate_dir_or_create,
                   help="Path to job output directory. Directory will be created if it does not exist.")
    return p


def _add_entry_point_option(p, help_msg="Entry Points using 'entry_idX:/path/to/file.txt' format."):
    p.add_argument('-e', '--entry', dest="entry_points", required=True,
                   action="append",
                   nargs="+", type=_validate_entry, help=help_msg)
    return p


def _add_preset_xml_option(p):
    p.add_argument('--preset-xml', action="append", type=validate_file,
                   default=[],
                   help="Preset/Option XML file.  This option may be "+
                        "repeated if you have multiple preset files.")
    return p


def _add_preset_json_option(p):
    p.add_argument('--preset-json', action="append", type=validate_file,
                   default=[],
                   help="Preset/Option JSON file.  This option may be "+
                        "repeated if you have multiple preset files.")
    return p


def _add_rc_preset_xml_option(p):
    p.add_argument('--preset-rc-xml', type=validate_file,
                   help="Skipping loading preset from ENV var '{x}' and Explicitly load the supplied preset.xml".format(x=ENV_PRESET))
    return p


def _add_output_preset_xml_option(p):
    p.add_argument('-j', '--output-preset-json', type=str, help="Write pipeline/task preset.json of options.")
    p.add_argument('-o', '--output-preset-xml', type=str, help="Write pipeline/task preset.xml of options.")
    return p


def _pretty_registered_pipelines(pipelines, pipeline_type):
    n = len(pipelines)
    title = "{n} Registered {t} Pipelines (name -> version, id)".format(n=n, t=pipeline_type)
    header = len(title) * "*"

    outs = []
    outs.append(header)
    outs.append(title)
    outs.append(header)
    max_name_len = max(len(pipeline.display_name) for pipeline in pipelines)
    pad = 4 + max_name_len

    spipelines = sorted(pipelines, key=lambda x: x.display_name)
    for i, k in enumerate(spipelines):
        outs.append(" ".join([(str(i + 1) + ".").rjust(4), k.display_name.ljust(pad), k.version, k.idx]))

    return "\n".join(outs)


def pretty_registered_pipelines(registered_new_pipelines_d, show_all=True):
    def _is_user_pipeline(pipeline):
        return not ("dev" in pipeline.tags or "internal" in pipeline.tags)
    pipelines = registered_new_pipelines_d.values()
    user_pipelines = [p for p in pipelines if _is_user_pipeline(p)]
    internal_pipelines = [p for p in pipelines if not _is_user_pipeline(p)]

    outs = [_pretty_registered_pipelines(user_pipelines, "User")]
    if show_all:
        outs.append(_pretty_registered_pipelines(internal_pipelines, "Developer/Internal"))
    else:
        outs.append("Run with --show-all to display developer/internal pipelines")
    return "\n\n".join(outs)


def pretty_bindings(bindings):
    entry_points = {i for i, o in bindings if i.startswith(ENTRY_PREFIX)}

    outs = []
    outs.append("**** Entry points ({n}) ****".format(n=len(entry_points)))

    for i, entry_point in enumerate(entry_points):
        outs.append(entry_point)

    max_length = max(len(i) for i, o in bindings)
    pad = 4

    outs.append("")
    outs.append("**** Bindings ({n}) ****".format(n=len(bindings)))

    for i, o in bindings:
        outs.append(" -> ".join([i.rjust(max_length + pad), o]))

    return "\n".join(outs)


def run_show_templates(avro_output_dir=None, json_output_dir=None,
                       show_all=False):
    import pbsmrtpipe.loader as L
    from pbsmrtpipe.pb_io import (write_pipeline_templates_to_avro,
                                  write_pipeline_templates_to_json)

    rtasks_d, _, _, pts = L.load_all()

    print pretty_registered_pipelines(pts, show_all=show_all)

    if avro_output_dir is not None:
        write_pipeline_templates_to_avro(pts.values(), rtasks_d, avro_output_dir)

    if json_output_dir is not None:
        write_pipeline_templates_to_json(pts.values(), rtasks_d, json_output_dir)

    return 0


def add_run_show_templates_options(p):
    add_log_level_option(p)

    def _to_h(m):
        return "Resolve, Validate and Output Registered pipeline templates to {m} files to output-dir".format(m=m)

    p.add_argument('--output-templates-avro', type=str, help=_to_h("AVRO"))
    p.add_argument('--output-templates-json', type=str, help=_to_h("JSON"))
    p.add_argument('--show-all', action="store_true", help="Display developer/internal pipelines")

    return p


def _args_run_show_templates(args):
    return run_show_templates(avro_output_dir=args.output_templates_avro,
                              json_output_dir=args.output_templates_json,
                              show_all=args.show_all)


def write_task_options_to_preset_xml_and_print(task_options_d, output_file, warning_msg):
    if task_options_d:
        IO.write_schema_task_options_to_xml(task_options_d, output_file)
        print "Wrote preset to {x}".format(x=output_file)
    else:
        print warning_msg


def write_presets_json_and_print(p, opts, output_file, warning_msg):
    if opts:
        IO.write_pipeline_presets_json(p, opts, output_file)
        print "Wrote preset to {j}".format(j=output_file)
    else:
        print warning_msg


def run_show_template_details(template_id, output_preset_xml, output_preset_json):

    rtasks, rfiles, operators, pipelines_d = __dynamically_load_all()

    from pbsmrtpipe.pb_io import binding_str_to_task_id_and_instance_id

    pb_options = {}

    if template_id in pipelines_d:
        pipeline = pipelines_d[template_id]
        print "**** Pipeline Summary ****"
        print "id            : {i}".format(i=pipeline.idx)
        print "version       : {i}".format(i=pipeline.version)
        print "name          : {x}".format(x=pipeline.display_name)
        # print "Schema version: {}".format(pipeline.schema_version)
        if pipeline.tags:
            print "Tags       : {t} ".format(t=",".join(pipeline.tags))
        print "Description: \n {x}".format(x=pipeline.description.strip())

        print
        print pretty_bindings(pipeline.all_bindings)

        for b_out, b_in, in pipeline.all_bindings:
                for x in (b_out, b_in):
                    if not binding_str_is_entry_id(x):
                        task_id, _, _ = binding_str_to_task_id_and_instance_id(x)
                        task = rtasks.get(task_id, None)
                        if task is None:
                            log.warn("Unable to load task {x}".format(x=task_id))
                        else:
                            for pb_opt in task.option_schemas:
                                if pb_opt.option_id in pb_options:
                                    continue
                                elif pb_opt.option_id in pipeline.task_options:
                                    x = copy.deepcopy(pb_opt)
                                    value = pipeline.task_options[pb_opt.option_id]
                                    x._default = value # XXX hacky
                                    pb_options[pb_opt.option_id] = x
                                else:
                                    pb_options[pb_opt.option_id] = pb_opt

        warn_msg = "Pipeline {i} has no options.".format(i=pipeline.idx)
        task_options_d = OrderedDict(
            [(k, pb_options[k].default) for k in sorted(pb_options.keys())])

        if isinstance(output_preset_xml, str):
            write_task_options_to_preset_xml_and_print(task_options_d, output_preset_xml, warn_msg)

        if isinstance(output_preset_json, str):
            write_presets_json_and_print(pipeline, task_options_d, output_preset_json, warn_msg)

        if pb_options:
            _print_pacbio_options([pb_options[k] for k in sorted(pb_options.keys())])
        else:
            print "No default task options"

    else:
        msg = "Unable to find template id '{t}' in registered pipelines. Use the show-templates option to get a list of workflow options.".format(t=template_id)
        log.error(msg)
        print msg

    return 0


def _args_run_show_template_details(args):
    return run_show_template_details(args.template_id, args.output_preset_xml, args.output_preset_json)


def __dynamically_load_all():
    """ Load the registered tasks and operators

    """
    import pbsmrtpipe.loader as L

    def _f(x):
        """length or None"""
        return "None" if x is None else len(x)

    rtasks, rfile_types, roperators, rpipelines = L.load_all()
    _d = dict(n=_f(rtasks), f=_f(rfile_types), o=_f(roperators), p=_f(rpipelines))
    print "Registry Loaded. Number of ToolContracts:{n} FileTypes:{f} ChunkOperators:{o} Pipelines:{p}".format(**_d)
    return rtasks, rfile_types, roperators, rpipelines


def run_show_tasks():

    r_tasks, _, _, _ = __dynamically_load_all()

    sorted_tasks = sorted(r_tasks.values(), key=lambda x: x.task_id)
    max_id = max(len(t.task_id) for t in sorted_tasks)
    pad = 4
    offset = max_id + pad
    print "Registered ToolContracts ({n})".format(n=len(sorted_tasks))
    print

    def _to_a(klass):
        d = {MetaTask: "", MetaScatterTask: "(scatter)", MetaGatherTask: "(gather)"}
        return d.get(klass, "")

    for i, t in enumerate(sorted_tasks):
        print " ".join([(str(i + 1) + ".").rjust(4), t.task_id.ljust(offset), _to_a(t) + t.display_name])

    return 0


def _args_run_show_tasks(args):
    return run_show_tasks()


def _print_option_schemas(option_schemas_d):
    """This is the legacy JSONSchema-ish supported model

    .. note: This will be deprecated.
    """

    def _get_v(oid, s, name):
        return s['properties'][oid][name]

    print "Number of Options {n}".format(n=len(option_schemas_d))
    if option_schemas_d:
        n = 0
        for opt_id, schema in option_schemas_d.iteritems():
            print "Option #{n} Id: {i}".format(n=n, i=opt_id)
            print "\tDefault     : ", _get_v(opt_id, schema, 'default')
            print "\tType        : ", _get_v(opt_id, schema, 'type')
            print "\tDescription : ", _get_v(opt_id, schema, 'description')
            n += 1
            print


def _print_pacbio_options(pacbio_options):
    """
    :type pacbio_options: pbcommand.models.BasePacBioOption
    :return:
    """
    pad = 15

    def to_s(name, value):
        return name.rjust(pad) + " : {}".format(value)

    def printer(name, value):
        print to_s(name, value)

    print "Number of Options {n}".format(n=len(pacbio_options))
    if pacbio_options:
        for i, pb_option in enumerate(pacbio_options):
            n = i + 1
            printer("Option #{n} Id".format(n=n), pb_option.option_id)
            printer("Type Id", pb_option.OPTION_TYPE_ID)
            printer("Default", pb_option.default)
            if isinstance(pb_option, BaseChoiceType):
                printer("Choices", pb_option.choices)
            printer("Display Name", pb_option.name)
            printer("Description", pb_option.description)
            print


def run_show_task_details(task_id):

    r_tasks, _, _, _ = __dynamically_load_all()

    meta_task = r_tasks.get(task_id, None)

    sep = "*" * 20

    if meta_task is None:
        raise KeyError("Unable to find Task id '{i}' Use 'show-tasks' option to list available task ids.".format(i=task_id))
    else:
        print sep
        print meta_task.summary()
        print "Option type for MetaTask", type(meta_task.option_schemas)
        _print_pacbio_options(meta_task.option_schemas)

    return 0


def _args_run_show_task_details(args):
    rcode = run_show_task_details(args.task_id)

    output_file = args.output_preset_xml

    if output_file is not None:

        rtasks, _, _, _ = __dynamically_load_all()

        meta_task = rtasks[args.task_id]

        if isinstance(meta_task.option_schemas, dict):
            opts = {o.option_id: o for o in meta_task.option_schemas}
        else:
            raise TypeError("Malformed task {t}".format(t=meta_task))

        write_task_options_to_preset_xml_and_print(opts, output_file, "WARNING. Task {i} has no task options. NO preset was wrote to a preset.xml file.".format(i=args.task_id))

    return rcode


def _cli_entry_point_args_to_dict(args_entry_points):
    """Translates the entry points from argparse style to the a dict of

    {e_0:/path/to/f1.txt, e_1:/path/to/f2.txt}

    argparse is kinda stupid, or I don't know how to use the API
    entry_points=[[('entry_idX', 'docs/index.rst')], [('entry_2', 'docs/wf_example.py')]]
    """
    ep_d = {}
    for elist in args_entry_points:
        for k, v in elist:
            if k in ep_d:
                log.info(pprint.pformat(args_entry_points))
                raise ValueError("entry point id '{i}' was given multiple times ".format(i=k))
            ep_d[k] = v
    return ep_d


def _args_run_pipeline(args):

    if args.debug:
        slog.debug(args)

    ep_d = _cli_entry_point_args_to_dict(args.entry_points)

    registered_tasks_d, registered_files_d, chunk_operators, pipelines_d = __dynamically_load_all()

    force_distribute, force_chunk = resolve_dist_chunk_overrides(args)

    # Validate all preset files exist
    preset_xmls = [os.path.abspath(os.path.expandvars(p)) for p in args.preset_xml]
    preset_jsons = [os.path.abspath(os.path.expandvars(p)) for p in args.preset_json]
    return D.run_pipeline(pipelines_d, registered_files_d, registered_tasks_d, chunk_operators,
                          args.pipeline_template_xml,
                          ep_d, args.output_dir, preset_jsons, preset_xmls, args.preset_rc_xml, args.service_uri,
                          force_distribute=force_distribute, force_chunk_mode=force_chunk, debug_mode=args.debug)


def _validate_entry_id(e):
    m = RX_ENTRY.match(e)
    if m is None:
        msg = "Entry point '{e}' should match pattern {p}".format(e=e, p=RX_ENTRY.pattern)
        raise MalformedEntryStrError(msg)
    else:
        return m.groups()[0]


def _validate_entry(e):
    """
    Validate that entry has the CLI form "entry_id:/path/to/file.txt"

    :raises ValueError, IOError
    """
    if ":" in e:
        x = e.split(":")
        if len(x) == 2:
            entry_id, path = x[0].strip(), x[1].strip()
            px = os.path.abspath(os.path.expanduser(path))
            if os.path.isfile(px):
                return entry_id, px
            else:
                raise IOError("Unable to find path '{p}' for entry id '{i}'".format(p=px, i=entry_id))

    raise ValueError("Invalid entry id '{e}' format. Expected (' -e 'entry_idX:/path/to/file.txt')".format(e=e))


def __validate_json_file(path):
    with open(path) as f:
        _ = json.loads(f.read())
    return path


def _validate_uri(value):
    # little bit of sanity testing
    u = urlparse.urlparse(value)
    msg = "Invalid or unsupported service URI '{i}".format(i=value)
    if u.scheme not in ("http", "https"):
        raise ValueError(msg)
    return value


def _add_webservice_config(p):
    p.add_argument('--service-uri', type=_validate_uri, default=None,
                   help="Remote Webservice URI to send status updates to.")
    return p


def __add_pipeline_parser_options(p):
    """Common options for all running pipelines or tasks"""
    funcs = [TU.add_override_chunked_mode,
             TU.add_override_distribute_option,
             _add_webservice_config,
             _add_rc_preset_xml_option,
             _add_preset_json_option,
             _add_preset_xml_option,
             _add_output_dir_option,
             _add_entry_point_option,
             add_log_debug_option]

    f = compose(*funcs)
    return f(p)


def add_pipline_parser_options(p):
    p.add_argument('pipeline_template_xml', type=validate_file,
                   help="Path to pipeline template XML file.")
    p = __add_pipeline_parser_options(p)
    return p


def add_pipeline_id_parser_options(p):
    p.add_argument('pipeline_id', type=str,
                   help="Registered pipeline id (run show-templates) to show a list of the registered pipelines.")
    p = __add_pipeline_parser_options(p)
    return p


def add_show_template_details_parser_options(p):
    p = _add_template_id_option(p)
    p = _add_output_preset_xml_option(p)
    return p


def _add_entry_point_single_task_option(p):
    m = "Entry Points should be defined as e_{x}:/path/to/file for " \
        "each positional index of the Task. Example, for a task with 2 inputs, -e e_0:/path/to/f1.txt -e e_1:/path/to/f2.txt"
    return _add_entry_point_option(p, help_msg=m)


def add_task_parser_options(p):

    funcs = [
        TU.add_override_chunked_mode,
        TU.add_override_distribute_option,
        _add_webservice_config,
        _add_rc_preset_xml_option,
        _add_preset_xml_option,
        _add_preset_json_option,
        _add_output_dir_option,
        _add_entry_point_single_task_option,
        _add_task_id_run_option,
        add_log_debug_option]

    f = compose(*funcs)
    return f(p)


def resolve_dist_chunk_overrides(args):
    # Assumes add_override_chunk_mode and add_override_distribute_mode options
    # were added
    force_distribute = None
    if args.force_distributed is True:
        force_distribute = True
    if args.local_only is True:
        force_distribute = False

    force_chunk = None
    if args.force_chunk_mode is True:
        force_chunk = True
    if args.disable_chunk_mode is True:
        force_chunk = False

    return force_distribute, force_chunk


def _args_task_runner(args):
    if args.debug:
        log.info(args)

    registered_tasks, registered_file_types, chunk_operators, pipelines = __dynamically_load_all()

    # This will return a dict of {e_ix:/path/to/f1.txt, e_iy:/path/to/f1.txt}
    ep_d = _cli_entry_point_args_to_dict(args.entry_points)

    force_distribute, force_chunk = resolve_dist_chunk_overrides(args)
    preset_xmls = [os.path.abspath(os.path.expandvars(p)) for p in args.preset_xml]
    preset_jsons = [os.path.abspath(os.path.expandvars(p)) for p in args.preset_json]
    return D.run_single_task(registered_file_types, registered_tasks, chunk_operators,
                             ep_d, args.task_id, args.output_dir, preset_jsons,
                             preset_xmls, args.preset_rc_xml, args.service_uri,
                             force_distribute=force_distribute,
                             force_chunk_mode=force_chunk, debug_mode=args.debug)


def _args_run_show_workflow_level_options(args):

    from pbsmrtpipe.pb_io import REGISTERED_WORKFLOW_OPTIONS

    _print_option_schemas(REGISTERED_WORKFLOW_OPTIONS)

    if args.output_preset_xml is not None:
        xml = IO.schema_workflow_options_to_xml(REGISTERED_WORKFLOW_OPTIONS)
        with open(args.output_preset_xml, 'w') as w:
            w.write(str(xml))
        log.info("wrote options to {x}".format(x=args.output_preset_xml))

    if args.output_preset_json is not None:
        IO.write_workflow_presets_json(REGISTERED_WORKFLOW_OPTIONS, args.output_preset_json)
        log.info("wrote options to {x}".format(x=args.output_preset_json))

    return 0


def add_show_task_options(p):
    p = _add_task_id_option(p)
    p = _add_output_preset_xml_option(p)
    return p


def _args_run_pipeline_id(args):

    registered_tasks_d, registered_files_d, chunk_operators, pipelines = __dynamically_load_all()

    if args.pipeline_id not in pipelines:
        raise ValueError("Unable to find pipeline id '{i}'".format(i=args.pipeline_id))

    pipeline = pipelines[args.pipeline_id]

    if args.debug:
        slog.debug(args)

    ep_d = _cli_entry_point_args_to_dict(args.entry_points)

    force_distribute, force_chunk = resolve_dist_chunk_overrides(args)
    preset_xmls = [os.path.abspath(os.path.expandvars(p)) for p in args.preset_xml]
    preset_jsons = [os.path.abspath(os.path.expandvars(p)) for p in args.preset_json]
    return D.run_pipeline(pipelines, registered_files_d,
                          registered_tasks_d,
                          chunk_operators,
                          pipeline,
                          ep_d, args.output_dir, preset_jsons, preset_xmls,
                          args.preset_rc_xml, args.service_uri,
                          force_distribute=force_distribute,
                          force_chunk_mode=force_chunk)


def _args_run_diagnostics(args):
    f = run_diagnostics
    if args.simple:
        f = run_simple_diagnostics

    precord = IO.parse_pipeline_preset_xml(args.preset_xml)
    wopts = precord.to_workflow_level_opt()

    if wopts.cluster_manager_path is not None and wopts.distributed_mode is True:
        output_dir = os.path.abspath(args.output_dir)
        return f(args.preset_xml, output_dir)
    else:
        log.warning("Cluster mode not enabled. Skipping cluster submission tests")
        return 0


def _args_show_chunk_operator_summary(args):
    import pbsmrtpipe.loader as L
    chunk_operators = L.load_all_installed_chunk_operators()

    if chunk_operators:
        for i, xs in enumerate(chunk_operators.iteritems()):
            op_id, chunk_operator = xs
            print "{i}. Chunk Operator Id: {o}".format(o=op_id, i=i)
            print "  Scatter :"
            print "    Scatter Task: {i} -> Chunk Task: {t}".format(i=chunk_operator.scatter.task_id, t=chunk_operator.scatter.scatter_task_id)
            for si, c in enumerate(chunk_operator.scatter.chunks):
                print "    {i} Task Binding: {t} -> Chunk Key: {k}".format(t=c.task_input, k=c.chunk_key, i=si)
            print "  Gather :"
            for ci, c in enumerate(chunk_operator.gather.chunks):
                print "    {i} Gather Task: {t} ".format(t=c.gather_task_id, i=ci)
                print "      Task Binding: {i} -> Chunk Key: {k} ".format(i=c.task_input, k=c.chunk_key)
    else:
        print "WARNING. No Chunk operators loaded"

    return 0


def _add_required_preset_xml_option(p):
    p.add_argument('preset_xml', type=validate_file, help="Path to Preset XML file.")
    return p


def _add_simple_mode_option(p):
    # Run the Full diagnostics suite
    p.add_argument('--simple', action='store_true',
                   help="Perform full diagnostics tests (e.g., submit test job to cluster).")
    return p


def add_args_run_diagnstic(p):
    _add_required_preset_xml_option(p)
    add_log_debug_option(p)
    _add_output_dir_option(p)
    _add_simple_mode_option(p)
    return p


def get_parser():
    desc = "Pbsmrtpipe workflow engine"
    p = get_default_argparser(pbsmrtpipe.get_version(), desc)

    sp = p.add_subparsers(help='commands')

    def builder(subparser_id, description, options_func, exe_func):
        subparser_builder(sp, subparser_id, description, options_func, exe_func)

    wf_desc = "Run a pipeline using a pipeline template or with explict Bindings and EntryPoints."
    builder('pipeline', wf_desc, add_pipline_parser_options, _args_run_pipeline)

    # Run a pipeline by id
    pipline_id_desc = "Run a registered pipeline by specifying the pipeline id."
    builder('pipeline-id', pipline_id_desc, add_pipeline_id_parser_options, _args_run_pipeline_id)

    builder('task', "Run Task (i.e., ToolContract) by id", add_task_parser_options, _args_task_runner)

    # Show Templates
    desc = "List all pipeline templates. A pipeline 'id' can be referenced in " \
           "your my_pipeline.xml file using '<import-template id=\"pbsmrtpipe.pipelines.my_pipeline_id\" />. This " \
           "can replace the explicit listing of EntryPoints and Bindings."

    builder('show-templates', desc, add_run_show_templates_options, _args_run_show_templates)

    # Show Template Details
    builder('show-template-details', "Show details about a specific Pipeline template.", add_show_template_details_parser_options, _args_run_show_template_details)

    # Show Tasks
    show_tasks_desc = "Show completed list of Tasks by id. Use ENV {x} to define a " \
                      "custom directory of tool contracts. These TCs will override " \
                      "the installed TCs (e.g., {x}=/path/to/my-tc-dir/)".format(x=ENV_TC_DIR)
    builder('show-tasks', show_tasks_desc, lambda x: x, _args_run_show_tasks)

    # Show Task id details
    desc_task_details = "Show Details of a particular task by id (e.g., 'pbsmrtpipe.tasks.filter_report'). Use 'show-tasks' to get a completed list of registered tasks."
    builder('show-task-details', desc_task_details, add_show_task_options, _args_run_show_task_details)

    wfo_desc = "Display all workflow level options that can be set in <options /> for preset.xml"
    builder('show-workflow-options', wfo_desc, _add_output_preset_xml_option, _args_run_show_workflow_level_options)

    diag_desc = "Diagnostic tests of preset.xml and cluster configuration"
    builder('run-diagnostic', diag_desc, add_args_run_diagnstic, _args_run_diagnostics)

    desc_chunk_op_show = "Show a list of loaded chunk operators for Scatter/Gather Tasks. Extend resource loading by exporting ENV var {i}. Example export {i}=/path/to/chunk-operators-xml-dir".format(i=ENV_CHK_OPT_DIR)
    builder('show-chunk-operators', desc_chunk_op_show, lambda x: x, _args_show_chunk_operator_summary)

    return p


def _pbsmrtipe_setup_log(alog, **kwargs):
    """Setup stdout log. pbsmrtpipe will setup pbsmrtpipe.log, master.log

    This should only emit 'status.*' messages.
    """
    # This is a essentially just a bootstrapping step before the job-dir/logs
    # can be created and proper log files (pbsmrtpipe.log, master.log) will
    # be setup for this to work with the new global dict setup model would have to
    # extended to support adding a custom filter.

    str_formatter = '%(message)s'

    level = kwargs.get('level', logging.INFO)
    setup_log(alog,
              level=level,
              file_name=None,
              log_filter=StdOutStatusLogFilter(),
              str_formatter=str_formatter)

    slog.info("Starting pbsmrtpipe v{v}".format(v=pbsmrtpipe.get_version()))


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_parser()

    return main_runner(argv_[1:], parser, args_executer, _pbsmrtipe_setup_log, log)
