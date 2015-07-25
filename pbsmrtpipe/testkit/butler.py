import ConfigParser
import logging
import functools
import abc
import os

log = logging.getLogger(__name__)

EXE = "pbsmrtpipe"

__all__ = ['ButlerTask',
           'ButlerWorkflow',
           'config_parser_to_butler']

__author__ = "Michael Kocher"


class TestkitCfgParserError(ValueError):
    pass


class Constants(object):

    """Allowed values in cfg file."""
    CFG_TASK = 'pbsmrtpipe:task'
    CFG_WORKFLOW = 'pbsmrtpipe:pipeline'

    CFG_JOB_ID = "id"

    CFG_ENTRY_POINTS = 'entry_points'

    CFG_PREFIX_XML = 'preset_xml'
    CFG_WORKFLOW_XML = 'pipeline_xml'
    CFG_TASK_ID = 'task_id'

    CFG_OUTPUT_DIR = 'output_dir'

    CFG_DEBUG = 'debug'
    CFG_MOCK = 'mock'


class Butler(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, job_id, output_dir, entry_points, preset_xml, debug, force_distribute=False):
        self.output_dir = output_dir
        self.entry_points = entry_points
        self.preset_xml = preset_xml
        self.debug_mode = debug
        self.force_distribute = force_distribute
        # this needs to be set in the Butler.cfg file.
        self.job_id = job_id

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, p=self.prefix)
        return "<{k} {p} >".format(**_d)

    @abc.abstractproperty
    def prefix(self):
        # Used in the repr
        return ""

    def to_cmd(self):
        return _to_pbsmrtpipe_cmd(self.prefix, self.output_dir,
                                  self.entry_points, self.preset_xml,
                                  self.debug_mode, self.force_distribute)


class ButlerWorkflow(Butler):

    def __init__(self, job_id, output_dir, workflow_xml, entry_points, preset_xml_path, debug, force_distribute=False):
        super(ButlerWorkflow, self).__init__(job_id, output_dir, entry_points, preset_xml_path, debug, force_distribute=force_distribute)
        self.workflow_xml = workflow_xml

    @property
    def prefix(self):
        return "pipeline {i}".format(i=self.workflow_xml)


class ButlerTask(Butler):

    def __init__(self, job_id, output_dir, task_id, entry_points, preset_xml, debug, force_distribute=False):
        super(ButlerTask, self).__init__(job_id, output_dir, entry_points, preset_xml, debug, force_distribute=force_distribute)
        self.task_id = task_id

    @property
    def prefix(self):
        return "task {i}".format(i=self.task_id)


def _to_pbsmrtpipe_cmd(prefix_mode, output_dir, entry_points_d, preset_xml, debug, force_distribute):
    ep_str = " ".join([' -e ' + ":".join([k, v]) for k, v in entry_points_d.iteritems()])
    d_str = '--debug' if debug else " "
    p_str = " " if preset_xml is None else "--preset-xml={p}".format(p=preset_xml)
    m_str = ' '
    force_dist_str = "--force-distribute" if force_distribute else ""
    _d = dict(x=EXE, e=ep_str, d=d_str, p=p_str, m=prefix_mode, o=output_dir, k=m_str, f=force_dist_str)
    cmd = "{x} {m} {d} {e} {p} {k} {f} --output-dir={o}"
    return cmd.format(**_d)


to_task_cmd = functools.partial(_to_pbsmrtpipe_cmd, 'task')
to_workflow_cmd = functools.partial(_to_pbsmrtpipe_cmd, 'pipeline')


def _parse_or_default(section, key, p, default):
    if p.has_option(section, key):
        return p.get(section, key)
    return default


def _parse_preset_xml(section_name, p, base_dir):
    v = _parse_or_default(section_name, Constants.CFG_PREFIX_XML, p, None)
    if v is None:
        return None
    else:
        p = v if os.path.isabs(v) else os.path.join(base_dir, v)
        if os.path.exists(p):
            return p
        else:
            raise IOError("Unable to find preset XML '{p}'".format(p=p))


def _parse_debug_mode(section_name, p):
    return bool(_parse_or_default(section_name, Constants.CFG_DEBUG, p, False))


def _parse_entry_points(p, root_dir_name):
    """

    Files may be defined relative to the butler.cfg file or absolute paths

    """
    ep_d = {}
    ep_keys = p.options(Constants.CFG_ENTRY_POINTS)

    for ep_key in ep_keys:
        v = p.get(Constants.CFG_ENTRY_POINTS, ep_key)
        if not os.path.isabs(v):
            v = os.path.join(root_dir_name, v)

        ep_d[ep_key] = os.path.abspath(v)

    return ep_d


def _parse_entry_points_and_preset_xml(section_name, p, root_dir):
    return _parse_entry_points(p, root_dir), _parse_preset_xml(section_name, p, root_dir)


def _to_parse_workflow_config(job_output_dir, base_dir):
    """

    :param job_output_dir: Job output directory
    :param base_dir:  base directory of the butler.cfg file
    :return:
    """
    def _parse_workflow_config(p):
        ep_d, preset_xml = _parse_entry_points_and_preset_xml(Constants.CFG_WORKFLOW, p, base_dir)
        x = p.get(Constants.CFG_WORKFLOW, Constants.CFG_WORKFLOW_XML)

        if not os.path.isabs(x):
            x = os.path.join(base_dir, x)

        if not os.path.exists(x):
            raise IOError("Unable to find pipeline XML '{x}'".format(x=x))

        d = _parse_debug_mode(Constants.CFG_WORKFLOW, p)
        workflow_xml = os.path.abspath(x)

        # FIXME. This should be defined in cfg file.
        default_job_id = os.path.basename(base_dir)
        job_id = _parse_or_default(Constants.CFG_WORKFLOW, Constants.CFG_JOB_ID, p, default_job_id)

        return ButlerWorkflow(job_id, job_output_dir, workflow_xml, ep_d, preset_xml, d)

    return _parse_workflow_config


def _to_parse_task_config(output_dir, base_dir):
    def _parse_task_config(p):
        # FIXME. This should be defined in cfg file.
        default_job_id = os.path.basename(base_dir)
        ep_d, preset_xml = _parse_entry_points_and_preset_xml(Constants.CFG_TASK, p, base_dir)
        job_id = _parse_or_default(Constants.CFG_TASK, Constants.CFG_JOB_ID, p, default_job_id)
        task_id = p.get(Constants.CFG_TASK, Constants.CFG_TASK_ID)
        d = _parse_debug_mode(Constants.CFG_TASK, p)
        b = ButlerTask(job_id, output_dir, task_id, ep_d, preset_xml, d, force_distribute=False)

        return b

    return _parse_task_config


def config_parser_to_butler(file_path):
    """

    :param file_path: path to butler config file
    :return: Butler instance

    :rtype: Butler
    """
    # this is weak. Needs error handling.

    p = ConfigParser.ConfigParser()
    _ = p.read(file_path)

    # paths within the config file can be relative the butler.cfg file

    base_dir = os.path.dirname(file_path)

    # pbsmrtpipe will make the directory if it doesn't exist
    default_output_dir = os.path.join(base_dir, 'job_output')
    output_dir = _parse_or_default(Constants.CFG_WORKFLOW, Constants.CFG_OUTPUT_DIR, p, default_output_dir)
    output_dir = os.path.abspath(output_dir)

    if p.has_section(Constants.CFG_WORKFLOW):
        func = _to_parse_workflow_config(output_dir, base_dir)
    elif p.has_section(Constants.CFG_TASK):
        func = _to_parse_task_config(output_dir, base_dir)
    else:
        _d = dict(x=Constants.CFG_WORKFLOW, y=Constants.CFG_TASK, f=file_path)
        raise TestkitCfgParserError("Expected section {x} or {y} in {f}".format(**_d))

    butler = func(p)

    return butler
