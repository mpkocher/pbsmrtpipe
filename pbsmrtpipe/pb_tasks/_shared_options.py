import logging

import jsonschema

import pbsmrtpipe.schema_opt_utils as U
from pbsmrtpipe.constants import to_task_option_ns

log = logging.getLogger(__name__)

# Globally accessible task options
GLOBAL_TASK_OPTIONS = {}


def register_global_task_option(option_id):

    def _(func):

        def _(*args, **kwargs):
            return args, kwargs

        schema = func()

        jsonschema.Draft4Validator(schema)

        # need to validate that it's a valid schema
        if option_id in GLOBAL_TASK_OPTIONS:
            raise KeyError("global task option {i} already registered.".format(i=option_id))
        else:
            GLOBAL_TASK_OPTIONS[option_id] = schema

    return _


@register_global_task_option(to_task_option_ns("use_subreads"))
def f():
    """

    :return: jsonschema
    """
    d = U.to_option_schema(to_task_option_ns("use_subreads"),
                           "boolean",
                           "Use Subreads",
                           "Enable the Use Subreads mode", True)
    return d


@register_global_task_option(to_task_option_ns("noise.data"))
def f():
    d = U.to_option_schema(to_task_option_ns("noise.data"),
                           "string",
                           "Noise Data",
                           "Description of Noise Data",
                           "-77.27,0.08654,0.00121")
    return d


@register_global_task_option(to_task_option_ns('num_stats_regions'))
def f():
    d = U.to_option_schema(to_task_option_ns("num_stats_regions"),
                           "integer",
                           "Number Stats Regions",
                           "Description of Number Stats Regions",
                           500)
    return d


@register_global_task_option(to_task_option_ns('global_debug_mode'))
def f():
    """This means that at the task level debug is enabled"""
    d = U.to_option_schema(to_task_option_ns("global_debug_mode"),
                           "boolean",
                           "Global Task Debug mode",
                           "Task level debug mode", False)
    return d
