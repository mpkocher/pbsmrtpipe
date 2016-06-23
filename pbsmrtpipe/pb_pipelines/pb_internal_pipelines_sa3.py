import logging


from pbsmrtpipe.core import register_pipeline
from pbsmrtpipe.constants import to_pipeline_ns

from .pb_pipelines_sa3 import Constants, Tags

log = logging.getLogger(__name__)


def register(relative_id, display_name, tags=(), task_options=None):
    pipeline_id = to_pipeline_ns(relative_id)
    ptags = list(set(tags + (Tags.INTERNAL, Tags.COND)))
    return register_pipeline(pipeline_id, display_name, "0.2.0", tags=ptags, task_options=task_options)


@register("internal_cond_dev", "Internal Condition JSON Dev Test", tags=(Tags.DEV, ))
def to_bs():
    """Hello World test for Conditions JSON"""
    b1 = [(Constants.ENTRY_COND_JSON, "pbinternal2.tasks.cond_to_report:0")]

    return b1


@register("internal_cond_dev2", "Internal Condition JSON Dev Test 2", tags=(Tags.DEV, ))
def to_bs():
    """Dev Test for AlignmentSet Condition Summary"""
    b1 = [(Constants.ENTRY_COND_JSON, "pbinternal2.tasks.cond_to_report:0")]

    b2 = [(Constants.ENTRY_COND_JSON, "pbinternal2.tasks.cond_to_alignmentsets_report:0")]

    return b1 + b2


@register("internal_cond_dev_r", "Internal Condition Dev R (hello world)", tags=(Tags.DEV, ))
def to_bs():
    """Hello World for R + Reports"""
    # Call the Python cond report for dev/testing purposes
    b1 = [(Constants.ENTRY_COND_JSON, "pbinternal2.tasks.cond_to_report:0")]

    # RRRRRRRR. This tasks should be renamed
    b2 = [(Constants.ENTRY_COND_JSON, "pbcommandR.tasks.hello_reseq_condition:0")]

    return b1 + b2


@register("internal_cond_dev_r_reports", "Internal Condition Dev R (hello world + Simple Condition Summary Report)", tags=(Tags.DEV, ))
def to_bs():
    """Hello World for R"""
    # Call the Python cond report for dev/testing purposes
    b1 = [(Constants.ENTRY_COND_JSON, "pbinternal2.tasks.cond_to_report:0")]

    # R tasks
    b2 = [(Constants.ENTRY_COND_JSON, "pbcommandR.tasks.hello_reseq_condition:0")]

    b3 = [(Constants.ENTRY_COND_JSON, "pbcommandR.tasks.hello_reseq_condition_report:0")]

    return b1 + b2 + b3


@register("internal_cond_acc_density", "Internal Condition Accuracy Density Plots using R")
def to_bs():
    """Accuracy Density Plots"""
    b1 = [(Constants.ENTRY_COND_JSON, "pbinternal2.tasks.cond_to_report:0")]

    b2 = [(Constants.ENTRY_COND_JSON, "pbcommandR.tasks.accplot_reseq_condition:0")]

    return b1 + b2


@register("mh_poc", "Example Condition Pipeline for Accuracy and Readlength comparison")
def to_bs():
    """Accuracy Density Plots"""
    b1 = [(Constants.ENTRY_COND_JSON, "pbinternal2.tasks.cond_to_report:0")]

    b2 = [(Constants.ENTRY_COND_JSON, "pbsmrtpipe_examples.tasks.dev_mh_toy:0")]

    return b1 + b2
