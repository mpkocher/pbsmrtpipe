import logging


from pbsmrtpipe.core import register_pipeline
from pbsmrtpipe.constants import to_pipeline_ns

from .pb_pipelines_sa3 import Constants, Tags

log = logging.getLogger(__name__)


def register(relative_id, display_name, tags=(), task_options=None):
    pipeline_id = to_pipeline_ns(relative_id)
    ptags = list(set(tags + (Tags.INTERNAL, Tags.COND )))
    return register_pipeline(pipeline_id, display_name, "0.1.0", tags=ptags, task_options=task_options)


@register("internal_cond_dev", "Internal Condition JSON Dev Test")
def to_bs():
    """Hello World test for Conditions JSON"""
    b1 = [(Constants.ENTRY_COND_JSON, "pbinternal2.tasks.cond_to_report:0")]

    return b1