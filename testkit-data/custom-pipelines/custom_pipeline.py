#!/usr/bin/env python
"""
Example for defining Custom Pipelines
using pipelines to emit a Pipeline XML or ResolvedPipeline Template JSON file
"""
import logging
import sys

from pbsmrtpipe.loader import load_all_installed_pipelines
from pbsmrtpipe.core import PipelineRegistry
from pbsmrtpipe.cli_custom_pipeline import registry_runner_main

log = logging.getLogger(__name__)


class Constants(object):
    PT_NAMESPACE = "pbsmrtpipe_examples"
    TAGS_DEV = "dev"

# MK. Need to take another pass at this.
loaded_pipelines = load_all_installed_pipelines().values()
registry = PipelineRegistry(Constants.PT_NAMESPACE, pipelines=loaded_pipelines)


def _example_topts():
    return {"pbsmrtpipe_examples.task_options.dev_message": "Preset Custom Dev Message from register pipeline",
            "pbsmrtpipe_examples.task_options.custom_alpha": 12345}


@registry("dev_a", "Example 01", "0.1.0", tags=("dev", "hello-world"), task_options=_example_topts())
def to_bs():
    """Custom Pipeline Registry for dev hello world tasks"""
    b1 = [('$entry:e_01', 'pbsmrtpipe.tasks.dev_hello_world:0')]

    b2 = [('pbsmrtpipe.tasks.dev_hello_world:0', 'pbsmrtpipe.tasks.dev_hello_worlder:0'),
          ('pbsmrtpipe.tasks.dev_hello_world:0', 'pbsmrtpipe.tasks.dev_hello_garfield:0')]

    b3 = [('pbsmrtpipe.tasks.dev_hello_world:0', 'pbsmrtpipe.tasks.dev_txt_to_fasta:0')]

    return b1 + b2 + b3


@registry("dev_b", "Example 02", "0.1.0", tags=("dev",), task_options=_example_topts())
def to_bs():
    """Custom Pipeline B for testing"""
    # Note this is using a custom Namespace and the pipeline should be referenced
    # by this namespace.
    b3 = [("pbsmrtpipe_examples.pipelines.dev_a:pbsmrtpipe.tasks.dev_txt_to_fasta:0", 'pbsmrtpipe.tasks.dev_filter_fasta:0')]
    return b3


@registry("dev_c", "Example 03", "0.1.0", tags=("dev",))
def to_bs():
    """Example Registry Pipeline that loads existing pipelines"""
    return [("pbsmrtpipe.pipelines.dev_04:pbsmrtpipe.tasks.dev_hello_world:0", "pbsmrtpipe.tasks.dev_txt_to_fasta:0")]


if __name__ == '__main__':
    sys.exit(registry_runner_main(registry)(argv=sys.argv))
