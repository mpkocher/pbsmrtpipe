"""Custom Pipelines that are used for testing"""
import logging
import os
import sys

import pbsmrtpipe.loader as L

from pbcommand.cli import get_default_argparser, pacbio_args_runner
from pbcommand.utils import setup_log

from pbsmrtpipe.core import PipelineRegistry
from pbsmrtpipe.pb_io import write_pipeline_templates_to_avro

from .pb_pipelines_sa3 import Tags

log = logging.getLogger(__name__)


class Constants(object):
    PT_NAMESPACE = "pbsmrtpipe"


registry = PipelineRegistry(Constants.PT_NAMESPACE)


def _example_topts():
    return {"pbsmrtpipe.task_options.dev_message": "Preset Custom Dev Message from register pipeline"}


@registry("dev_a", "Example 01", "0.1.0", tags=(Tags.DEV, "hello-world"), task_options=_example_topts())
def to_bs():
    """Custom Pipeline Registry for dev hello world tasks"""
    b1 = [('$entry:e_01', 'pbsmrtpipe.tasks.dev_hello_world:0')]

    b2 = [('pbsmrtpipe.tasks.dev_hello_world:0', 'pbsmrtpipe.tasks.dev_hello_worlder:0'),
          ('pbsmrtpipe.tasks.dev_hello_world:0', 'pbsmrtpipe.tasks.dev_hello_garfield:0')]

    b3 = [('pbsmrtpipe.tasks.dev_hello_world:0', 'pbsmrtpipe.tasks.dev_txt_to_fasta:0')]

    return b1 + b2 + b3


@registry("dev_b", "Example 02", "0.1.0", tags=(Tags.RPT, Tags.DEV))
def to_bs():
    b3 = [("pbsmrtpipe.pipelines.dev_a:pbsmrtpipe.tasks.dev_txt_to_fasta:0", 'pbsmrtpipe.tasks.dev_filter_fasta:0')]
    return b3


def registry_runner(registry_, rtasks, output_dir):
    # this will emit the PTs to an output dir
    d = os.path.abspath(os.path.expanduser(output_dir))
    r = registry_
    log.info("Writing Pipeline Templates to ")
    print "Emitting pipelines to output dir {d}".format(d=d)
    write_pipeline_templates_to_avro(r.pipelines.values(), rtasks, d)
    log.info("Successful wrote {n} pipelines to {d}".format(n=len(r.pipelines), d=d))
    return 0


def get_parser():
    desc = "Custom PipelineTemplate Registry to write pipeline templates to output directory"
    p = get_default_argparser("0.1.0", desc)
    p.add_argument('output_dir', help="Path to output directory")
    return p


def args_runner(args):
    rtasks = L.load_all_tool_contracts()
    return registry_runner(registry, rtasks, args.output_dir)


def main(argv=sys.argv):
    return pacbio_args_runner(argv[1:], get_parser(), args_runner, log, setup_log)


if __name__ == '__main__':
    sys.exit(main())
