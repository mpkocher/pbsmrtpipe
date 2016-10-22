import os
import os.path as op
import sys
import logging
import json
import re

from pbcommand.utils import setup_log
from pbcommand.cli import pacbio_args_runner, get_default_argparser_with_base_opts
from pbcommand.validators import validate_dir
import pbsmrtpipe.loader as L

log = logging.getLogger(__name__)
slog = logging.getLogger('status.' + __name__)

__version__ = "0.2.0"


INDEX_RST_TEMPLATE = """

PacBio Pipeline Docs
====================

Contents:

.. toctree::
    :maxdepth: 1

"""

PIPELINE_DETAIL_TEMPLATE = """
Pipeline {n}
------------

:id: {i}
:name: {n}
:version: {v}
:tags: {g}
:description: {d}

EntryPoints
^^^^^^^^^^^

{e}

Task Options ({t})
^^^^^^^^^^^^^^^^^^
"""

ENTRY_POINT_TEMPLATE = """
::

    :id: {i}
    :name: {n}
    :fileTypeId: {f}

"""


def make_rst_table(rows, headers=None):
    """
    Construct RST syntax for a generic table.
    """
    _rows = list(rows)
    if headers is not None:
        assert len(headers) == len(rows[0])
        _rows.append(headers)
    widths = [max([len(_rows[j][i]) for j in range(len(_rows))])
              for i in range(len(_rows[0]))]
    format_str = "| " + \
        " | ".join(["%-{:d}s".format(x) for x in widths]) + " |"
    sep_str = "+" + "+".join(["-" * (x + 2) for x in widths]) + "+"
    table = [sep_str]
    if headers is not None:
        table.append(format_str % tuple(headers))
        table.append(re.sub("-", "=", sep_str))
    for row in rows:
        table.append(format_str % tuple(row))
        table.append(sep_str)
    return "\n".join(table)


def load_pipelines_from_dir(dir_name):
    """
    :arg path: Path to pipeline template dir
    :type path: basestring
    :rtype path: list[Pipeline]
    """
    pipelines = []
    if os.path.exists(dir_name):
        for file_name in os.listdir(dir_name):
            if file_name.endswith(".json"):
                try:
                    pipelines.append(json.load(open(os.path.join(dir_name, file_name))))
                except Exception as e:
                    log.warn("Unable to load Resolved Pipeline Template from {}. {}".format(
                        dir_name, str(e)))
    return pipelines


def sanitize(s):
    return s.replace("\n", " ")


def entry_point_to_str(e_d):
    return ENTRY_POINT_TEMPLATE.format(i=e_d.get('entryId', "NONE"), f=e_d.get('fileTypeId', "UNKNOWN"), n=e_d['name'])


def entry_points_to_str(ep_d_list):
    return "\n".join([entry_point_to_str(e) for e in ep_d_list])


def tags_to_str(tags):
    if tags:
        return ",".join(tags)
    else:
        return ""


def convert_pipeline_to_rst(pipeline):
    """:type pipeline: Pipeline"""
    converted_pipeline = []

    # this requires fixing the IO layer and the Pipeline class
    # task_options = pipeline.task_options

    pipeline_version = pipeline['version']
    task_options = pipeline['taskOptions']
    tags = pipeline['tags']
    pipeline_id = pipeline['id']
    name = sanitize(pipeline['name'])
    desc = pipeline['description']
    raw_entry_points = pipeline['entryPoints']
    entry_point_str = entry_points_to_str(raw_entry_points)

    _d = dict(i=pipeline_id, n=name, d=desc, t=len(task_options),
              e=entry_point_str, v=pipeline_version, g=tags_to_str(tags))
    s = PIPELINE_DETAIL_TEMPLATE.format(**_d)

    if task_options:
        header = ["Name", "ID", "Default Value", "OptionType", "Description"]
        table = []
        for to in task_options:
            raw_description = to['description']
            # this will generate invalid rst tables if |n are present
            description = sanitize(raw_description)
            option_type = to['optionTypeId']
            row = [to['name'], to['id'], str(to['default']), option_type, description]
            table.append(row)
        rst_table = make_rst_table(table, headers=header)
        converted_pipeline.append(s + rst_table)
    else:
        converted_pipeline.append(s)

    converted_pipeline.append(pipeline_id)
    converted_pipeline.append(name)
    return converted_pipeline


def generate_index(pipeline_ids):

    outs = []
    f = outs.append

    for pipeline_id in pipeline_ids:
        f("    {}".format(pipeline_id))

    return INDEX_RST_TEMPLATE + "\n".join(outs)


def _write_file(output, s):
    with open(output, 'w+') as f:
        f.write(s)
    return s


def write_converted_pipelines(converted_pipelines, doc_output_dir, index_rst="index.rst"):

    if not os.path.exists(doc_output_dir):
        os.makedirs(doc_output_dir)

    pipeline_ids = set([])
    for pipeline_str, pipeline_id, pipeline_name in converted_pipelines:
        pipeline_rst = op.join(doc_output_dir, pipeline_id + ".rst")
        _write_file(pipeline_rst, pipeline_str)
        pipeline_ids.add(pipeline_id)

    full_index_rst = os.path.join(doc_output_dir, index_rst)
    index_str = generate_index(pipeline_ids)
    _write_file(full_index_rst, index_str)

    return 0


def convert_pipeline_json_files(args):

    output_dir = os.path.abspath(os.path.expanduser(args.output_dir))
    # see comments above about IO layer
    # pipelines = L.load_resolved_pipeline_template_jsons_from_dir(args.pipeline_dir)
    pipelines = load_pipelines_from_dir(args.pipeline_dir)
    converted_pipelines = [convert_pipeline_to_rst(p) for p in pipelines]
    write_converted_pipelines(converted_pipelines, output_dir)

    return 0


def get_parser():
    desc = "Generate Pipeline documentation from a directory of Resolved Pipeline Templates"
    p = get_default_argparser_with_base_opts(__version__, desc)
    p.add_argument("pipeline_dir", type=validate_dir,
                   help="Path to Pipeline Template JSON Dir")
    p.add_argument('-o', "--output-dir", default="pipeline-docs",
                   help="Path to RST Output Dir")
    return p


def main(argv=sys.argv):
    parser = get_parser()
    return pacbio_args_runner(argv[1:], parser, convert_pipeline_json_files, log, setup_log)

if __name__ == "__main__":
    sys.exit(main(argv=sys.argv))
