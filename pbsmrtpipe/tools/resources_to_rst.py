import os
import os.path as op
import sys
import logging
import json
import re

from pbcommand.utils import setup_log
from pbcommand.cli import pacbio_args_runner, get_default_argparser_with_base_opts
from pbcommand.validators import validate_dir

log = logging.getLogger(__name__)
slog = logging.getLogger('status.' + __name__)

__version__ = "0.1.0"


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


def convert_pipeline_to_rst(pipeline):
    """:type pipeline: ConvertedPipeline"""
    converted_pipeline = []
    if len(pipeline['taskOptions']) > 0:
        header = ["Name", "ID", "Description"]
        table = []
        for to in pipeline['taskOptions']:
            row = [to['name'], to['id'], to['description']]
            table.append(row)
        rst_table = make_rst_table(table, headers=header)
        converted_pipeline.append(rst_table)
    else:
        converted_pipeline.append('')
    converted_pipeline.append(pipeline['id'])
    return converted_pipeline

def write_converted_pipelines(converted_pipelines, output_dir):
    pipeline_dir = op.join(output_dir, 'pipelines')
    os.makedirs(pipeline_dir)
    for cp in converted_pipelines:
        pipeline_rst = op.join(pipeline_dir, cp[1] + ".rst")
        with open(pipeline_rst, "w") as f:
            f.write(cp[0])
    return 0


def convert_pipeline_json_files(args):

    pipelines = load_pipelines_from_dir(args.pipeline_dir)
    converted_pipelines = [convert_pipeline_to_rst(p) for p in pipelines]
    write_converted_pipelines(converted_pipelines, args.output_dir)

    return 0


def get_parser():
    desc = "Description"
    p = get_default_argparser_with_base_opts(__version__, desc)
    p.add_argument("pipeline_dir", type=validate_dir,
                   help="Path to Pipeline Template JSON Dir")
    p.add_argument("--output-dir", 
                   help="Path to RST Output Dir")
    return p


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_parser()
    return pacbio_args_runner(argv_[1:], parser, convert_pipeline_json_files, log, setup_log)

if __name__ == "__main__":
    main()
