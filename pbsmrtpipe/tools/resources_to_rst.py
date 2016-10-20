import os
import os.path as op
import sys
from collections import namedtuple

from pbcommand.cli import get_default_argparser_with_base_opts
from pbcommand.validators import validate_dir

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
    converted_pipeline = namedtuple("", "")
    header = ["Name", "ID", "Description"]
    table = []
    for to in pipeline['taskOptions']:
        row = [to['name'], to['id]', to['description']]
        table.append(row)
    rst_table = make_rst_table(table, headers=header)
    return converted_pipeline(rst_table, pipeline['id'])

def write_converted_pipelines(converted_pipelines, output_dir):
    """
    writes a index rst file to the output dir with each pipeline named
    pipeline_id.rst
    Maybe each individual pipeline.rst should be nested within a subdir called "pipelines"
    index.rst
    pipelines/
        - my_pipeline.rst
    :type converted_pipelines: list[ConvertedPipeline]
    :type output_dir: basestring
    """
	pipeline_dir = op.join(output_dir, 'pipelines')
	os.makedirs(pipeline_dir)
	for cp in converted_pipelines:
		pipeline_rst = op.join(pipeline_dir, cp[1] + ".rst")
		with open(pipeline_rst, "w") as f:
			f.write(cp[0])
    return 0


def convert_pipeline_json_files(pipeline_dir, output_dir):

    pipelines = load_pipelines_from_dir(pipeline_dir)
    converted_pipelines = [convert_pipeline_to_rst(p) for p in pipelines]
    write_converted_pipelines(converted_pipelines, output_dir)

    return 0


def get_parser():
    desc = "Description"
    p = get_default_argparser_with_base_opts(__version__, desc)
    p.add_argument("path_to_pipeline_dir", type=validate_dir,
                   help="Path to Pipeline Template JSON Dir")
    p.add_argument("--output-dir", "pipeline-outputs")
    return p


def main(args):

    return 0


if __name__ == '__main__':
    sys.exit(main(args=sys.argv[1:]))
