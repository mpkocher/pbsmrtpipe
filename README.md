# pbsmrtpipe

(Beta) Workflow Engine for working with PacBio data.

[Official Latest Documentation](http://pbsmrtpipe.readthedocs.org/)

[![Circle CI](https://circleci.com/gh/PacificBiosciences/pbsmrtpipe.svg?style=svg)](https://circleci.com/gh/PacificBiosciences/pbsmrtpipe)


# Features

- Integrates natively with Pacific Biosciences sequence data
- Runs Scatter/Gather model to scale bioinformatic analysis
- Configurable HPC scheduler (SGE, PBS, Slurm)
- [pbcommand](https://github.com/PacificBiosciences/pbcommand) Python Tool Contract API to define tasks
- Simple Bindings model to define pipelines and pipeline templates 
- HTML Reports
- interfaces with [smrtflow](http://github.com/Pacificbiosciences/smrtflow)


# Defining Tasks using Tool Contracts

pbcommand library can be used to call python functions natively, or generate an CLI interface that can be emit **Tool Contract** JSON files, or run **Resolved Tool Contract** JSON files.
 
See [pbcommand](http://pbcommand.readthedocs.org/) for details.
 
An example using the 'quick' model that allows you to call python functions and define metadata about the task, such as the **FileType**

```python
from pbcommand.models import FileTypes
from pbcommand.cli import registry_builder, registry_runner

log = logging.getLogger(__name__)

registry = registry_builder("pbcommand", "python -m pbcommand.cli.examples.dev_quick_hello_world ")


def _example_main(input_files, output_files, **kwargs):
    log.info("Running example main with {i} {o} kw:{k}".format(i=input_files,
                                                               o=output_files, k=kwargs))
    # write mock output files, otherwise the End-to-End test will fail
    xs = output_files if isinstance(output_files, (list, tuple)) else [output_files]
    for x in xs:
        with open(x, 'w') as writer:
            writer.write("Mock data\n")
    return 0


@registry("dev_qhello_world", "0.2.1", FileTypes.FASTA, FileTypes.FASTA, nproc=1, options=dict(alpha=1234))
def run_rtc(rtc):
    return _example_main(rtc.task.input_files[0], rtc.task.output_files[0], nproc=rtc.task.nproc)


@registry("dev_fastq2fasta", "0.1.0", FileTypes.FASTQ, FileTypes.FASTA)
def run_rtc(rtc):
    return _example_main(rtc.task.input_files[0], rtc.task.output_files[0])


if __name__ == '__main__':
    sys.exit(registry_runner(registry, sys.argv[1:]))
```

The tool contracts can now be emitted to a directory and tool ids can be used in **Pipeline Bindings** to define the workflow graph.

```bash
$> python -m pbcommand.cli.examples.dev_quick_hello_world -o /path/to/my-tool-contracts
```

## Defining a Pipeline

A pipeline template can be defined using a simple micro language to bind outputs to inputs.

See **pbsmrtpipe.pb_pipelines_dev** for examples.

The **bindings** are a list of tuples that are used to define the mappings of the output of a task to the input of task.

This will map the 0-th output of **pbsmrtpipe.tasks.dev_txt_to_fasta** to the first input of task **pbsmrtpipe.tasks.dev_filter_fasta**.

```python
b1 = [('pbsmrtpipe.tasks.dev_txt_to_fasta:0', 'pbsmrtpipe.tasks.dev_filter_fasta:0')]

```

Registering a pipeline can be written in python. An **entry-point** must be defined as the external input to the pipeline.


```python

import logging

from pbsmrtpipe.core import register_pipeline
from pbsmrtpipe.constants import to_pipeline_ns

log = logging.getLogger(__name__)


@register_pipeline("pipelines.pbsmrtpipe.dev_local", "Dev Local Hello Pipeline")
def get_dev_local_pipeline():
    """Simple example pipeline"""
    
    b1 = [('$entry:e_01', 'pbsmrtpipe.tasks.dev_hello_world:0')]

    b2 = [('pbsmrtpipe.tasks.dev_hello_world:0', 'pbsmrtpipe.tasks.dev_hello_worlder:0')]

    b3 = [('pbsmrtpipe.tasks.dev_hello_world:0', 'pbsmrtpipe.tasks.dev_txt_to_fasta:0')]

    b4 = [('pbsmrtpipe.tasks.dev_txt_to_fasta:0', 'pbsmrtpipe.tasks.dev_filter_fasta:0')]

    return b1 + b2 + b3 + b4
```

Once the pipeline is registered it can be referenced in **pipeline-template** and will accessible via the CLI using **pbsmrtpipe show-templates**

The pipeline can by run by referencing id **pipelines.pbsmrtpipe.dev_local** using `--entry-point e_01:/path/to/file.txt` as a CLI arg.

## CLI



    > pbsmrtpipe pipeline my_workflow.xml --output=/path/to/output_dir entry_01:/path/to/file.txt entry_id_02:/path/to/file2.txt
    
    > pbsmrtpipe pipeline-id pbsmrtpipe.pipelines.dev_pipeline --output=/path/to/output_dir entry_01:/path/to/file.txt entry_id_02:/path/to/file2.txt
    
    > pbsmrtpipe show-templates # Displays all registered pipelines templates
    
    > pbsmrtpipe show-template-details [template id] # Displays pipeline template details
    

### Help


```bash
(mk_pbsmrtpipe_test)pbsmrtpipe $> pbsmrtpipe --help
usage: pbsmrtpipe [-h] [-v]
                  {pipeline,pipeline-id,task,show-templates,show-template-details,show-tasks,show-task-details,show-workflow-options}
                  ...

Pbsmrtpipe workflow engine

positional arguments:
  {pipeline,pipeline-id,task,show-templates,show-template-details,show-tasks,show-task-details,show-workflow-options}
                        commands
    pipeline            Run a pipeline using a pipeline template or with
                        explict Bindings and EntryPoints.
    pipeline-id         Run a registered pipline by specifiying the pipline
                        id.
    task                Run Task by id.
    show-templates      List all pipeline templates. A pipeline 'id' can be
                        referenced in your my_pipeline.xml file using
                        '<import-template
                        id="pbsmrtpipe.pipelines.my_pipeline_id" />. This can
                        replace the explicit listing of EntryPoints and
                        Bindings.
    show-template-details
                        Show details about a specific Pipeline template.
    show-tasks          Show completed list of Tasks by id
    show-task-details   Show Details of a particular task by id (e.g.,
                        'pbsmrtpipe.tasks.filter_report'). Use 'show-tasks' to
                        get a completed list of registered tasks.
    show-workflow-options
                        Display all workflow level options that can be set in
                        <options /> for preset.xml

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
```

### Development and Testing


"testkit" is the test framework for testing pipelines.

Uninstall and Install pbsmrtpipe

```
make install
```

Test defined Tasks

```
make test-tasks
```

Test defined Pipelines

```
make test-pipelines
```

Test defined Chunk Operators

```
make test-operators
```

Test All core defined tasks, pipelines and chunk operators

```
make test-sanity
```

Running test-data Integration Pipelines with testkit

```
make test-dev
```

Run the entire test suite (unittests and integration tests) This should be run before every pull request.

```
make test-suite
```

Clean All 

```bash
make clean-all
```
