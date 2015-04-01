# pbsmrtpipe

(Alpha) Workflow Engine for working with PacBio data.

[Official Latest Documentation](http://pbsmrtpipe.readthedocs.org/en/latest/)

[![Build Status](https://travis-ci.org/mpkocher/pbsmrtpipe.svg?branch=master)](https://travis-ci.org/mpkocher/pbsmrtpipe)


# Features

- Integrates natively with Pacific Biosciences sequence data
- Runs Scatter/Gather model to scale bioinformatic analysis
- Configurable scheduler (SGE, PBS, Slurm)
- Tasks can be defined in python
- Simple Bindings model to define pipelines and pipeline templates 
- HTML Reports


# Defining Tasks

```python
class GffToBed(MetaTaskBase):
    TASK_ID = "pbsmrtpipe.tasks.mapping_gff_to_bed"
    NAME = "Gff to Bed"
    VERSION = "1.0.0"

    TASK_TYPE = TaskTypes.DISTRIBUTED

    INPUT_TYPES = [(FileTypes.GFF, "gff", "Gff file")]
    OUTPUT_TYPES = [(FileTypes.BED, "bed", "Bed File")]
    OUTPUT_FILE_NAMES = [('coverage', 'bed')]

    NPROC = 1
    SCHEMA_OPTIONS = {}
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        name = 'meanCoverage'
        purpose = 'coverage'
        description = 'Mean coverage of genome in fixed interval regions'
        exe = "gffToBed.py"

        _d = dict(e=exe, i=input_files[0], o=output_files[0], n=name, d=description, p=purpose)
        return 'gffToBed.py --name={n} --description="{d}" {p} {i} > {o}'.format(**_d)
```




## CLI


    > pbsmrtpipe pipeline my_workflow.xml --output=/path/to/output_dir entry_01:/path/to/file.txt entry_id_02:/path/to/file2.txt
    
    > pbsmrtpipe pipeline-id pbsmrtpipe.pipelines.dev_pipeline --output=/path/to/output_dir entry_01:/path/to/file.txt entry_id_02:/path/to/file2.txt
    
    > pbsmrtpipe show-templates # Displays all registered pipelines templates
    
    > pbsmrtpipe show-template-details [template id] # Displays pipeline template details
    

### Help


    (dev_pbsmrtpipe_test)pbsmrtpipe $> pbsmrtpipe --help
    usage: pbsmrtpipe [-h] [-v]
                      {pipeline,pipeline-id,task,show-templates,show-template-details,show-tasks,show-task-details,show-workflow-options}
                      ...
    
    Description of pbsmrtpipe 0.5.18
    
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

### Tools

Bundled commandline utils are `pbtools-*`

    (dev_pbsmrtpipe_test)pbsmrtpipe $> pbtools-
    pbtools-chunker    pbtools-converter  pbtools-gather     pbtools-report     pbtools-runner     pbtools-scatter   



Example `pbtools-scatter`
    
    (dev_pbsmrtpipe_test)pbsmrtpipe $> pbtools-scatter --help
    usage: pbtools-scatter [-h] [-v] {fofn,fasta,fastq,reference-entry} ...
    
    Scattering File Tool used within pbsmrtpipe.
    
    positional arguments:
      {fofn,fasta,fastq,reference-entry}
                            Subparser Commands
        fofn                Split FOFN (file of file names) into multiple FOFNs
        fasta               Split Fasta file into multiple files.
        fastq               Split Fastq file into mulitple files.
        reference-entry     Split Reference Entry into multiple chunks
    
    optional arguments:
      -h, --help            show this help message and exit
      -v, --version         show program's version number and exit
    
