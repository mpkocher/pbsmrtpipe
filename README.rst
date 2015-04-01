pbsmrtpipe
==========

(In Development) Workflow Engine for working with PacBio data.

Prototyped New CLI
------------------


.. code-block:: bash

    > pbsmrtpipe workflow my_workflow.xml --output=/path/to/output_dir entry_01:/path/to/file.txt entry_id_02:/path/to/file2.txt

    > pbsmrtpipe show-templates [no options]

    > pbsmrtpipe show-template-details [template id]


Help
----


.. code-block:: bash

    (dev_pbsmrtpipe)pbsmrtpipe $> pbsmrtpipe --help
    usage: pbsmrtpipe [-h] [-v]
                      {workflow,show-templates,show-template-details,show-tasks,show-task-details}
                      ...

    Description of pbsmrtpipe 2.2.1

    positional arguments:
      {workflow,show-templates,show-template-details,show-tasks,show-task-details}
                            commands
        workflow            Run a workflow using a workflow template or with
                            explict Bindings and EntryPoints
        show-templates      List all workflow templates. A workflow 'id' can be
                            referenced in your workflow.xml file using '<template
                            id="RS_Subreads.1" />. This can replace the explicit
                            listing of EntryPoints and Bindings
        show-template-details
                            Show details about a specific Workflow template
        show-tasks          Show completed list of Tasks by id
        show-task-details   Show Details of a particular task by id (e.g.,
                            'pbsmrtpipe.tasks.filter_report'). Use 'show-tasks' to
                            get a completed list of registered tasks.

    optional arguments:
      -h, --help            show this help message and exit
      -v, --version         show program's version number and exit


Included Commandline Tools
--------------------------

Bundled commandline utils are `pbtools-*`


.. code-block::

    (dev_pbsmrtpipe)pbsmrtpipe $> pbtools-
    pbtools-chunker    pbtools-converter  pbtools-gather     pbtools-runner     pbtools-scatter


Example `pbtools-scatter`

    (venv)mkocher$ pbtools-scatter --help
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
