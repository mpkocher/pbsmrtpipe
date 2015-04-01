Advanced Task Model Using Dependency Injection Model
====================================================

.. _notebook: http://nbviewer.ipython.org/gist/mpkocher/8bb1673da78246e59f4c/ScratchPad.ipynb

.. code-block:: python

    # Very simple task
    one_proc = 1
    opts_schema = {} # task has no options

    @register_task('pbsmrtpipe.tasks.simple_task_01',
                    TaskTypes.LOCAL,
                    (FileTypes.FASTA, ),
                    (FileTypes.REPORT, ),
                    opts_schema, one_proc, (),
                    output_file_names = (('my_awesome_report', 'json'), ))
    def to_cmd(input_files, output_files, resolved_opts, nproc, resources):
        """Simple Hello world task. fasta -> report """
        _d = dict(i=input_files[0], o=output_files[0], n=nproc)
        return "my-simple-cmd.sh --nproc {n} {i} {o}".format(**_d)


The **register_task** deco signature (task_id, task_type, input_types, output_types, task_option_schema, nproc, resource_types, output_file_names=None). The decorator is syntactic sugar for creating a `MetaTask` instance and registering the meta task in a globally accessible registry by id.

The **to_cmd** function will have the same signature (with the exception of task_id and task_type and kwargs), but the values passed to the function will be resolved (e.g., input_types
will be replaced with actual paths to files, or resolved options).

Details (or order of function signature)

- **task_id** This the global unique identifier for the task (it must begin with "pbsmrtpipe.tasks")
- **task_type** is an enum of pbsmrtpipe.constants.local_task or pbsmrtpipe.constants.distributed_task (referenced by class constants TaskTypes.LOCAL and TaskTypes.DISTRIBUTED)
- **input file types** by ids (e.g., ['pbsmrtpipe.files.fasta', 'pbsmrtpipe.files.movies']) The can be referenced using class constants of FileTypes. For example, FileTypes.FASTA, FileTypes.REPORT)
- **output file types** see **input_types_files**
- **opts_schema** are task options as a dict of {option_id:jsonschema}
- **nproc** is the number of processors used.
- **resource_types** are files or dirs are 'resources' (e.g, ResourceTypes.TMP_FILE, ResourceTypes.TMP_DIR, ResourceTypes.LOG_FILE) that can be accessed positionally into the list. For example, in **to_cmd**, you can reference **resources[0]**.
- **output_file_names** is a keyword argument that allows you to specify the name of each output file. The format is a list of tuples (base name, extension)


For task_type, opts, nproc and nchunks (introduced later), the values can be provided as corresponding primitive data type (e.g., int for nproc), or they can be configured via a **Dependency Injection Model**. This model allows the description of the computation of the value to be specified by a custom function and reference other values. For example, nproc can be computed from the task type, and, or the resolved options.

This allows for a great deal of flexibility and expressiveness. These custom function can be defined to have explicit dependencies on values that **will be resolved**. These are called **symbols** and are prefixed \$ value (e.g., \$max_nproc).

Symbols Definitions

    - \$max_nproc the max number of processors (no dependencies, accessible by any DI configurable option)
    - \$max_nchunks the max number of chunks to use  (no dependencies, accessible by any DI configurable option)
    - \$opts_schema the options id schema that is provided to the task decorator
    - \$opts is the options provided by the user
    - \$task_type the value computed from task type DI model or primitive value provided
    - \$ropts the resolved and validated options
    - \$nproc the value computed from nproc DI model or primitive value provided
    - \$nchunks the value computed from nchunks DI model or primitive value provided in the **register_scatter_task** deco


Dependency Injection Model
==========================

The format for the DI model is a list of values with the last item being a custom function that can be called.

The general Dependency Injection model works as follows to compute the number of processors to use.

To define how to compute `$nproc`, you can specify an int, a `$max_nproc` or use a dependency injection model by specifying a list of dependencies (e.g, resolved options, task type, nchunks) and a function in a list.

The function is specified as the last element of the list, and the signature of the function must have a signature of list[:-1].

The only constraint when defining dependencies is to not create cyclic dependencies. For example, determining the value of '$nproc' has a dependent on resolved task options ('$ropt') and the resolved options have a dependency on '$nproc'. This creates a cycle.

TLDR
----

The task type, options, nproc and number of chunks can be given as primitives, or they can be provided as a **Dependency Injection** list model.

Using the DI list model, each computation of task type, nproc, resolved opts, number of chunks can reference other 'resolved' values (\$task_type, \$nproc, \$ropts, \$nchunks, respectively), or provided Symbol values (\$max_nproc, \$max_nchunks). The **only** constraint is **DO NOT** create a cycle between the dependencies.

(**Please see** this ipython notebook_ for an example of the resolved DI models for several example tasks)

Example #1
----------

Specify the computation of the number of processors to use based on the resolved options. In the **register_task** deco would replace the primitive value (1) in the task with a **Dependency Injection Model** list definition.

.. code-block:: python

    def my_func(resolved_opts, n, m):
        return 8

    ['$ropts', 5, 3.14159, my_func]

**my_func** is a custom function and expected to have the signature (resolve_opts, n, m) and '$ropts' will be injected with the resolved task options.

Example #2
----------

Specify the number of processors for the task to use based on the max number of processors and the resolved options.

.. code-block:: python

    def my_nproc_func(nproc, ropts, resolved_opts):
        return nproc / 2

    ['$max_nproc', '$ropts', my_nproc_func]


Accessing Data in Files
-----------------------

Passing data at runtime to task options, nproc, task_type, or nchunks can be performed by accessing a input file type that is pbreport JSON file (i.e., `FileTypes.REPORT`).

Example of a pbreport JSON file.


.. code-block:: javascript

    {
    "_changelist": 130583,
    "_version": "2.3",
    "attributes": [
        {
            "id": "pbreports_example_report.nmovies",
            "name": null,
            "value": 19
        },
        {
            "id": "pbreports_example_report.ncontigs",
            "name": null,
            "value": 5
        },
        {
            "id": "pbreports_example_report.my_attribute_id",
            "name": null,
            "value": 10.0
        }
    ],
    "id": "pbreports_example_report",
    "plotGroups": [],
    "tables": []
    }


If your task has an input file that is a report, you can access values in the JSON report and use them in a **Dependency Injection Model** list. For example, you can compute the number of processors based on a value in a report that is computed at run time.

A new Symbol **\$inputs.0.my_attr_id** is used to specify the input file and the positional index into the **input types** list. The **my_attr_id** is the report attribute id in the report to extract. The value **will be resolved at runtime**.

For example, '\$inputs.2.attr_x' is interpreted as: grab the third file in input files (which must be of type `FileTypes.Report`) and extract attribute 'attr_x' from the JSON report.


Passing Data to Tasks
---------------------

Use case motivation: Determine the **task_type** from the **resolved options** and **attr_id** in the JSON report input file.

Similar to the DI modelable values, \$task_type, \$ropts, \$nproc, or \$nchunks, \$inputs.0 can be used in a DI model list.

Extending a previous example:

**This assumes that the first file in the task input types list is a report type (FilesTypes.REPORT))**

.. code-block:: python

    def my_nproc_func(nproc, resolved_opts, my_report_attribute_id):
        return nproc / 2

    ['$max_nproc', '$ropts', '$inputs.0.attr_x', my_nproc_func]


At **runtime** the value will be extracted from the report and passed the custom function.


Complete Example using Dependency Injection Model
-------------------------------------------------

Use case Motivations:

- We want to compute the task type based on the resolved options and a 'attr_id' from first file in the input files.
- We want to compute the resolved opts based on 'attr_id' from first file of the input files.
- We want to compute nproc based on the max number of processors and resolved options

Additionally, we need to request a temp directory to be created. This directory is managed by the workflow and will automatically be deleted on the remote node when the task is completed.

.. code-block:: python


    def dyn_opts_func(opts, my_option_01):
        """Return a dict of resolved opts with a value that is extracted from
        a pbreports JSON file."""
        # override option
        opts['my_option_01'] = my_option_01
        return opts

    def compute_task_type(resolved_opts, attr_value):
        """ Can compute the task type based on the resolved opts and
        and attribute from the report"""
        return TaskTypes.DISTRIBUTED

    def _my_nproc(max_nproc, resolved_opts):
        """ Determine the value of nproc based on resolve options"""
        return max_nproc / 3


    @register_task('pbsmrtpipe.tasks.task_id4',
               ('$ropts', '$inputs.0.attr_id', compute_task_type),
               (FileTypes.REPORT, FileTypes.FASTA),
               (FileTypes.ALIGNMENT_CMP_H5,),
               (opts, '$inputs.0.attr_id', dyn_opts_func),
               ('$max_nproc', '$ropts', _my_nproc),
               (ResourceTypes.TMP_DIR, ),
               output_file_names=(('my_awesome_alignment', 'cmp.h5'), ))
    def to_cmd(input_files, output_files, resolved_opts, nproc, resources):
        """
        Example of dynamically passing values computed at runtime from an previous task via a pbreport.

        Map the first input file (must be a report type)

        $inputs.0 -> input_file_types.POSITIONAL.report_attribute_id

        Looks for 'attr_id' in the Attribute section of report.

        Can compute if the job should be submitted to the queue via ['$ropts', '$inputs.0.attr_id', compute_task_type]

        """
        _d = dict(e="my_exe.sh", f=input_files[0], o=output_files[0], n=nproc, t=resources[0])
        return "{e} --nproc={n} --tmpdir={t} --fasta={f} -o={o}".format(**_d)


Explicit Resolved Tasks
=======================

See several examples of the resolved dependencies in this ipython notebook_. **PLEASE VIEW ME**

These examples shown the graph of DI model lists and explicitly show the dependency resolution and the conversion of a **MetaTask** instance to a **Task** instance.


More Task Examples
==================

(these are taken from `di_task_api.py`)

.. code-block:: python

    @register_task('pbsmrtpipe.tasks.simple_task_01',
                   TaskTypes.LOCAL,
                   (FileTypes.FASTA, ),
                   (FileTypes.REPORT, ), {}, 1, ())
    def to_cmd(input_files, output_files, resolved_opts, nproc, resources):
        """Simple Hello world task. fasta -> report """
        _d = dict(i=input_files[0], o=output_files[0], n=nproc)
        return "my-simple-cmd.sh --nproc {n} {i} {o}".format(**_d)


    @register_task('pbsmrtpipe.tasks.my_task_id',
                   TaskTypes.LOCAL,
                   (FileTypes.FASTA, FileTypes.RGN_FOFN),
                   (FileTypes.ALIGNMENT_CMP_H5, ),
                   opts,
                   one_proc,
                   (ResourceTypes.TMP_DIR, ResourceTypes.LOG_FILE),
                   output_file_names=(('my_awesome_alignments', 'cmp.h5'), ))
    def to_cmd(input_files, output_files, resolved_opts, nproc, resources):
        """
        Simple Example Task
        """
        my_tmp_dir = resources[0]
        _d = dict(e="my_exe.sh", l=resources[1], t=my_tmp_dir, o=resolved_opts['my_task_option_id'], i=input_files[0], f=input_files[1], r=output_files[0], n=nproc)
        return "{e} --nproc={n} --tmp={t} --log={l} --my-option={o} fasta:{i} --region={f} --output-report={r}".format(**_d)


    @register_task('pbsmrtpipe.tasks.task_id2',
                   TaskTypes.DISTRIBUTED,
                   (FileTypes.VCF, ),
                   (FileTypes.REPORT, ),
                   opts,
                   '$max_nproc',
                   (ResourceTypes.TMP_DIR, ResourceTypes.TMP_FILE, ResourceTypes.TMP_FILE))
    def to_cmd(input_files, output_files, resolved_opts, nproc, resources):
        """
        Note: Multiple 'resources' of the same type can be provided.
        """
        _d = dict(e="my_exe.sh")
        return "{e}".format(**_d)


    def compute_nproc(global_nproc, resolved_opts):
        return global_nproc / 2


    def compute_task_type(opts):
        """This must return pbsmrtpipe.constants.{local_task,distributed_task}"""
        return "pbsmrtpipe.constants.local_task"


    @register_task('pbsmrtpipe.tasks.task_id8',
                   ('$ropts', compute_task_type),
                   (FileTypes.FASTA, FileTypes.REPORT),
                   (FileTypes.VCF, ),
                   opts,
                   ('$max_nproc', '$ropts', compute_nproc),
                   (ResourceTypes.TMP_DIR, ResourceTypes.TMP_FILE, ResourceTypes.TMP_FILE))
    def to_cmd(input_files, output_files, resolved_opts, nproc, resources):
        """
        Note: Set nproc via dependency injection based on $max_nproc,
        The nproc DI list (x) is translated to x[-1](*x[:-1])

        Compute the task type based on the options
        """
        _d = dict(e="my_exe.sh")
        return "{e}".format(**_d)


    def my_nproc_func(global_nproc, opts):
        return 12


    def my_custom_validator(resolved_opts, a, b, c):
        """Returns resolved option dict or raises an exception.

        This is just illustrating that the DI is blindly passing values. if there's a $X 'special' value, then
        this will be injected. But passing numbers will work as well.
        """
        return resolved_opts


    @register_task('pbsmrtpipe.tasks.task_id3',
                   TaskTypes.DISTRIBUTED,
                   (FileTypes.MOVIE_FOFN, FileTypes.RGN_FOFN),
                   (FileTypes.FASTA, FileTypes.FASTQ),
                   (opts, 1, 2, 3, my_custom_validator),
                   ('$max_nproc', '$ropts', my_nproc_func),
                   (ResourceTypes.TMP_FILE,),
                   output_file_names=(('my_file', 'fasta'), ('my_f2', 'fastq')))
    def to_cmd(input_files, output_files, resolved_opts, nproc, resources):
        """
        Let's set nproc to be dependent on the resolved options and $max_nproc

        Note: '$opts' is the resolved options, whereas 'opts' is the {option_id:JsonSchema}

        Need to think this over a bit.

        """
        _d = dict(e="my_exe.sh")
        return "{e}".format(**_d)


    def dyn_opts_func(opts, my_option_01):
        """Return a dict of resolved opts

        Are these the resolve opts that are passed in?
        """
        # override option
        opts['my_option_01'] = my_option_01
        return opts


    def compute_task_type(opts, attr_value):
        return "pbsmrtpipe.constants.distributed_task"


    def _my_nproc(global_nproc, resolved_opts):

        return global_nproc / 3


    @register_task('pbsmrtpipe.tasks.task_id4',
                   ('$ropts', '$inputs.0.attr_id', compute_task_type),
                   (FileTypes.REPORT, FileTypes.FASTA),
                   (FileTypes.ALIGNMENT_CMP_H5,),
                   (opts, '$inputs.0.attr_id', dyn_opts_func),
                   ('$max_nproc', '$ropts', _my_nproc),
                   (ResourceTypes.TMP_FILE, ),
                   output_file_names=(('my_file', 'cmp.h5'), ))
    def to_cmd(input_files, output_files, resolved_opts, nproc, resources):
        """
        Example of dynamically passing values computed at runtime from an previous task via a pbreport.

        Map the first input file (must be a report type)
        $inputs.0 -> ft.report_type_id

        And looks for 'attr_id' in the Attribute section of report

        Can compute if the job should be submitted to the queue via ['$opts', '$inputs.0.attr_id', compute_task_type]

        """
        _d = dict(e="my_exe.sh")
        return "{e}".format(**_d)


    def nchunks_func(nmovies, resolved_opts, resolved_nproc):
        max_chunks = resolved_opts['max_chunks']
        return min(int(nmovies), max_chunks)


    @register_scatter_task('pbsmrtpipe.tasks.task_id5',
                           'pbsmrtpipe.constants.distributed_task',
                           (FileTypes.REPORT, FileTypes.MOVIE_FOFN),
                           (FileTypes.RGN_FOFN, ),
                           opts,
                           ('$max_nproc', '$ropts', _my_nproc),
                           (ResourceTypes.OUTPUT_DIR, ),
                           ('$inputs.0.attr_id', '$ropts', '$nproc', nchunks_func),
                           output_file_names=(('my_rgn_movie', 'fofn'), ))
    def to_cmd(input_files, output_files, resolved_opts, nproc, resources):
        """
        Scatter Tasks extend the standard task and include a 7th DI mechanism which will set $nchunks which can be used at the workflow level.

        $nchunks is only communicated to the workflow level for proper graph construction, therefore it's not included in the to_cmd signature.

        For example, if $nchunks is set to 3, then outFiles will have the ['/path/to/chunk1', '/path/to/chunk2', '/path/to/chunk3']

        This yields a slightly odd API from a commandline. my_exe.sh input.fofn --output '/path/to/chunk1' '/path/to/chunk2' '/path/to/chunk3'

        Is this a more nature commandline API would be my_exe.sh input.fofn --output-prefix="chunk_" --output-dir=/path/to --nchunks=3
        """
        _d = dict(e="my_exe.sh")
        return "{e}".format(**_d)
