Pbsmrtpipe Chunking Framework
=============================

The section describes the chunking (aka scatter/gather) model for splitting tasks into smaller tasks to enable task scalability.

Chunking Basic Design Model
---------------------------

At the highest level, the task chunking works as follows.

Unchunked case: File1 -> Task1 -> File2 -> Task2

Will be transformed to:

File1 -> ScatterTask1 -> **scatter.chunk.json** -> (0..N) Task1 -> **gather.chunk.json** -> GatherTask1 -> File2 -> Task2

Where (0..N) Task1 is a list of tasks with task type id Task1.

The new .chunk.json files provide a new API and file format that the workflow semantically understands via chunk keys (e.g., $chunk.movie_fofn_id). The chunking operation is only supported at single task level, not multiple tasks or "channels". In other words, a single task is scatter and gathered, then the single output can be passed to a downstream task.

.. note:: The general scattering/gathering problem is a bit involved that has a modicum of complexity.

For example, given a task, `my-exe`, with a input type signature of (Fasta, Fastq, CSV), there are several models of splitting up the files. The first input (Fasta) can be split into N chunks, while the second input can not be chunk (i.e., passed directly) and the CSV could be split into M chunks (where N < M). Other possibilities include an all to all comparison of Fasta and Fastq to generate N chunks.

There are a myriad of other scatter/gather patterns that are specific to the access patterns of a specific tool. The current design separates the chunking operations from the task of interest in a language agnostic manner. By delegating the scatter/gather pattern to the separate tool, combined with defining a chunk operator, the chunking mechanism is a user defined and dynamic runtime process that has minimal constraints on the chunking pattern.


High level Model of Chunking Steps
----------------------------------

The chunking process is broken down into 4 steps described below.

- Step 0. Define a "companion" scatter/chunking task that takes the same input type signature but will a emit a single JSON Chunk file (defined below) as output
- Step 1. pbsmrtpipe will "re-map" task inputs (Task1) to the companion scattered task input (ScatterTask1), run scatter task to create scatter.chunks.json
- Step 2. at the workflow level read in the scatter.chunks.json, Create N new chunked Task1, map **$chunk.{key_id}** to N chunk Task1 inputs
- Step 3. after N chunked Task1 are completed, at the workflow level, create gather.chunk.json from **$chunk.{key_id}** of outputs
- Step 4. call gather task (GatherTask1) using gather.chunks.json to create output file(s), "re-map" outputs of GatherTask1 to Task2


Example Chunk model
-------------------

Taken from `testkit-data/dev_local_fasta_chunk <https://github.com/PacificBiosciences/pbsmrtpipe/tree/master/testkit-data/dev_local_fasta_chunk>`_

.. note:: Please run this testkit job included within pbsmrtpipe and examine the logs and graph in the job directory output of `job_output/workflow/workflow.svg` in your webbrower to examine the DAG.

More examples of chunking tools and examples are in `pbcoretools <https://github.com/PacificBiosciences/pbcoretools>`_


In this testkit-job, the `"pbsmrtpipe.tasks.dev_filter_fasta" <https://github.com/PacificBiosciences/pbsmrtpipe/blob/master/pbsmrtpipe/pb_tasks/dev.py#L191>`_ has a companion `chunked task <https://github.com/PacificBiosciences/pbsmrtpipe/tree/master/pbsmrtpipe/chunk_operators/operator_chunk_dev_filter_fasta.xml>`_ . The task has one input a **Fasta** File and emits a single **Fasta** file type.

For simplicity, scatter Tasks have the same function signature as Task to be scattered. This creates a simple one-to-one mapping of the inputs of the original task to the inputs of the scattered task.

The output signature of Scatter Task **must always** be ChunkJson type (only one output). Similarly, Gather tasks will **always** have an input type of ChunkJson type.


Chunk Operator for dev_filter_fasta
-----------------------------------

Chunk Operators are XML files that describe the mapping of scattered task and gathered task(s) for a specific task id. This model separates the chunking mechanism from the workflow construction and design.

On startup, pbsmrtpipe will load the XML operators and will apply them if a task-id is registered to be chunked.


::

    Loaded 18 operators from pbsmrtpipe.chunk_operators -> /Users/mkocher/.virtualenvs/pbsmrtpipe_test/lib/python2.7/site-packages/pbsmrtpipe/chunk_operators
    Filtered 17 chunk operators from registry.
    Starting to chunk task type pbsmrtpipe.tasks.dev_filter_fasta with chunk-group 72d80506-0d68-4574-ba71-2b61ddc1dd12 for operator pbsmrtpipe.operators.chunk_dev_filter_fasta
    Successfully applying chunk operator ['pbsmrtpipe.operators.chunk_dev_filter_fasta'] to TaskScatterBindingNode pbcoretools.tasks.dev_scatter_filter_fasta-1 to generate 7 tasks.
    Starting chunking gathering process for task TaskScatterBindingNode pbcoretools.tasks.dev_scatter_filter_fasta-1 chunk-group 72d80506-0d68-4574-ba71-2b61ddc1dd12 with operator pbsmrtpipe.operators.chunk_dev_filter_fasta



There will be a log message of the form:

::

    [DEBUG] 2016-08-18 01:47:49,748Z [status.__name__ add_chunkable_task_nodes_to_bgraph 1090] Starting to chunk task type pbsmrtpipe.tasks.dev_filter_fasta with chunk-group 72d80506-0d68-4574-ba71-2b61ddc1dd12 for operator pbsmrtpipe.operators.chunk_dev_filter_fasta
    [INFO] 2016-08-18 01:47:49,783Z [status.__name__ apply_chunk_operator 1239] Successfully applying chunk operator ['pbsmrtpipe.operators.chunk_dev_filter_fasta'] to TaskScatterBindingNode pbcoretools.tasks.dev_scatter_filter_fasta-1 to generate 7 tasks.



Chunk Operator Data Model
~~~~~~~~~~~~~~~~~~~~~~~~~

- `task-id` The original task id to scatter

Scatter
~~~~~~~

- `scatter:scatter-task-id` The companion scatter task (*must* have the same input type signature as `task-id`. The output signature must be a single chunk json file type.
- `scatter:chunk` maps a specific chunk key from the chunk.json file to the input(s) of a original `task-id`. For every positinal input, there *must* be a unique mapping to a chunk_key

Gather
~~~~~~

- `gather:chunk:gather-task-id` Gather task to call
- `gather:chunk:chunk-key` chunk key to pass to gather task
- `gather:chunk:task-output` binding output of the gather task to output of the original task-id. (e.g., "pbsmrtpipe.tasks.dev_filter_fasta:0"). This simply maps the output of the original task to the gathered task.

.. note:: Each Gather only has one output. If the original task has N-outputs, then there needs to be N gather tasks and N unique `chunk_key` in the original gather chunk json.


Example: for pbcoretools.tasks.dev_scatter_filter_fasta in `pbsmrtpipe/chunk_operators`


.. literalinclude:: ../pbsmrtpipe/chunk_operators/operator_chunk_dev_filter_fasta.xml
    :language: xml


Step 1.
-------

At the **workflow level**, the original inputs of the task ("pbsmrtpipe.tasks.dev_filter_fasta") to be scattered will be "re-mapped" to the scatter task `("pbcoretools.tasks.dev_scatter_filter_fasta") <https://github.com/PacificBiosciences/pbcoretools/blob/master/pbcoretools/tasks/scatter_filter_fasta.py>`_. The scatter task will be run to generate a scatter.chunk.json file from the original fasta file.

The workflow will use the ToolContract and ResolvedToolContract interfaces to call tasks. For purposes of transparency, the "raw" CLI interface will use to communicate the chunking mechanism.


.. code-block:: bash

    $> python -m pbcoretools.tasks.scatter_filter_fasta --chunk_key fasta_id --max_nchunks 7 /WHEREVER/pbsmrtpipe/testkit-data/dev_local_fasta_chunk/job_output/tasks/pbsmrtpipe.tasks.dev_txt_to_fasta-0/file.fasta scatter.chunk.json

The `--chunk_key` value is provided to communicate (from the workflow engine) the specific chunk value inside the `PipelineChunk` data model.

The `--max_chunks` value is provided to communicate (from the workflow engine) the maximum number of chunks that chould be created by the chunking task. If the your chunking tool exceeds this value, the workflow engine will raise an exception at terminate the pipeline execution.

.. note:: The max nchunks can be configured in the workflow level options with identifier `pbsmrtpipe.options.max_nchunks` in either the preset.xml or workflow.xml

.. code-block:: xml

    <option id="pbsmrtpipe.options.max_nchunks" >
        <value>7</value>
    </option>

This output of the exe will create a scatter.chunk.json. For tools using python, `pbcommand` has several helper methods to help write the PipelineChunk model(s) to JSON. Chunk keys that begin with '$chunk.' are inputs or outputs that communicated and written (by the workflow), respectively. Keys not beginning with '$chunk.' are assumed to be chunk metadata written by the scatter tool.

Raw output from the example testkit output task dir `testkit-data/dev_local_fasta_chunk/job_output/tasks/pbcoretools.tasks.dev_scatter_filter_fasta-1`

.. code-block:: bash

    (pbsmrtpipe_test)pbcoretools.tasks.dev_scatter_filter_fasta-1 $> ls
        chunk.json                   runnable-task.json       scattered-fasta_1.fasta  scattered-fasta_3.fasta  scattered-fasta_5.fasta  stderr  task-report.json
        resolved-tool-contract.json  scattered-fasta_0.fasta  scattered-fasta_2.fasta  scattered-fasta_4.fasta  scattered-fasta_6.fasta  stdout  tool-contract.json


The chunk.json file is written using the `PipelineChunk` data model defined in `pbcommand.models.common <https://github.com/PacificBiosciences/pbcommand/blob/master/pbcommand/models/common.py/>`_ with the Chunk key of `$chunk.fasta_id`

.. literalinclude:: example_filter_chunk_scatter.json
    :language: javascript


.. note:: A scattered/chunked JSON file can be written from any language that supports writing this spec.

Step 2.
-------

At the **workflow level**, the outputs of the N-chunked filter fasta tasks need to be
passed to the gather task(s).

The workflow will read in the scattered_chunk.json and write a new file ("gathered.chunks.json")
with the new chunks keys from the outputs of N-chunked `pbsmrtpipe.tasks.dev_filter_fasta` tasks outputs. Specifically, at the workflow level, the outputs of the chunked filter fasta task using the defined chunk key.

Example gather.chunks.json file


.. literalinclude:: example_filter_chunk_gather.json
    :language: javascript

.. note:: The chunk key (`$chunk.fasta_id`) from the original scatter operation is kept to record the entire chunking mechanism.

These workflow generated files are hidden with form {task-id}-{UUID}-gathered-pipeline.chunks.json in the `tasks` dir.

Example: `.pbcoretools.tasks.dev_scatter_filter_fasta-d6ba7f08-7ac7-4d94-a292-89d897f5cd06-gathered-pipeline.chunks.json` in `tasks` dir.

Step 3.
-------

Call the required gather tasks to create the final gathered files.

Gather tasks for `pbsmrtpipe.tasks.dev_filter_fasta <https://github.com/PacificBiosciences/pbcoretools/blob/master/pbcoretools/tasks/gather_fasta.py>`_ only require 1 gather output. In general, if a task has N outputs, it must defined N gather tasks. Each gather task takes a chunked JSON file and a chunk-key and will emit a single file.

Example

.. code-block:: bash

    $> python -m pbcoretools.tasks.gather_fasta --chunk_key filtered_fasta_id .pbcoretools.tasks.dev_scatter_filter_fasta-d6ba7f08-7ac7-4d94-a292-89d897f5cd06-gathered-pipeline.chunks.json gathered.output.fasta

This will create a single output file for that specific gather chunk defined in the Chunk Operator.


.. note:: Gather commandline tools must only have one output. In other words, these tools take a single json file and chunk key and emit a single output file that maps to one of the outputs of the original unchunked task (e.g., `pbsmrtpipe.tasks.dev_filter_fasta:0`.

Step 4.
-------

At the workflow level, "Re-map" the original task outputs to the gathered task outputs. Update any
tasks in the BindingsGraph.


More Chunking Operators Examples
================================

The four steps are encoded in XML for the "pbsmrtipe.tasks.dev_filter_fasta" example shown above and are called a **Chunking Operator**

Chunking Operators are registered under the **pbsmrtpipe.operators.** namespace.


.. literalinclude:: ../pbsmrtpipe/chunk_operators/operator_chunk_align_ds.xml
    :language: xml


Quiver Chunking Operator Example

.. literalinclude:: ../pbsmrtpipe/chunk_operators/operator_chunk_quiver_ds.xml
    :language: xml
