Creating Chunked Operators
==========================

The section describes the chunking (aka scatter/gather) model for splitting tasks into smaller tasks to enable task scalability.

Chunking Basic Design Model
---------------------------

At the highest level, the task chunking works as follows.

File1 -> Task1 -> File2 -> Task2

Will be transformed to:

File1 -> ScatterTask1 -> **scatter.chunk.json** -> (0..N) Task1 -> **gather.chunk.json** -> GatherTask1 -> File2 -> Task2

Where (0..N) Task1 is a list of tasks with task type id Task1.

The new .chunk.json files provide a new API and file format that the workflow semantically understands via chunk keys (e.g., $chunk.movie_fofn_id)

The chunking process is broken down into 4 steps described below.

High level Model of Chunking Steps
----------------------------------

(using the example above)

- Step 1. "re-map" task inputs (Task1) to scattered task input (ScatterTask1), run scatter task to create scatter.chunks.json
- Step 2. at the workflow level read in the scatter.chunks.json, Create N new chunked Task1, map **$chunk.{key_id}** to N chunk Task1 inputs
- Step 3. after N chunked Task1 are completed, at the workflow level, create gather.chunk.json from **$chunk.{key_id}** of outputs
- Step 4. call gather task (GatherTask1) using gather.chunks.json to create output file(s), "re-map" outputs of GatherTask1 to Task2


Example Chunk model
-------------------

To make this less abstract, consider chunking the "pbsmrtpipe.tasks.filter" task. The filter task as two inputs types **MovieFofn**, **Report** and two outputs types, **RegionFofn** and **CSV**.

For simplicity, scatter Tasks have the same function signature as Task to be scattered. This creates a simple one-to-one mapping of the inputs of the original task to the inputs of the scattered task.

The output signature of Scatter Task is **always** ChunkJson type (only one output)
Similarly, Gather tasks will **always** have an input type of ChunkJson type.


Step 1.
-------

At the **workflow level**, the original inputs of the task ("pbsmrtpipe.tasks.filter") to be scattered will be "re-mapped" to the scatter task ("pbsmrtpipe.tasks.scatter_filter"). The scatter task will be run to generate a scatter.chunk.json file.

scattered_chunk.json from scatter filter task (chunk the movie fofn file)

.. code-block:: bash

    $> pbtools-scatter fofn input.fofn report.json --chunk-key movie_fofn_id --max-chunks 3 -o scatter.chunk.json

The `chunk-key` option is just provided so that the commandline exe can be used for any fofn type.

The `max-chunks` argument is set to a value consistent with the metadata of the FOFN (nfiles extracted from from the report.json file). The max number of chunks is described via Dependency Injection model using a DI model list with a custom function to determine the number of chunks from the metadata provided in the report.json file).

This output of the exe will create a scatter.chunk.json. Chunk keys that begin with '$chunk.' are inputs or outputs that communicated and written (by the workflow), respectively. Keys not beginning with '$chunk.' are assumed to be chunk metadata written by the scatter tool.


.. literalinclude:: example_filter_chunk_scatter.json
    :language: javascript

.. note:: The report is always passed because the function signature of the filter task and the scatter task must be the same (even though the report might not be used within the filter task. However, as a general principle, tasks should know metadata about the input files to have a better understanding of determining options, or processing requirements).

At the **workflow level**, the original task inputs (movie.fofn, report) in this
example, will need to be re-mapped. The value of the chunk key, "$chunk.movie_fofn_id" and "report_id" from each
chunk key will get passed as the input to the original task, "pbsmrtpipe.task.filter.0" and "pbsmrtpipe.task.filter.1", respectively.


Step 2.
-------

At the **workflow level**, the outputs of the N-chunked filter tasks need to be
passed to the gather task(s).

The workflow will read in the scattered_chunk.json and write a new file ("gathered.chunks.json")
with the new chunks keys from the outputs of N-chunked filter tasks outputs. Specifically, at the workflow level, the outputs of the chunked filter task (who's output file signature is FileTypes.RGN_FOFN, FileTypes.CSV) will be added to the chunk.json file using chunk keys "$chunk.rgn_fofn_id" and "$chunk.csv_id", respectively.

Example gather.chunks.json file


.. literalinclude:: example_filter_chunk_gather.json
    :language: javascript

.. note:: The chunk key ("$chunk.movie_fofn_id") from the original scatter operation is kept to record the entire chunking mechanism.

Step 3.
-------

Call the required gather tasks to create the final gathered files.

Gather tasks for “pbsmrtpipe.tasks.filter”

Gather #1

.. code-block:: bash

    $> pbtools-gather csv --chunk-key csv_id chunk.json -o file.csv

Gather #2

.. code-block:: bash

    $> pbtools-gather fofn chunk.json --chunk-key rgn_fofn_id -o region.fofn

(there's an assumption that chunk "keys" in the chunk file are hardcoded, i.e., hardcoded to pbsmrtpipe task definition to always look for "$chunk.rgn_fofn_id" in chunk.json file. This is probably not the best idea, but I don't know if passing the chunk keys within a task makes sense.)

Also, gather commandline tools should only have one output.

Step 4.
-------

At the workflow level, "Re-map" the original task outputs to the gathered task outputs. Update any
tasks in the BindingsGraph.


Chunking Operators
==================

The four steps are encoded in XML for the "pbsmrtipe.tasks.filter" example shown above and are called a **Chunking Operator**

Chunking Operators are registered under the **pbsmrtpipe.operators.** namespace.


.. literalinclude:: ../pbsmrtpipe/chunk_operators/operator_chunk_filter.xml
    :language: xml


Filter Subread Chunking Operator Example

.. literalinclude:: ../pbsmrtpipe/chunk_operators/operator_chunk_filter_subreads.xml
    :language: xml

.. note:: This is still in flux.