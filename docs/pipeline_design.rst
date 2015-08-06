
Constructing Pipelines
----------------------

Fundamental Definitions

**Task** is function + metadata, such as input, output types, task options. Task input and outputs are referred to by positional index. See :doc:`task_model` for details.

**Binding** are explicit mappings of specific task output to specific task input by id. It's represented as tuple `(output, input)`.

**Bindings** list of **Binding**

**EntryPoint** is a special type of **Binding** that are yet to be resolved. This provides an interface of required files necessary to a pipeline. Entry Points ids are prefixed with "entry:" (e.g., "entry:e_01", "entry:e_01_input_xml")

**Pipeline** is **Bindings** + **EntryPoints**

**PipelineTemplate** is a **Pipeline** with default `Workflow Params` and default `Task` options. Both workflow options and task options can be empty.

**Workflow** is an internal term used to refer to a 'resolved' **PipelineTemplate** with specific files bound to each required **EntryPoint**

**WorkflowOptions** are workflow engine level options (i.e., non-domain related) options (e.g., nproc, nchunks) The are namespaced to **pbsmrtpipe.options.** and are only accessible within workflow engine; tasks don't have direct access to these options.

**TaskOptions** are global task options that are accessible in a task. Task options have universal identifiers (be mindful when naming!) and are namespaced to **pbsmrtpipe.task_options.**. (e.g., "pbsmrtpipe.task_options.filter_readscore")


.. note:: Protocol and Module are no longer used. These terms were overloaded and had ill-defined meaning in the previous code base.

Simple Bindings
---------------

Given two tasks with ids, `pbsmrtpipe.tasks.my_task` (with 1 inputs and 1 output) and `pbsmrtpipe.tasks.filter_task` (with 2 inputs and 2 outputs), a single binding of the first output of the first task to the second input of the second task can be defined as:


.. code-block:: python

    b = ("pbsmrtpipe.tasks.my_task.0", "pbsmrtpipe.tasks.filter_task.1")


Where the binding has the form: `{task_id}.{position index of input or output}`

A list of bindings:

.. code-block:: python

    bindings = [
        ("pbsmrtpipe.tasks.my_task.0", "pbsmrtpipe.tasks.filter_task.1"),
        ("pbsmrtpipe.tasks.my_task.0", "pbsmrtpipe.tasks.mapping_task.0")
    ]

An **EntryPoint** is a Binding-esque symbol that is added to the bindings list which will be resolved/bound at a later date.

.. code-block:: python

    bindings = [
        ("pbsmrtpipe.tasks.my_task.0", "pbsmrtpipe.tasks.filter_task.1"), 
        ("pbsmrtpipe.tasks.my_task.0", "pbsmrtpipe.tasks.mapping_task.0")]
    # Now add a EntryPoint
    bindings.append(("entry:entry_01", "pbsmrtpipe.tasks.my_tasks.0"))

Similar to the bindings, a **EntryPoint** has a `entry:{my entry_id}` format.

A list of **Bindings** with an **EntryPoint** is core component of creating a **Pipeline** and **PipelineTemplate**.



Basic Task Model
----------------

.. image:: http://s3.amazonaws.com/media.pacb.com/pbsmrtpipe_docs_images/task_model.001.jpg


Bindings Example
----------------


.. image:: http://s3.amazonaws.com/media.pacb.com/pbsmrtpipe_docs_images/task_model.002.jpg

Bindings from pipeline are shown as b2, b3. The bindings would have the form:

.. code-block:: python

    bindings = [
        ("task_1.0", "task_2.1"), # b2
        ("task_1.0", "task_3.0")  # b3
        ]


To expose an interface of required files, entry points need to be defined. Select entry points are labeled as b0, b1 in figure below.

.. image:: http://s3.amazonaws.com/media.pacb.com/pbsmrtpipe_docs_images/task_model.003.jpg

.. code-block:: python

    bindings = [
        ("entry:e_0", "task_1.0"), # b1
        ("entry:e_0", "task_2.0"),
        ("entry:e_1", "task_1.1"),
        ("entry:e_2", "task_1.2"),
        ("entry:e_0", "task_2.0"), # b0
        ("task_1.0", "task_2.1"),  # b2
        ("task_1.0", "task_3.0")   # b3
        ]

**Entry points** are the exposed interface for files that will be bound during creation. The entry point symbols defined in pipeline templates (discussed later) have the form "entry:e_01" while the commandline interface to bind file paths to of entry points defined in a pipeline template is given by **-e "e_01:/path/to/file.txt" commandline args.


Advanced Bindings
-----------------

For advanced bindings (i.e., binding an input to multiple instances of a task), there's an advanced binding form.

`{task_id}.{instance of task as an int}.{position index of input of output}`

This provides a terse mini-language and simple mechanism to explicit describe task input/output bindings to other task inputs/outputs.


Defining Pipeline Templates
===========================

A task group or Workflow is a static graph of Bindings + well defined EntryPoints. EntryPoints can be bound to files at runtime. Effectively, this is a pipeline template specification that can be created by any language.


.. note:: Currently pipelines are defined in **pbsmrtpipe.pb_pipelines**. Python code is used to compose the bindings and and entry points. As stabilization of pipelines is reached, registered pipelines defined in **pbsmrtpipe.pb_pipelines** will be converted to a serialized form (XML or JSON). This allows for better cross language capability, specifically for the use in Portal services. In summary, python is used an intermediate format to produce a static representation of the pipeline template.


.. code-block:: python

    from pbsmrtpipe.core import register_pipeline
    from pbsmrtpipe.constants import to_pipeline_ns # this util func returns the pipeline id to_pipeline_ns("my_id") -> "pbsmrtpipe.pipelines.my_id"

    @register_pipeline(to_pipeline_ns("dev_local"), "Dev Local Hello Workflow Pipeline")
    def get_dev_local_pipeline():
        """Simple example pipeline"""
        b1 = [('entry:e_01', 'pbsmrtpipe.tasks.dev_hello_world.0')]

        b2 = [('pbsmrtpipe.tasks.dev_hello_world.0', 'pbsmrtpipe.tasks.dev_hello_worlder.0'),
              ('pbsmrtpipe.tasks.dev_hello_world.0', 'pbsmrtpipe.tasks.dev_hello_garfield.0')]

        return b1 + b2


    @register_pipeline(to_pipeline_ns("dev_dist"), "Dev Hello Distributed Workflow Pipeline")
    def get_dist_dev_pipeline():
        """Simple distributed example pipeline"""
        bs = get_dev_local_pipeline()

        b2 = [('pbsmrtpipe.tasks.dev_hello_world.0', 'pbsmrtpipe.tasks.dev_hello_distributed.0'),
              ('pbsmrtpipe.tasks.dev_hello_worlder.0', 'pbsmrtpipe.tasks.dev_hello_distributed.1')]

        return bs + b2

Bindings in other pipelines can be referenced in other pipelines. This allows users to compose pipelines by building off of other pipelines without changing those pipelines. Similarly to the **@register_task** decorator, the **register_pipeline** requires an Id (e.g., "pbsmrtpipe.pipelines.my_pipeline") and a display name. The pipeline description is taken from the function docstring.

Once the pipelines are defined in **pbsmrtpipe.pb_pipelines**, they can be referenced in pipeline templates by id.

An example of XML representation of **pbsmrtpipe.pipelines.rs_filter_1**


.. literalinclude:: pipeline_rs_filter_1.xml
    :language: xml


.. note:: Currently, the pipelines and registered tasks are stored within pbsmrtpipe python package to minimize the number of moving pieces. However, the tasks and pipelines might be refactored into a separate python package to keep configuration separate from workflow engine "kernel".

Accessing Pipeline Templates by ID
----------------------------------

An pipeline template `Id` can be referenced within a template.


.. literalinclude:: ../testkit-data/fetch_01/workflow_id.xml
    :language: xml


Simple example of running from the Commandline
----------------------------------------------

Entry points can be provided at the commandline using the **-e** option


To get the entry point symbols for a specific pipeline, use the **show-template-details** subparser option.


.. code-block:: bash

    $> pbsmrtpipe show-template-details pbsmrtpipe.pipelines.rs_filter_1
    Pipeline id   : pbsmrtpipe.pipelines.rs_filter_1
    Pipeline name : RS_Filter.1
    Description   : Description of RS Filter
    Entry points : 1
    ********************
    entry:eid_input_xml

    Bindings     : 11
    ********************
             pbsmrtpipe.tasks.input_xml_to_fofn.0 -> pbsmrtpipe.tasks.movie_overview_report.0
             pbsmrtpipe.tasks.input_xml_to_fofn.0 -> pbsmrtpipe.tasks.adapter_report.0
                        pbsmrtpipe.tasks.filter.0 -> pbsmrtpipe.tasks.filter_subreads.1
                        pbsmrtpipe.tasks.filter.0 -> pbsmrtpipe.tasks.filter_subread_summary.0
                        pbsmrtpipe.tasks.filter.1 -> pbsmrtpipe.tasks.filter_report.0
        pbsmrtpipe.tasks.filter_subread_summary.0 -> pbsmrtpipe.tasks.filter_subread_report.0
                        pbsmrtpipe.tasks.filter.1 -> pbsmrtpipe.tasks.loading_report.0
             pbsmrtpipe.tasks.input_xml_to_fofn.0 -> pbsmrtpipe.tasks.filter.0
             pbsmrtpipe.tasks.input_xml_to_fofn.1 -> pbsmrtpipe.tasks.filter.1
             pbsmrtpipe.tasks.input_xml_to_fofn.0 -> pbsmrtpipe.tasks.filter_subreads.0
                              entry:eid_input_xml -> pbsmrtpipe.tasks.input_xml_to_fofn.0


From the commandline using `pipeline` subparser argument to run a **PipelineTemplate**:

.. code-block:: bash

    $> pbsmrtpipe pipeline my_pipeline.xml -e "eid_input_xml:/path/to/input.xml" --preset-xml /path/to/my_preset.xml --output-dir=/path/to/output


See :doc:`cli` for more details and examples.

Explicit Bindings in Pipeline Template
--------------------------------------
Alternatively to referring to pipeline templates by id, explicit bindings can be provided in the XML to create **custom** pipelines.

.. literalinclude:: custom_wf_template.xml
   :language: xml

Pipeline Presets
----------------

Workflow presets allow users to define groups of task options to overwrite options defined in Workflow Templates. The preset can override both workflow level options as well as task level options. Presets can also be used in testkit.cfg.

.. literalinclude:: ../testkit-data/fetch_01/preset.xml
    :language: xml

.. note:: You can use the **show-task-details** with the `-o preset.xml` to write the default tasks options to preset.xml file. Modify as necessary.

Loading of Presets and the "RC" Preset
--------------------------------------

pbsmrtpipe supports an 'RC' file, which acts as a base preset that can be applied to general commandline invocation of pbsmrtpipe exe. This can be defined by setting the "PB_SMRTPIPE_XML_PRESET" to point to preset.xml file.


Loading Order

- PB_SMRTPIPE_XML_PRESET (if ENV var "PB_SMRTPIPE_XML_PRESET" defined as path to XML file exists)
- pipeline XML
- preset XML (if provided)
