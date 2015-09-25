Configuration
=============

General pipeline engine level options details and default values can be emitted to stdout using:

.. code-block:: bash

    $> pbsmrtpipe pbsmrtpipe show-workflow-options


.. literalinclude:: cli_pbsmrtpipe_show_workflow_options_help.sh
    :language: bash


The pipeline level options can be set in the **options** section of the preset (or workflow) XML file.

.. literalinclude:: wf_preset_01.xml
    :language: xml


To have a global base level preset applied to all pipeline executions, create a preset.xml file, then export the ENV variable "PB_SMRTPIPE_XML_PRESET"

Example

.. literalinclude:: example_base_preset.xml
    :language: xml

Cluster Manager
---------------

The cluster manager configuration (**pbsmrtpipe.options.cluster_manager**) is often the first item that needs to be configured to enable distributed computing of your pipeline.


The cluster manager configuration points to a directory of two cluster templates, **start.tmpl** and **stop.tmpl**.

The **start.tmpl** is the most important. It must contain a **single** bash line that exposes several template variables that will be replaced. This will often have the queue name that jobs will be submitted to.

.. literalinclude:: example_cluster_start_tmpl.sh


Required Template Variables

- NPROC the number of processors/slots to use
- JOB_ID This will adhere to the
- STDOUT_FILE the qsub level stdout
- STDERR_FILE the qsub level stderr
- CMD this will the absolute path to the command to be executed


.. note:: The cluster manager configuration can point to a python package which contains the cluster templates, or an absolute path to the cluster templates. The python package model is used primarily for internal purposes.


Stop Template example. Only JOB_ID is required.

.. literalinclude:: example_cluster_stop_tmpl.sh



Environment Handling
--------------------

By default pbsmrtpipe will use the standard *nix model of inheriting the parent environment to find executables in your path.

The simplest model is to create a **setup.sh** to explicitly set your desired path.

A simple example:

.. literalinclude:: example_setup_sh_01.sh
    :language: bash


If you're building several tools, you may need to wrap each tool to keep each tool wrapped in it's own environment settings.
