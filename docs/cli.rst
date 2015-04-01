pbsmrtpipe Command Line Interface
---------------------------------


.. code-block:: bash

    $> pbsmrtpipe --help

.. literalinclude:: cli_pbsmrtpipe_help.sh
    :language: bash

.. code-block:: bash

    $> pbsmrtpipe pipeline --help

.. literalinclude:: cli_pbsmrtpipe_workflow_help.sh
    :language: bash

.. code-block:: bash

    $>pbsmrtpipe task --help

.. literalinclude:: cli_pbsmrtpipe_task_help.sh
    :language: bash

Show all registered pipeline templates
--------------------------------------

.. code-block:: bash

    $> pbsmrtpipe show-templates --help

.. literalinclude:: cli_pbsmrtpipe_show_workflow_templates_help.sh
    :language: bash


Show pipeline details by pipeline id

.. code-block:: bash

    $> pbsmrtpipe show-template-details pbsmrtpipe.pipelines.rs_resquencing_1

.. literalinclude:: cli_pbsmrtpipe_show_template_details_help.sh
    :language: bash


Show All registered Tasks

.. code-block:: bash

    $> pbsmrtpipe show-tasks

.. literalinclude:: cli_pbsmrtpipe_show_tasks_help.sh
    :language: bash

.. code-block:: bash

    $> pbsmrtpipe show-task-details pbsmrtpipe.tasks.dev_hello_world

.. literalinclude:: cli_pbsmrtpipe_show_task_details_hello_world_help.sh
    :language: bash

.. code-block:: bash

    $> pbsmrtpipe show-task-details pbsmrtpipe.tasks.align

To Show details of task. Use the **-o default_align_opts.xml** to write the default values to preset.xml file.

.. literalinclude:: cli_pbsmrtpipe_show_task_details_align_help.sh
    :language: bash

.. code-block:: bash

    $> pbsmrtpipe show-workflow-options

To list the workflow level options (use the **-o** option to write to default values to an XML file)

.. literalinclude:: cli_pbsmrtpipe_show_workflow_options_help.sh
    :language: bash
