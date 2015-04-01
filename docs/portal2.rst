Workflow Registry
-----------------


Internal to the workflow there's a `registry` of files.

These will be exposed via services but also accessible from commandline usage of pbsmrtpipe.

::

    REGISTRY_ROOT/
        /file-registry/
        /task-registry/
        /workflow-templates-registry/
        /workflow-template-view-rules/
        /workflow-presets/



The **file-registry** directory lists the files types and metadata about the file types the workflow understands

Similarly, the **tasks-registry** contains a list of tasks that are understood by the workflow.

The **workflow-templates-registry** are canned workflows that can be referenced by unique id.

The **workflow-presets** directory contains presets (groups of task and workflow level parameters). Each preset references an specific **workflow-template-registry** by id.

The **workflow-template-view-rules** contains specific display related data.


Building a Workflow Template
----------------------------

A `WorkflowTemplate` contains the bindings (i.e., task inputs/output mappings), explicit bindings and any required defaults task options to be set.


.. literalinclude:: custom_wf_template.xml
    :language: xml


Building Workflow Presets
-------------------------

A preset is a reference to an existing workflow template (by id) and overriding of workflow level and task parameters.

This is a very simple merge of task and workflow level options by `id`.


.. literalinclude:: wf_preset_01.xml
    :language: xml



Workflow Template View Rules
----------------------------

A workflow template view rules (WFTVR) represents that view information of Task Options and Workflow Options. A WFTVR **must** reference a workflow template by id.

A preset represents a `WorkflowTemplate` and default `Workflow` options as well as `Task` options.

.. literalinclude:: wft_example_01.xml
    :language: xml


.. note:: 
    It might be a good idea to separate out the display view of options and workflow/task option default groups.
