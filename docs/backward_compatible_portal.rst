Backward Compatible with existing SMRTPortal model
==================================================

Migration model from P module model


Existing Portal Protocol Model converted to use Bindings
--------------------------------------------------------

The existing P module model is extremely unprincipled and has created large amounts of friction in generating custom workflows, or modifing existing workflows. Furthermore, the Portal Protocol model that has been built on top of the P Module adds yet another layer to complicate matter. More importantly, smrtpipe does not fundamentally understand the concept of a Protocol and you can not run `RS_Resequencing.1` from the commandline. This is been a constant source of testing headaches.

By removing the existing P_module model in favor of explicit `Bindings`, workflows can be referenced by id (which pbsmrtpipe will lookup the corresponding workflow in a global registry). To deal with the disabling and other logic that has been embedded in the existing P modules, there still needs to be a compatibility layer to abstract the logic defined within the P modules.

A Protocol is an abstraction that can disabled task (or task groups) and generate a static workflow. The workflow produced is not a well-defined composable unit of work that can be combined with other task groups or workflows. This only exists for legacy purposes and should be considered the not supported after 2.3. 

Internally, the Protocol id is looked up via a function with the signature `f(workflow_settings)`. The workflow settings are then used to generate resolve/create the necessary static workflow by building up bindings. This should capture the logic of that currently exists in many of the P Modules validate settings methods. (Manually disabling tasks by settings the task name = False in the existing XML is not supported. All modifications to the graph must be handled in the protocol function)


In the new model, Portal `Protocols` can be run from both the commandline and via Portal. The P_Module scoped parameters are only used as simple preset mechanism to set default values. Similar to how a Protocol is internally looked up by id, the parameter options are looked up. The existing sub-Protocol mechanism can still exist, however, it can only be used as a 'Preset' (i.e., a grouping of task options and display options).


.. code-block:: python

    @register_protocol("RS_Subreads.1")
    def my_protocol(workflow_settings):
        # do stuff to sortout if the Barcodes is selected, or Control, etc,...
        # this is a giant pain in the ass to disable tasks, or task groups and generate a static graph
        # the exact return type is a graph with explicit bindings
        return {}

In the 2.3 timeframe, Protocols can be run from both the commandline and via Portal. The existing P_Module scoped parameters (which are 'compiled', or 'resolved' by Portal are only used as simple preset mechanism to set default values. Similar to how a Protocol is internally looked up by id, the parameter options are looked up. The existing sub-Protocol mechanism can still exist, however, it can only be used as a 'Preset' (i.e., a grouping of task options and display options, the graph will remain fixes).

For internal protocols (e.g, Standard_Standard.1), this model will work nicely. Most of the graphs internally are only changing groups of task options, not changing the desired workflow graph.


.. note::
    The inputs for the Protocols is **always** using movies as the fundamental input using the existing input.xml model. This should be replaced with the new `DataSet` abstraction.*


Registering WorkflowTemplates
-----------------------------

Similar to how protocols are registered, a `Workflow` can be defined with a global id and referenced in a similar style to the Protocols. The aim of the `Workflow` is create an abstraction that is language agnostic and extendable in a well-defined manner.

.. *Not Sure where and how the workflow registry gets created. This should aim to be language agnostic.*

Example of registering a **WorkflowTemplate**.

.. literalinclude:: wf_example.py
   :language: python