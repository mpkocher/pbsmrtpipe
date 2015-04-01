Task Model
----------

Tasks are written in python, task options are defined using the jsonschema spec. Tasks have a simple format as well as an advanced "Dependency Injection" model for defining dependencies between nproc, task type, task options.

.. note:: As of 0.6.0, there is a new class based model for defining tasks. The old **@register_task** decorator is still supported, but all new tasks should be defined in the new class based style.


Defining Task Options
---------------------

Task options are defined using jsonschema format (similarly, the workflow options are defined using jsonschema).


The jsonschema spec supports several fundamental datatypes, such as string, number. None-able values (i.e., `Option[String]`) can be represented by a list **["null", "string"]**.

All tasks **must** define a default value, display name and description. The display information is accessible from the commandline and Portal2 UI. Using jsonschema allows options to be validated consistently at both the workflow level and the (javascript) UI and (scala) Services level.

Using previous dev task, the raw jsonschema is:

.. literalinclude:: example_json_schema_dev.json
    :language: js

pbsmrtpipe supports a short hand for defining options.


.. code-block:: python

    import pbsmrtpipe.schema_opt_utils as OP
    # generate a task option id
    oid = OP.to_opt_id('dev.hello_message')
    # generate a schema
    s = OP.to_option_schema(oid, "string", 'Hello Message', "Hello Message for dev Task.", "Default Message")
    return {oid: s}

The **to_option_schema** has signature:

.. code-block:: python

    OP.to_option_schema(OPTION_ID, OPTION_TYPE_OR_TYPES, DISPLAY_NAME, DESCRIPTION, DEFAULT VALUE)


To create an optional value (i.e., `Option[String]`):

.. code-block:: python

    s = OP.to_option_schema(oid, ("null", "string"), 'Hello Message', "Hello Message for dev Task.", None)


Defining Simple Task
--------------------

Task are written in python and registered by inheriting from the **MetaTaskBase** class from **pbsmrtpipe.core**.


The **MetaTaskBase** class requires you to defined several class variables and implement a **staticmethod** called **to_cmd**. The function must have the function signature (input_files, output_files, task_options, nproc, resource). The class inheritance is syntactic sugar for creating a `MetaTask` instance and registering the meta task in a globally accessible registry by id. Stylistically, this somewhat similar to the model used in Django, or SqlAlchemy, which use python classes as config.

The **to_cmd** staticmethod must have the signature (input files, output files, resolve_options, nproc, resources) and emmit a list of commandline string.

Details

- **TASK_ID** This the global unique identifier for the task (it must begin with "pbsmrtpipe.tasks")
- **NAME** is the task display name
- **VERSION** a semantic style version string
- **TASK_TYPE** (DI list model) is an enum of pbsmrtpipe.constants.local_task or pbsmrtpipe.constants.distributed_task. Referenced by class constants TaskTypes.LOCAL and TaskTypes.DISTRIBUTED.
- **INPUT_FILE_TYPES** are defined by a list of tuples (**FileType**, binding label, description) for example, [(**FileTypes.FASTA**, "target_fasta", "A Target Fasta file"), (**FileTypes.FASTA**, "query_fasta", "A query Fasta file")]
- **OUTPUT_FILE_TYPES** see **INPUT_FILE_TYPES**
- **OUTPUT_FILE_NAMES** (Optional) allows you to specify the name of each output file. The format is a list of tuples (base name, extension)
- **SCHEMA_OPTIONS** (DI list model) are task options as a dict of {option_id:jsonschema}
- **NPROC** (DI list model) is the number of processors used.
- **RESOURCE_TYPES** (Optional) are files or dirs are 'resources' (e.g, ResourceTypes.TMP_FILE, ResourceTypes.TMP_DIR, ResourceTypes.LOG_FILE) that can be accessed positionally into the list. For example, in **to_cmd**, you can reference **resources[0]**.


A simple task which has one option and has one TXT file type as input, one TXT file type as output and one task option.

.. literalinclude:: ../pbsmrtpipe/pb_tasks/dev_simple.py
    :language: py


Advanced Tasks are can be defined in :doc:`task_model_advanced`.