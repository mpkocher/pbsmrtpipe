Identifiers Model
-----------------

IDs are used to globally resolve an object type identifier

- pbsmrtpipe.files.* FileType Ids (GFF, XML, JSON, REPORT, etc...)
- pbsmrtpipe.tasks.* Tasks Id defined in the task definition
- pbsmrtpipe.options.* Workflow level options (e.g., nproc, cluster manager)
- pbsmrtpipe.task_options.* Option specific to a task (tasks can borrow or share task options by referencing by Id)
- pbsmrtpipe.constants.* Workflow Internal constants
- pbmsrtpipe.pipelines.* Pipeline identifiers
- pbsmrtpipe.operators.* "Chunking" operators that operate on the bindings to produce new tasks
