Identifiers Model
-----------------

IDs are used to globally resolve an object type identifier

- pbsmrtpipe.files.* FileType Ids (GFF, XML, JSON, REPORT, etc...) These are defined in pbcommand
- pbsmrtpipe.tasks.* Tasks Id defined in the task definition. Tasks/ToolContracts ids format of {namespace}.tasks.{task_id}
- pbsmrtpipe.options.* Workflow/Pipeline engine level options (e.g., nproc, cluster manager)
- pbsmrtpipe.task_options.* Option specific to a task (tasks can borrow or share task options by referencing by Id) {namespace}.tasks.{option_id}
- pbsmrtpipe.constants.* Workflow Internal constants
- pbmsrtpipe.pipelines.* Pipeline identifiers
- pbsmrtpipe.operators.* "Chunking" operators that operate on the bindings to produce new tasks
