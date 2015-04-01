For task chunking, what do I need to define?
--------------------------------------------

1. Define a scatter task via **@register_scatter_task** decorator. The scatter task **must** have the same input signature as the task you want to chunk. The output type signature will be a single **FileTypes.CHUNK**.
2. Write a scatter commandline exe that produces a scatter.chunk.json file.
3. define a gather task via **@register_gather_task** decorator. The gather task has one input type **FileTypes.CHUNK** and emits a **single** file of any type.
4. Write a gather exe that takes a gather.chunk.json file and emits a single file.
5. Define an Chunked Operator XML in **pbsmrtpipe.chunked_operators**


Chunk.json spec
---------------

Add an example doc here.