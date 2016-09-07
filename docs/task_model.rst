Tool Contract and Defining Tasks
--------------------------------

At a high level, pbsmrtpipe leverages the Tool Contract interface defined using the pbcommand_ to call external processes, such as blasr, or pbalign.

This language agnostic interface defines fundamental metadata about the task, such as the options, input and output types. The base Tool Contract data model is extended by Scatter/Chunking Tool Contract type as well as a Gather Tool Contract type to enable chunking of files to address individual task scalability.

See the pbcommand toolcontract_ docs for examples and more details.

For Scatter/Chunking and Gather tasks, please see the pbcommand_ documentation. Further examples can be seen in pbcoretools_.

.. _pbcommand: https://github.com/PacificBiosciences/pbcommand#pbcommand-high-level-overview
.. _toolcontract: http://pbcommand.readthedocs.io/en/latest/commandline_interface.html#details-of-tool-contract
.. _pbcoretools: https://github.com/PacificBiosciences/pbcoretools/tree/master/pbcoretools/tasks