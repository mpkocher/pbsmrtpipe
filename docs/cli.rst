pbsmrtpipe Command Line Interface
---------------------------------

Core Pbsmrtpipe executable
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. argparse::
   :module: pbsmrtpipe.cli
   :func: get_parser
   :prog: pbsmrtpipe


Chunking Tools
~~~~~~~~~~~~~~

Runner

.. argparse::
   :module: pbsmrtpipe.tools.runner
   :func: get_main_parser
   :prog: pbtools-runner



Testkit Runner
~~~~~~~~~~~~~~


.. argparse::
   :module: pbsmrtpipe.testkit.runner
   :func: get_parser
   :prog: pbtestkit-runner
