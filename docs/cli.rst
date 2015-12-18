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


Chunking/Scatter

.. argparse::
   :module: pbsmrtpipe.tools.chunker
   :func: get_parser
   :prog: pbtools-chunker


Gather

.. argparse::
   :module: pbsmrtpipe.tools.gather
   :func: get_parser
   :prog: pbtools-gather


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
