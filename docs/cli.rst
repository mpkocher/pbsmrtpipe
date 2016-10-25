Command Line Interface Tools
----------------------------

Core Pbsmrtpipe executable
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. argparse::
   :module: pbsmrtpipe.cli
   :func: get_parser
   :prog: pbsmrtpipe


pbtools-runner
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


Testkit Multi-Runner
~~~~~~~~~~~~~~~~~~~~

.. argparse::
   :module: pbsmrtpipe.testkit.multirunner
   :func: get_parser
   :prog: pbtestkit-multirunner


Resource RST Pipeline Generation from Resolved Pipeline JSON
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Example:

.. code-block:: bash

   $> python -m pbsmrtpipe.tools.resources_to_rst /path/to/resolved-pipeline-templates -o pipeline-docs/ --title "PacBio Custom Pipelines" --debug
   $> cd pipeline-docs && make clean html
   # pipelines docs will be accessible in _build/html


.. argparse::
   :module: pbsmrtpipe.tools.resources_to_rst
   :func: get_parser
   :prog: python -m pbsmrtpipe.tools.resources_to_rst
