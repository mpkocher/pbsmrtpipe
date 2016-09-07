Pbsmrtpipe TestKit Framework
============================

The core functionality of pysiv for running integration tests (using RS-era smrtpipe.py) is now in pbsmrtpipe. It resides in **pbsmsrtpipe.testkit.*** and the **monkey_patch** decorator is in **pbsmrtpipe.testkit.core**. Both, individual tasks and well as pipelines can be run using testkit.

The main change is the ability to load tests from **any** python module (e.g., pysiv2.core.test_zero). It should be straightforward to expand **monkey_patch** decorator to define your own test cases building assertions.


.. literalinclude:: ../testkit-data/dev_01/testkit.cfg


Currently, there are dev integration tests in **testkit-data** root level directory that only dependend on pbcore. These are useful to running example jobs and understanding how pipelines are constructed.


Defining Test Cases
-------------------

Similarly to pysiv, tests cases can inherit from **TestBase**.

Here's a example taken from **test_resources**, which checks if the core job resources were created correctly. The job directory can be accessed via **self.job_dir**.

.. literalinclude:: ../pbsmrtpipe/testkit/core/test_resources.py
    :language: py


Running Testkit
---------------

The **pbtestkit-runner** exe can be used to run tests driven from `testkit.cfg` files.


.. argparse::
   :module: pbsmrtpipe.testkit.runner
   :func: get_parser
   :prog: pbtestkit-runner



See the CLI docs for details :doc:`cli`.