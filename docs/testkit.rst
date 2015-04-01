TestKit (Butler 2.0)
====================

The core functionality of Pysiv's butler for running integration tests is now in pbsmrtpipe. It resides in **pbsmsrtpipe.testkit.*** and the **monkey_patch** decorator is in **pbsmrtpipe.testkit.core**. Both, individual tasks and well as pipelines can be run using testkit.

The main change is the ability to load tests from **any** python module (e.g., pysiv2.core.test_zero). It should be straightforward to expand **monkey_patch** decorator to define your own test cases building assertions.


.. literalinclude:: ../testkit-data/fetch_01/testkit.cfg


Currently, there are dev integration tests in **testkit-data** root level directory. This will eventually moved into pysiv2.


Defining Test Cases
-------------------

Similarly to pysiv, tests cases can inherit from **TestBase**.

Here's a example taken from **test_resources**, which checks if the core job resources were created correctly. The job directory can be accessed via **self.job_dir**.

.. literalinclude:: ../pbsmrtpipe/testkit/core/test_resources.py
    :language: py


Running Testkit
---------------

The **pbtestkit-runner** is replacement for **siv_butler.py**.

.. code-block:: bash

    (dev_pbsmrtpipe_test)pbsmrtpipe $> pbtestkit-runner --help
    usage: pbtestkit-runner [-h] [-v] [--debug] [--no-mock | --mock]
                            [--only-tests]
                            butler_cfg

    Testkit Tool to run pbsmrtpipe jobs.

    positional arguments:
      butler_cfg     Path to butler.cfg file.

    optional arguments:
      -h, --help     show this help message and exit
      -v, --version  show program's version number and exit
      --debug        Debug to stdout.
      --no-mock      Override testkit.cfg. Disable mock mode.
      --mock         Override testkit.cfg. Enable mock mode.
      --only-tests   Only run the tests.
