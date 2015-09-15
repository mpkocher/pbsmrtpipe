TestKit (Butler 2.0)
====================

The core functionality of Pysiv's butler for running integration tests is now in pbsmrtpipe. It resides in **pbsmsrtpipe.testkit.*** and the **monkey_patch** decorator is in **pbsmrtpipe.testkit.core**. Both, individual tasks and well as pipelines can be run using testkit.

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

The **pbtestkit-runner** is replacement for **siv_butler.py**.

.. code-block:: bash

    (pbsmrtpipe_test)pbsmrtpipe $> pbtestkit-runner --help
    usage: pbtestkit-runner [-h] [-v] [--debug] [--log-file LOG_FILE]
                            [--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                            [--only-tests] [--force-distributed | --local-only]
                            [--force-chunk-mode | --disable-chunk-mode]
                            testkit_cfg

    Testkit Tool to run pbsmrtpipe jobs.

    positional arguments:
      testkit_cfg           Path to testkit.cfg file.

    optional arguments:
      -h, --help            show this help message and exit
      -v, --version         show program's version number and exit
      --debug               Send logging info to stdout. (default: False)
      --log-file LOG_FILE   Path to log file (default: None)
      --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                            Log LEVEL (default: INFO)
      --only-tests          Only run the tests. (default: False)
      --force-distributed   Override XML settings to enable distributed mode (if
                            cluster manager is provided) (default: None)
      --local-only          Override XML settings to disable distributed mode. All
                            Task will be submitted to Michaels-MacBook-Pro.local
                            (default: None)
      --force-chunk-mode    Override to enable Chunk mode (default: None)
      --disable-chunk-mode  Override to disable Chunk mode (default: None)
