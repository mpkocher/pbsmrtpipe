Pbsmrtpipe TestKit Framework
============================

The core functionality of pysiv for running integration tests (using RS-era smrtpipe.py) is now in pbsmrtpipe. It resides in **pbsmsrtpipe.testkit.*** and the **monkey_patch** decorator is in **pbsmrtpipe.testkit.core**. Both individual tasks and well as pipelines can be run using testkit (although we mostly just use the latter for production testing).

The main change is the ability to load tests from **any** python module (e.g., pysiv2.core.test_zero). It should be straightforward to expand the **monkey_patch** decorator to define your own test cases building assertions.

Tests are driven by configuration files, which by default are usually in "INI
file" format.  An example:

.. literalinclude:: ../testkit-data/dev_01/testkit.cfg

The pipeline to run is specified by a separate XML file (``pipeline_xml``), as
are any preset task options.  Entry points are specified as entry ID/path
pairs.

For pipelines, an alternative is to use JSON format, which is more flexible but
not as lightweight.

.. literalinclude:: testkit-data/dev_local_fasta_chunk/testkit.json

As an alternative to a workflow XML, you can instead specify ``pipelineId``.

Currently, there are dev integration tests in **testkit-data** root level directory that only dependend on pbcore. These are useful to running example jobs and understanding how pipelines are constructed.  In our Perforce repos, most tests live in ``//depot/software/smrtanalysis/siv/testkit-jobs/sa3_pipelines``, which has multiple examples for all of our production pipelines.  Each test should have its own subdirectory, preferably grouped by application type.  Please note that although pbsmrtpipe pipelines will often work with "bare" BAM or FASTA files as inputs instead of PacBio DataSet XML files, testkit jobs should always use the corresponding DataSet XML as entry points, to ensure compatibility with SMRT Link services (see below).


Defining Test Cases
-------------------

Similarly to pysiv, tests cases can inherit from **TestBase**.

Here's a example taken from **test_resources**, which checks if the core job resources were created correctly. The job directory can be accessed via **self.job_dir**.

.. literalinclude:: ../pbsmrtpipe/testkit/core/test_resources.py
    :language: py


Running Testkit
---------------

**pbsmrtpipe** testkit jobs may be run in one of two ways:

  1. directly on the command line, using ``pbtestkit-runner``.
  2. indirectly via SMRT Link services, using ``pbtestkit-service-runner``; this requires a separate working SMRT Link server.

In addition, tests may be run in parallel using the corresponding "multirunner" commands (see below).

For command-line-only testing, ``pbtestkit-runner testkit.cfg`` is the minimal
command required, which will use the default workflow options specified in the
``preset.xml``.  This can be further modified by the ``--force-distributed``
and ``--local-only`` options which control use of a queuing system to dispatch
tasks, and by ``--force-chunk-mode`` and ``--disable-chunk-mode`` to toggle
chunking.  (We recommend that you try all possible modes when developing a
pipeline, but simple functional tests are often quicker to run locally and
unchunked.)  If you have run the testkit job already and just want to re-run the
test suite with modifications, add ``--only-tests``.

.. argparse::
   :module: pbsmrtpipe.testkit.runner
   :func: get_parser
   :prog: pbtestkit-runner


Testing via services is slightly less flexible but has the added advantage of
making the job visible in SMRT Link, including all reports and plots, plus
checking for functionality in the server environment.  Note that services
absolutely requires that all entry points be DataSet XML files.
The equivalent command is slightly more complicated::

  $ pbtestkit-service-runner --host smrtlink-bihourly --port 8081 testkit.cfg

Workflow options (but not task options) will be ignored in this mode in favor of whatever the server defaults are.  To re-run tests only, you need to first
determine the integer ID of the smrtlink job, then add ``--only-tests <ID>``
to the arguments.

.. argparse::
   :module: pbsmrtpipe.testkit.service_runner
   :func: get_parser
   :prog: pbtestkit-service-runner


For running suites of related tests, the multirunner commands take an FOFN
pointing to the testkit configs to run::

  $ pbtestkit-multirunner mapping_tests.txt --nworkers 8
  $ pbtestkit-service-multirunner mapping_tests.txt --nworkers 8 --host smrtlink-bihourly --port 8081

``pbtestkit-multirunner`` also supports the same arguments to control job
distribution and chunking.  Note however that test-only mode is not supported
by the multirunners.

.. argparse::
   :module: pbsmrtpipe.testkit.multirunner
   :func: get_parser
   :prog: pbtestkit-multirunner


.. argparse::
   :module: pbsmrtpipe.testkit.service_multirunner
   :func: get_parser
   :prog: pbtestkit-service-multirunner

See the CLI docs for details :doc:`cli`.
