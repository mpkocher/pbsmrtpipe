Installing
==========

pbsmrtpipe's only major python dependencies are pbcommand and pbcore. Installing should be painless to install locally for testing, or installing on the remote system.

.. note:: graphviz is external subprocess dependency. You *must* have dot in your path.

Create a new virtualenv (use > 11.6)

.. code-block:: bash

    # this will use the default
    $> python $HOME/path/to/virtualenv.py /path/to/myvenv

As a bootstrapping step, it's good to just pull virtualenv directly
and store it locally. Then call virtualenv directly. This will use the
bundled pip and won't pull anything over the wire.

.. code-block:: bash

    $> python $HOME/bin/virtualenv-1.11.6/virtualenv.py /path/to/myvenv

Also, add a download cache to make pip installing package speedy.

create a ~/.pip/pip.conf with

.. code-block:: bash

    $> mkocher@login14-biofx01:.pip$ cat /home/UNIXHOME/mkocher/.pip/pip.conf
    [install]
    download-cache = ~/.pip/download_cache


Installing requirements
-----------------------

Grab pbsmrtpipe

.. code-block:: bash

    $> git clone https://github.com/PacificBiosciences/pbsmrtpipe.git
    $> cd pbsmrtpipe
    $> # this will install pbcore and pbcommand
    $> pip install -r PB_REQUIREMENTS.txt # this will install pbcommand and pbcore from master on github
    $> pip install -r REQUIREMENTS.txt # will install from pypi
    $> pip install .
    $> pbsmrtpipe --help


Running unittests
-----------------

.. code-block:: bash

    $> make test-unit

Run the unittests (if you're on the pacbio cluster, a test job will be
submitted to the cluster, otherwise the cluster tests will be skipped). This will take a few minutes.


Running Integration Tests
-------------------------

Running the integration tests using pbsmrtpipe testkit framework. See `pbsmrtpipe/testkit-data/` after the tests have completed.

.. code-block:: bash

    $> make test-dev
    $> # which integration tests. See the example output in pbsmrtpipe/test-data/*


Running the entire Test Suite
-----------------------------

.. code-block:: bash

    $> make test-suite
    $> # which just run several unittests and integration tests.


.. note:: Before every pull request to pbsmrtpipe, this should be run.


Ready to start!