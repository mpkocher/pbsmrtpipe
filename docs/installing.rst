Installing
==========

pbsmrtpipe only major python dependencies are pbcommand and pbcore. Installing should be painless to install locally for testing, or installing on the remote system.

.. note:: graphviz is external subprocess dependency. You must have dot in your path.

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

This stuff is sometimes touchy to install, so installing it explicitly here.

.. code-block:: bash

    $> pip install numpy
    $> pip install cython
    $> pip install h5py
    $> pip install nose

Grab pbsmrtpipe

.. code-block:: bash

    $> git clone https://github.com/PacificBiosciences/pbsmrtpipe.git

    $> cd pbsmrtpipe

    $> # this will install pbcore andn pbcommand
    $> pip install -r PB_REQUIREMENTS.txt # this will install pbcommand and pbcore from master on github
    $> pip install -r REQUIREMENTS.txt # will install from pypi
    $> pip install .

    $> # for nosetests
    $> pip install -r REQUIREMENTS_DEV.txt

Run the tests (if you're on the pacbio cluster, a test job will be
submitted to the cluster, otherwise the cluster tests will be skipped). This will take a few minutes.

.. code-block:: bash

    $> make test-suite
    $> # which just run several unittests and integration tests. See the example output in test-data/*

Ready to start!

.. code-block:: bash

    $> pbsmrtpipe --help

