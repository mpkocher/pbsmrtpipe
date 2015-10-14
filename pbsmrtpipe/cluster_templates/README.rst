Cluster Templates
-----------------

Cluster templates can be specified by schedulers (e.g., SGE, PBS).

The spec requires two files:

- stop.tmpl
- start.tmpl

Each template must specify several parameters.

.. code-block:: python

    cmd = {'CMD': command, 'JOB_ID': jobId, 'STDOUT_FILE': stdoutFile,
           'STDERR_FILE': stderrFile, 'EXTRAS': extras, 'NPROC': nproc}



Example 'interactive' template.

::
     qsub -S /bin/bash -sync y -V -q secondary -N ${JOB_ID} -o ${STDOUT_FILE} -e ${STDERR_FILE} -pe smp ${NPROC} ${CMD}

