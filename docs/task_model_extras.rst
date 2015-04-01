Job Directory Structure
-----------------------

Job directory Structure. Details and examples of the files are shown below.

- root-job-dir/
    - stdout (pbsmrtpipe exe standard out. Minimal status updates)
    - stderr (pbsmrtpipe exe standard err. Should be the first place to look for workflow level traceback and task errors
    - logs/
        - pbsmrtpipe.log
        - master.log
    - workflow/
        - entry_points.json (this is essentially a heterogeneous dataset of the input.xml or cmdline entry points with entry_id)
        - datastore.json (fundamental store of all output files. Also contains initial task and workflow level values)
    - html/
        - css/
        - js/
        - index.html (main summary page)
    - tasks/ # all tasks are here
        - {task_id}-{instance_id}/  # each task has a directory
            - task-manifest.json
            - task-report.json (when the task is completed)
            - outfile.1.txt (output files)
            - stdout
            - stderr
            - task.log (optional if task uses $logfile in resources
            - cluster.stdout (if non-local job)
            - cluster.stderr (if non-local job)
            - cluster.sh (qsub submission script)

        - {task_id}-{instance_id}/ # more tasks



Example datastore.json
----------------------

File paths are `workflow` directory.

.. literalinclude:: example_datastore.json
    :language: js


Example task-manifest.json
--------------------------

In each task directory, a **task-manifest** is written. It contains all the metadata necessary for the task to be run on the execution node (or run locally). The task manifest is run via the **pbtools-runner** commandline tool.

There are several motivations for **pbtool-runner** abstraction
- NFS checks to validate input files can be found (related to python NFS caching errors that often result in IOErrors)
- create and cleanup tmp resources on execution node
- write env.json to document the environment vars
- write metadata results about the output of the tasks
- allow a bit of tweaking and rerunning by hand for failed tasks
- strict documenting of input files, resolved task type, resolved task options used to run the task

`task-manifest.json` contains all the resolved task type, resolved task values, such as resolved options, input files, output files, and resources (e.g., temp files, temp dirs).

Example file.

.. literalinclude:: example_task-manifest.json
    :language: js


Example task-report.json
------------------------

After **pbtool-runner** completes executing the task, a metadata report of job is written to job directory.

`task-report.json` contains basic metadata about the completed task.


.. literalinclude:: example_task-report.json
    :language: js
