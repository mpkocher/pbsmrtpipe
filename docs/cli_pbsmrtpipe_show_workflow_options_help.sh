Number of Options 11
Option #0 Id: pbsmrtpipe.options.debug_mode
	Default     :  False
	Type        :  boolean
	Description :  Debug will emit debug messages to Stdout and set the level in the master log to DEBUG.

Option #1 Id: pbsmrtpipe.options.max_nproc
	Default     :  16
	Type        :  integer
	Description :  Maximum number of Processors per Task.

Option #2 Id: pbsmrtpipe.options.tmp_dir
	Default     :  /tmp
	Type        :  ['string', 'null']
	Description :  Temporary directory (/tmp) on the execution node. If running in distributed mode, the tmp directory must be on the head node too.

Option #3 Id: pbsmrtpipe.options.chunk_mode
	Default     :  False
	Type        :  boolean
	Description :  Enable file splitting (chunking) mode

Option #4 Id: pbsmrtpipe.options.max_total_nproc
	Default     :  None
	Type        :  ['integer', 'null']
	Description :  Maximum Total number of Processors/Slots the workflow engine will use (null means there is no limit).

Option #5 Id: pbsmrtpipe.options.max_nchunks
	Default     :  10
	Type        :  integer
	Description :  Max Number of chunks that a file will be scattered into

Option #6 Id: pbsmrtpipe.options.distributed_mode
	Default     :  True
	Type        :  boolean
	Description :  Enable Distributed mode to submit jobs to the cluster. (Must provide 'cluster_manager' path to cluster templates)

Option #7 Id: pbsmrtpipe.options.progress_status_url
	Default     :  None
	Type        :  ['string', 'null']
	Description :  Post status progress updates to URL.

Option #8 Id: pbsmrtpipe.options.exit_on_failure
	Default     :  False
	Type        :  boolean
	Description :  Immediately exit if a task fails (Instead of trying to run as many tasks as possible before exiting.

Option #9 Id: pbsmrtpipe.options.cluster_manager
	Default     :  pbsmrtpipe.cluster_templates.sge_pacbio
	Type        :  ['string', 'null']
	Description :  Path to Cluster template files directory. The directory must contain 'start.tmpl', 'interactive.tmpl' and 'kill.tmpl' Or path to python module (e.g., 'pbsmrtpipe.cluster_templates.sge')

Option #10 Id: pbsmrtpipe.options.max_nworkers
	Default     :  100
	Type        :  integer
	Description :  Max Number of concurrently running tasks. (Note:  max_nproc will restrict the number of workers if max_nworkers * max_nproc > max_total_nproc)

