********************
MetaTask summary id:pbsmrtpipe.tasks.align
--------------------
 Input Types (4)
--------------------
  0 <FileType id=pbsmrtpipe.files.movie_fofn name=movie.fofn >
  1 <FileType id=pbsmrtpipe.files.rgn_fofn name=region.fofn >
  2 <FileType id=pbsmrtpipe.files.fasta name=file.fasta >
  3 <FileType id=pbsmrtpipe.files.report name=report.json >
--------------------
 Output Types (1)
--------------------
  0 <FileType id=pbsmrtpipe.files.alignment_cmp_h5 name=alignments.cmp.h5 >
--------------------
 Task Type           : pbsmrtpipe.constants.distributed_task
 nproc               : $max_nproc
 Number of Options   : 12
--------------------
 Override Output files names (1)
   0: <FileType id=pbsmrtpipe.files.alignment_cmp_h5 name=alignments.cmp.h5 > -> aligned_reads.cmp.h5 
--------------------
Number of Options 12
Option #0 Id: pbsmrtpipe.task_options.use_quality
	Default     :  False
	Type        :  boolean
	Description :  Use Quality Description

Option #1 Id: pbsmrtpipe.task_options.max_hits
	Default     :  -1
	Type        :  integer
	Description :  Max Hits Description

Option #2 Id: pbsmrtpipe.task_options.max_error
	Default     :  -1.0
	Type        :  number
	Description :  Mad Divergence description

Option #3 Id: pbsmrtpipe.task_options.use_subreads
	Default     :  True
	Type        :  boolean
	Description :  Enable the Use Subreads mode

Option #4 Id: pbsmrtpipe.task_options.pbalign_opts
	Default     :  
	Type        :  string
	Description :  PBAlign Commandline Options

Option #5 Id: pbsmrtpipe.task_options.min_anchor_size
	Default     :  -1
	Type        :  integer
	Description :  Minimum Anchor size Description

Option #6 Id: pbsmrtpipe.task_options.load_pulses_opts
	Default     :  
	Type        :  string
	Description :  Load Pulses Description

Option #7 Id: pbsmrtpipe.task_options.concordant
	Default     :  False
	Type        :  boolean
	Description :  Concordant Description

Option #8 Id: pbsmrtpipe.task_options.load_pulses
	Default     :  True
	Type        :  boolean
	Description :  Load Pulses Description

Option #9 Id: pbsmrtpipe.task_options.place_repeats_randomly
	Default     :  True
	Type        :  boolean
	Description :  Place Repeats Randomly Description

Option #10 Id: pbsmrtpipe.task_options.load_pulses_metrics
	Default     :  DeletionQV,IPD,InsertionQV,PulseWidth,QualityValue,MergeQV,SubstitutionQV,DeletionTag
	Type        :  string
	Description :  Load Pulses Description

Option #11 Id: pbsmrtpipe.task_options.filter_adapters_only
	Default     :  False
	Type        :  boolean
	Description :  Filter Adapter Only mode Description

