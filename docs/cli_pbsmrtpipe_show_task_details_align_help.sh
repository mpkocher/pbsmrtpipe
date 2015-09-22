Registry Loaded. Number of MetaTasks:94 FileTypes:43 ChunkOperators:12 Pipelines:30
********************
ToolContractMetaTask summary id:pbalign.tasks.pbalign
--------------------
Description:
Mapping PacBio sequences to references using an algorithm selected from a
selection of supported command-line alignment algorithms. Input can be a
fasta, pls.h5, bas.h5 or ccs.h5 file or a fofn (file of file names). Output
can be in CMP.H5, SAM or BAM format. If output is BAM format, aligner can
only be blasr and QVs will be loaded automatically.
--------------------
 Input Types (2)
--------------------
  0 <FileType id=PacBio.DataSet.SubreadSet name=file.subreadset.xml >
  1 <FileType id=PacBio.DataSet.ReferenceSet name=file.referenceset.xml >
--------------------
 Output Types (1)
--------------------
  0 <FileType id=PacBio.DataSet.AlignmentSet name=file.alignmentset.xml >
--------------------
 Is Distributed      : True
 nproc               : DI list (n) items
 Number of Options   : 6
--------------------
 Override Output files names (1)
   0: <FileType id=PacBio.DataSet.AlignmentSet name=file.alignmentset.xml > -> aligned.subreads.alignmentset.xml 
--------------------
Number of Options 6
Option #0 Id: pbalign.task_options.min_accuracy
	Default     :  70.0
	Type        :  number
	Description :  Minimum required alignment accuracy (percent)

Option #1 Id: pbalign.task_options.algorithm_options
	Default     :  -useQuality -minMatch 12 -bestn 10 -minPctIdentity 70.0
	Type        :  string
	Description :  List of space-separated arguments passed to BLASR (etc.)

Option #2 Id: pbalign.task_options.min_length
	Default     :  50
	Type        :  integer
	Description :  Minimum required alignment length

Option #3 Id: pbalign.task_options.hit_policy
	Default     :  randombest
	Type        :  string
	Description :  Specify a policy for how to treat multiple hit
  random    : selects a random hit.
  all       : selects all hits.
  allbest   : selects all the best score hits.
  randombest: selects a random hit from all best score hits.
  leftmost  : selects a hit which has the best score and the
              smallest mapping coordinate in any reference.
Default value is randombest.

Option #4 Id: pbalign.task_options.concordant
	Default     :  False
	Type        :  boolean
	Description :  Map subreads of a ZMW to the same genomic location

Option #5 Id: pbalign.task_options.useccs
	Default     :  
	Type        :  string
	Description :  Map the ccsSequence to the genome first, then align
subreads to the interval that the CCS reads mapped to.
  useccs: only maps subreads that span the length of
          the template.
  useccsall: maps all subreads.
  useccsdenovo: maps ccs only.

