=====================
How to use pbsmrtpipe
=====================

Getting started
===============

The ``pbsmrtpipe`` command is designed to be more-or-less self-documenting.
It is normally run in one of several modes, which is specified as a
positional argument.  To get the full overview of functionality, run this::

  $ pbsmrtpipe --help

and you can get further help for individual modes, for example::

  $ pbsmrtpipe pipeline-id --help

To display a list of available pipelines, use *show-templates*::

  $ pbsmrtpipe show-templates

For details about a specific pipeline, specify the ID (the last field in each
item in the output of *show-templates*) with *show-template-details*::

  $ pbsmrtpipe show-template-details pbsmrtpipe.pipelines.sa3_ds_resequencing

Among other things, this will list the **entry points** required for the
pipeline.  These will usually be PacBio Dataset XML files (see Appendix B
for instructions on generating these), although single raw data files
(BAM or FASTA format) may be acceptable for some use cases.  The most common
input will be ``eid_subread``, a SubreadSet XML dataset, which contains one or
more BAM files containing the raw unaligned subreads.  Also common is
``eid_ref_dataset``, for a ReferenceSet or genomic FASTA file.


Parallelization
===============

The algorithms used to analyze PacBio data are computationally intensive but
also intrinsically highly parallel.  pbsmrtpipe is designed to scale to at
least hundreds of processors on multi-core systems and/or managed clusters.
This is handled by two distinct but complementary methods:

  - **multiprocessing** is implemented in the underlying tasks, all of which
    are generally shared-memory programs.  This is effectively always turned
    on unless the ``max_nchunk`` parameter is set to 1 (see examples section
    below for a description of how to modify parameter values).  For most
    compute node configurations a value between 8 and 16 is appropriate.

  - **chunking** is implemented by pbsmrtpipe and works by applying filters to
    the input datasets, which direct tasks to operate on a subset ("chunk") of
    the data.  These chunks are most commonly either a contiguous subset of
    reads or windows in the reference genome sequence.  

Note that at present, the task-level output directories (and the locations
of the final result files) may be slightly different depending on whether
chunking is used, since an intermediate "gather" step is required to join
chunked results.


Common workflows
================

All pipelines in pbsmrtpipe are prefixed with "pbsmrtpipe.pipelines."; for
clarity this is omitted from the table below.


+-------------------------------+------------------------------------------+
|Pipeline                       | Purpose                                  |
+===============================+==========================================+
|sa3_sat                        | Site Acceptance Test run on all new      |
|                               | PacBio installations                     |
+-------------------------------+------------------------------------------+
|sa3_ds_resequencing            | Map subreads to reference genome and     |
|                               | determine consensus sequence with Quiver |
+-------------------------------+------------------------------------------+
|sa3_ds_ccs                     | Generate high-accuracy Circular          |
|                               | Consensus Reads from subreads            |
+-------------------------------+------------------------------------------+
|sa3_ds_ccs_align               | ConsensusRead (CCS) + mapping to         |
|                               | reference genome, starting from subreads |
+-------------------------------+------------------------------------------+
|sa3_ds_isoseq_classify         | IsoSeq transcript classification,        |
|                               | starting from subreads                   |
+-------------------------------+------------------------------------------+
|sa3_ds_isoseq                  | Full IsoSeq with clustering and          |
|                               | Quiver polishing (much slower)           |
+-------------------------------+------------------------------------------+
|ds_modification_motif_analysis | Base modification detection and motif    |
|                               | finding, starting from subreads          |
+-------------------------------+------------------------------------------+
|sa3_hdfsubread_to_subread      | Convert HdfSubreadSet to SubreadSet      |
|                               | (import bax.h5 basecalling files         |
+-------------------------------+------------------------------------------+

Nearly all of these pipelines (except for sa3_hdfsubread_to_subread) require
a SubreadSet as input; many also require a ReferenceSet.  Output is more
varied:

+-------------------------------+------------------------------------------+
|Pipeline                       | Essential outputs                        |
+===============================+==========================================+
|sa3_sat                        | variants GFF, SAT report                 |
+-------------------------------+------------------------------------------+
|sa3_ds_resequencing            | AlignmentSet, consensus ContigSet,       |
|                               | variants GFF                             |
+-------------------------------+------------------------------------------+
|sa3_ds_ccs                     | ConsensusReadSet, FASTA and FastQ files  |
+-------------------------------+------------------------------------------+
|sa3_ds_ccs_mapping             | As above plus ConsensusAlignmentSet      |
+-------------------------------+------------------------------------------+
|sa3_ds_isoseq_classify         | ContigSets of classified transcripts     |
+-------------------------------+------------------------------------------+
|sa3_ds_isoseq                  | As above plus polished isoform ContigSet |
+-------------------------------+------------------------------------------+
|ds_modification_motif_analysis | Resequencing output plus basemods GFF,   |
|                               | motifs CSV                               |
+-------------------------------+------------------------------------------+
|sa3_hdfsubread_to_subread      | SubreadSet                               |
+-------------------------------+------------------------------------------+



Practical Examples
==================

Basic resequencing
------------------

This pipeline uses **pbalign** to map reads to a reference genome, and
**quiver** to determine the consensus sequence.

We will be using the **sa3_ds_resequencing** pipeline::

  $ pbsmrtpipe show-template-details pbsmrtpipe.pipelines.sa3_ds_resequencing

Which requires two entry points: a ``SubreadSet`` and a ``ReferenceSet``.  A
typical invocation might look like this (for a hypothetical lambda virus
genome)::

  $ pbsmrtpipe pipeline-id pbsmrtpipe.pipelines.sa3_ds_resequencing \
    -e eid_subread:/data/smrt/2372215/0007/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml \
    -e eid_ref_dataset:/data/references/lambdaNEB/lambdaNEB.referenceset.xml

This will run for a while and emit several directories, including tasks, logs,
and workflow.  The tasks directory is the most useful, as it stores the
intermediate results and resolved tool contracts (how the task was executed)
for each task. The directory names (task_ids) should be somewhat
self-explanatory.  If you want to direct the output to a subdirectory in the
current working directory, use the ``-o`` flag: ``-o job_output_1``.

Other pipelines related to resequencing, such as the basemods detection
and motif finding, have nearly identical command-line arguments except for the
pipeline ID.


Site Acceptance Test
--------------------

The SAT pipeline is used to validate all new PacBio systems upon installation.
It is essentially the resequencing pipeline applied to high-coverage lambda
virus genome data collected on a PacBio instrument, with an additional report.
The invocation is therefore nearly identical, but you should always be using
the **lambdaNEB** reference genome::

  $ pbsmrtpipe pipeline-id pbsmrtpipe.pipelines.sa3_sat \
    -e eid_subread:/data/smrt/2372215/0007/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml \
    -e eid_ref_dataset:/data/references/lambdaNEB/lambdaNEB.referenceset.xml \
    -o job_output_2

The output directories will be the same as the resequencing job plus
``pbreports.tasks.sat_report-0``.  The most important file is (assuming the
command line arguments shown above)::

  job_output_2/tasks/pbreports.tasks.sat_report-0/report.json

The JSON file will have several statistics, the most important of which are
coverage and accuracy, both expected to be 1.0.


Quiver (Genomic Consensus)
--------------------------

If you already have an AlignmentSet on which you just want to run quiver, the
**sa3_ds_genomic_consensus** pipeline will be faster::

  $ pbsmrtpipe pipeline-id pbsmrtpipe.pipelines.sa3_ds_genomic_consensus \
    -e eid_bam_alignment:/data/project/my_lambda_genome.alignmentset.xml \
    -e eid_ref_dataset:/data/references/lambda.referenceset.xml \
    --preset-xml=preset.xml

See Appendix B below for instructions on generating an AlignmentSet XML from
one or more mapped BAM files.


Circular Consensus Reads
------------------------

To obtain high-quality consensus reads (also known as CCS reads) for
individual SMRTcell ZMWs from high-coverage subreads::

  $ pbsmrtpipe pipeline-id pbsmrtpipe.pipelines.sa3_ds_ccs \
    -e eid_subread:/data/smrt/2372215/0007/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml \
    --preset-xml preset.xml -o job_output

This pipeline is relatively simple and also parallelizes especially well.
The essential outputs are a ConsensusRead dataset (composed of one or more
unmapped BAM files) and corresponding FASTA and FASTQ files:

  job_output/tasks/pbccs.tasks.ccs-0/ccs.consensusreadset.xml
  job_output/tasks/pbsmrtpipe.tasks.bam2fasta_ccs-0/file.fasta
  job_output/tasks/pbsmrtpipe.tasks.bam2fastq_ccs-0/file.fastq

The ``pbccs.tasks.ccs-0`` task directory will also contain a JSON report
with basic metrics for the run such as number of reads passed and rejected
for various reasons.  (Note, as explained below, that the location of the
final ConsensusRead XML - and JSON report - will be different in chunk mode.)

Because the full resequencing workflow operates directly on subreads to
produce a genomic consensus, it is not applicable to CCS reads.  However, a
CCS pipeline is available that incorporates the Blasr mapping step::

  $ pbsmrtpipe pipeline-id pbsmrtpipe.pipelines.sa3_ds_ccs_align \
    -e eid_subread:/data/smrt/2372215/0007/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml \
    -e eid_ref_dataset:/data/references/lambda.referenceset.xml \
    --preset-xml preset.xml -o job_output


IsoSeq Transcriptome Analysis
-----------------------------

The IsoSeq workflows automate use of the **pbtranscript** package for
investigating mRNA transcript isoforms.  The transcript analysis uses CCS
reads where possible, and the pipeline incorporates the CCS pipeline with
looser settings.  The starting point is therefore still a SubreadSet.  The
simpler of the two pipelines is ``sa3_ds_isoseq_classify``, which runs CCS
and classifies the reads as full-length or not::

  $ pbsmrtpipe pipeline-id pbsmrtpipe.pipelines.sa3_ds_isoseq_classify \
    -e eid_subread:/data/smrt/2372215/0007/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml \
    --preset-xml preset.xml -o job_output

The output files from the CCS pipeline will again be present (note however
that the sequences will be lower-quality since the pipeline tries to use as
much information as possible).  The output task folder
``pbtranscript.tasks.classify-0`` (or gathered equivalent; see below) contains
the classified transcripts in various ContigSet datasets (or underlying FASTA
files).

A more thorough analysis yielding Quiver-polished, high-quality isoforms is
the ``pbsmrtpipe.pipelines.sa3_ds_isoseq`` pipeline, which is invoked
identically to the classify-only pipeline.  Note that this is significantly
slower, as the clustering step may take days to run for large datasets.


Exporting Subreads to FASTA/FASTQ
---------------------------------

If you would like to convert a PacBio SubreadSet to FASTA or FASTQ format for
use with external software, this can be done as a standalone pipeline.
Unlike most of the other pipelines, this one has no task-specific options and
no chunking, so the invocation is always very simple::

  $ pbsmrtpipe pipeline-id pbsmrtpipe.pipelines.sa3_ds_subreads_to_fastx \
    -e eid_subread:/data/smrt/2372215/0007/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml \
    -o job_output

The result files will be here::

  job_output/tasks/pbsmrtpipe.tasks.bam2fasta-0/file.fasta
  job_output/tasks/pbsmrtpipe.tasks.bam2fastq-0/file.fastq

Both are also available gzipped in the same directories.


Chunking
--------

To take advantage of pbsmrtpipe's parallelization, we need an XML configuration
file for global pbsmrtpipe options, which can be generated by the following
command::

  $ pbsmrtpipe show-workflow-options -o preset.xml

The output ``preset.xml`` will have this format::

  <?xml version="1.0" encoding="utf-8" ?>
  <pipeline-preset-template>
      <options>
          <option id="pbsmrtpipe.options.max_nproc">
              <value>16</value>
          </option>
          <option id="pbsmrtpipe.options.chunk_mode">
              <value>False</value>
          </option>
          <!-- MANY MORE OPTIONS OMITTED -->
      </options>
  </pipeline-preset-template>

The appropriate types should be clear; quotes are unnecessary, and boolean
values should have initial capitals (``True``, ``False``).  To enable chunk
mode, change the value of option ``pbsmrtpipe.options.chunk_mode`` to ``True``.
Several additional options may also need to be modified:

  - ``pbsmrtpipe.options.distributed_mode`` enables execution of most tasks on
    a managed cluster such as Sun Grid Engine.  Use this for chunk mode if
    available.
  - ``pbsmrtpipe.options.max_nchunks`` sets the upper limit on the number of
    jobs per task in chunked mode.  Note that more chunks is not always better,
    as there is some overhead to chunking (especially in distributed mode).
  - ``pbsmrtpipe.options.max_nproc`` sets the upper limit on the number of
    processors per job (including individual chunk jobs).  This should be set
    to a value appropriate for your compute environment.

You can adjust ``max_nproc`` and max_nchunks`` in the preset.xml to consume as
many queue slots as you desire, but note that the number of slots consumed will
be the product of the two numbers.  For some shorter jobs (typically with
low-volume input data), it may make more sense to run the job unchunked but
still distribute tasks to the cluster (where they will still use multiple
cores if allowed).

Once you are satisfied with the settings, add it to your command like this::

  $ pbsmrtpipe pipeline-id pbsmrtpipe.pipelines.sa3_ds_resequencing \
    --preset-xml preset.xml \
    -e eid_subread:/data/smrt/2372215/0007/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml \
    -e eid_ref_dataset:/data/references/lambda.referenceset.xml

Alternately, the flags ``--force-chunk-mode``, ``--force-distributed``,
``--disable-chunk-mode``, and ``--local-only`` can be used to toggle the
chunk/distributed mode settings on the command line (but this will not affect
the values of max_nproc or max_nchunks).

If the pipeline runs correctly, you should see an expansion of task folders.
The final results for certain steps (alignment, variantCaller, etc), should
end up in the appropriate "gather" directory. For instance, the final gathered
fasta file from quiver should be in ``pbsmrtpipe.tasks.gather_contigset-1``.
Note that for many dataset types, the gathered dataset XML file will often
encapsulate multiple BAM files in multiple directories.


Modifying task-specific options
-------------------------------

You can generate an appropriate initial preset.xml containing task-specific
options relevant to a selected pipeline by running the *show-template-details*
sub-command::

  $ pbsmrtpipe show-template-details pbsmrtpipe.pipelines.sa3_ds_resequencing \
      -o preset_tasks.xml

The output XML file will be in a format similar to the global presets XML::

  <?xml version="1.0" encoding="utf-8" ?>
  <pipeline-preset-template>
      <task-options>
          <option id="pbalign.task_options.min_accuracy">
              <value>70.0</value>
          </option>
          <option id="pbalign.task_options.algorithm_options">
              <value>-useQuality -minMatch 12 -bestn 10 -minPctIdentity 70.0</value>
          </option>
      </task-options>
  </pipeline-preset-template>

You may specify multiple preset files on the command line::

  $ pbsmrtpipe pipeline-id pbsmrtpipe.pipelines.sa3_ds_resequencing \
    --preset-xml preset.xml --preset-xml preset_tasks.xml \
    -e eid_subread:/path/to/subreadset.xml \
    -e eid_ref_dataset:/path/to/referenceset.xml

Alternately, the entire ``<task-options>`` block can also be copied-and-pasted
into the equivalent level in the ``preset.xml`` that contains global options.


Appendix A: hdfsubreadset to subreadset conversion.
===================================================

If you have existing bax.h5 files that you would like to process with
pbsmrtpipe, you will need to convert them to a SubreadSet before continuing.
Bare bax.h5 files aren't directly compatible with pbsmrtpipe, but we can
generate an HdfSubreadSet XML file from a fofn or folder of bax.h5 files
using the python dataset xml api/cli very easily. 

From a fofn, allTheBaxFiles.fofn::

  $ dataset create --type HdfSubreadSet allTheBaxFiles.hdfsubreadset.xml allTheBaxFiles.fofn

Or a directory with all the bax files::

  $ dataset create --type HdfSubreadSet allTheBaxFiles.hdfsubreadset.xml allTheBaxFiles/*.bax.h5

We can then use this as an entry point to the conversion pipeline (we
recommend using chunked mode if there is more than one bax.h5 file, so include
the appropriate preset.xml)::

  $ pbsmrtpipe pipeline-id pbsmrtpipe.pipelines.sa3_hdfsubread_to_subread \
    --preset-xml preset.xml -e eid_hdfsubread:allTheBaxFiles.hdfsubreadset.xml

And use the gathered output xml as an entry point to the resequencing pipeline
from earlier::

  $ pbsmrtpipe pipeline-id pbsmrtpipe.pipelines.sa3_ds_resequencing \
    --preset-xml preset.xml \
    -e eid_subread:tasks/pbsmrtpipe.tasks.gather_subreadset-0/gathered.xml \
    -e eid_ref_dataset:/data/references/lambda.referenceset.xml


Appendix B: Working with datasets
=================================

Datasets can also be created for one or more existing subreads.bam files or
alignedsubreads.bam files for use with the pipeline::

  $ dataset create --type SubreadSet allTheSubreads.subreadset.xml \
      mySubreadBams/*.bam

or::

  $ dataset create --type AlignmentSet allTheMappedSubreads.alignmentset.xml \
      myMappedSubreadBams/*.bam

Make sure that all ``.bam`` files have corresponding ``.bai`` and ``.pbi``
index files before generating the dataset, as these make some operations
significantly faster and are required by many programs.  You can create indices
with **samtools** and **pbindex**, both included in the distribution::

  $ samtools index subreads.bam
  $ pbindex subreads.bam

In addition to the BAM-based datasets and HdfSubreadSet, pbsmrtpipe also
works with two dataset types based on FASTA format: ContigSet (used for both
de-novo assemblies and other collections of contiguous sequences such as
transcripts in the IsoSeq workflows) and ReferenceSet (a reference genome).
These are created in the same way as BAM datasets::

  $ dataset create --type ReferenceSet human_genome.referenceset.xml \
      genome/chr*.fasta

FASTA files can also be indexed for increased speed using samtools, and this
is again recommended before creating the dataset::

  $ samtools faidx chr1.fasta

Note that `PacBio's specifications <http://pacbiofileformats.readthedocs.org/en/3.0/>`_ for BAM and FASTA files impose additional restrictions on content and
formatting; files produce by non-PacBio software are not guaranteed to work
as input.  The ``pbvalidate`` tool can be used to check for format compliance.
