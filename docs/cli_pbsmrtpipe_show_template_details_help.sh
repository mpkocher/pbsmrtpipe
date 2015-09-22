Registry Loaded. Number of MetaTasks:94 FileTypes:43 ChunkOperators:12 Pipelines:30
Pipeline id   : pbsmrtpipe.pipelines.sa3_ds_resequencing
Pipeline name : SA3 SubreadSet Resequencing
Description   : Core Resequencing Pipeline
Entry points  : 2
********************
$entry:eid_subread
$entry:eid_ref_dataset

Bindings      : 5
********************
    pbalign.tasks.pbalign:0 -> pbreports.tasks.mapping_stats:0
    pbalign.tasks.pbalign:0 -> genomic_consensus.tasks.variantcaller:0
     $entry:eid_ref_dataset -> genomic_consensus.tasks.variantcaller:1
         $entry:eid_subread -> pbalign.tasks.pbalign:0
     $entry:eid_ref_dataset -> pbalign.tasks.pbalign:1
No default task options
