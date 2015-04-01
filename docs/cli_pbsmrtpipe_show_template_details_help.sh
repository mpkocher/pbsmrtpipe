Pipeline id   : pbsmrtpipe.pipelines.rs_resequencing_1
Pipeline name : RS Resequencing Pipeline
Description   : Official Pacbio Resequencing Pipeline Description
Entry points : 2
********************
entry:eid_ref_fasta
entry:eid_input_xml

Bindings     : 49
********************
                pbsmrtpipe.tasks.input_xml_to_fofn.0 -> pbsmrtpipe.tasks.movie_overview_report.0
                pbsmrtpipe.tasks.input_xml_to_fofn.0 -> pbsmrtpipe.tasks.adapter_report.0
                           pbsmrtpipe.tasks.filter.0 -> pbsmrtpipe.tasks.filter_subreads.1
                           pbsmrtpipe.tasks.filter.0 -> pbsmrtpipe.tasks.filter_subread_summary.0
                           pbsmrtpipe.tasks.filter.1 -> pbsmrtpipe.tasks.filter_report.0
           pbsmrtpipe.tasks.filter_subread_summary.0 -> pbsmrtpipe.tasks.filter_subread_report.0
                           pbsmrtpipe.tasks.filter.1 -> pbsmrtpipe.tasks.loading_report.0
                pbsmrtpipe.tasks.input_xml_to_fofn.0 -> pbsmrtpipe.tasks.filter.0
                pbsmrtpipe.tasks.input_xml_to_fofn.1 -> pbsmrtpipe.tasks.filter.1
                pbsmrtpipe.tasks.input_xml_to_fofn.0 -> pbsmrtpipe.tasks.filter_subreads.0
                pbsmrtpipe.tasks.input_xml_to_fofn.0 -> pbsmrtpipe.tasks.align.0
                           pbsmrtpipe.tasks.filter.0 -> pbsmrtpipe.tasks.align.1
                    pbsmrtpipe.tasks.ref_to_report.0 -> pbsmrtpipe.tasks.align.3
                  pbsmrtpipe.tasks.filter_subreads.0 -> pbsmrtpipe.tasks.extract_unmapped_subreads.0
                     pbsmrtpipe.tasks.cmph5_repack.0 -> pbsmrtpipe.tasks.extract_unmapped_subreads.1
                       pbsmrtpipe.tasks.cmph5_sort.0 -> pbsmrtpipe.tasks.cmph5_repack.0
                     pbsmrtpipe.tasks.cmph5_repack.0 -> pbsmrtpipe.tasks.cmph5_to_sam.1
                     pbsmrtpipe.tasks.cmph5_repack.0 -> pbsmrtpipe.tasks.coverage_summary.0
                 pbsmrtpipe.tasks.coverage_summary.0 -> pbsmrtpipe.tasks.mapping_gff_to_bed.0
                            pbsmrtpipe.tasks.align.0 -> pbsmrtpipe.tasks.cmph5_sort.0
                pbsmrtpipe.tasks.input_xml_to_fofn.0 -> pbsmrtpipe.tasks.mapping_stats_report.0
                           pbsmrtpipe.tasks.filter.0 -> pbsmrtpipe.tasks.mapping_stats_report.1
                     pbsmrtpipe.tasks.cmph5_repack.0 -> pbsmrtpipe.tasks.mapping_stats_report.2
                 pbsmrtpipe.tasks.coverage_summary.0 -> pbsmrtpipe.tasks.coverage_report.0
                     pbsmrtpipe.tasks.cmph5_repack.0 -> pbsmrtpipe.tasks.call_variants_with_fastx.1
    pbsmrtpipe.tasks.write_reference_contig_chunks.0 -> pbsmrtpipe.tasks.call_variants_with_fastx.2
         pbsmrtpipe.tasks.call_variants_with_fastx.0 -> pbsmrtpipe.tasks.consensus_gff_to_bed.0
         pbsmrtpipe.tasks.call_variants_with_fastx.0 -> pbsmrtpipe.tasks.gff_to_vcf.0
         pbsmrtpipe.tasks.call_variants_with_fastx.0 -> pbsmrtpipe.tasks.summarize_consensus.0
                 pbsmrtpipe.tasks.coverage_summary.0 -> pbsmrtpipe.tasks.summarize_consensus.1
                     pbsmrtpipe.tasks.cmph5_repack.0 -> pbsmrtpipe.tasks.call_variants_with_fastx.1
         pbsmrtpipe.tasks.call_variants_with_fastx.0 -> pbsmrtpipe.tasks.consensus_gff_to_bed.0
         pbsmrtpipe.tasks.call_variants_with_fastx.0 -> pbsmrtpipe.tasks.gff_to_vcf.0
                 pbsmrtpipe.tasks.coverage_summary.0 -> pbsmrtpipe.tasks.summarize_consensus.1
         pbsmrtpipe.tasks.call_variants_with_fastx.0 -> pbsmrtpipe.tasks.summarize_consensus.0
                    pbsmrtpipe.tasks.ref_to_report.0 -> pbsmrtpipe.tasks.write_reference_contig_chunks.1
         pbsmrtpipe.tasks.call_variants_with_fastx.0 -> pbsmrtpipe.tasks.top_variants_report.1
              pbsmrtpipe.tasks.summarize_consensus.0 -> pbsmrtpipe.tasks.variants_report.1
         pbsmrtpipe.tasks.call_variants_with_fastx.0 -> pbsmrtpipe.tasks.variants_report.2
                                 entry:eid_input_xml -> pbsmrtpipe.tasks.input_xml_to_fofn.0
                                 entry:eid_ref_fasta -> pbsmrtpipe.tasks.ref_to_report.0
                                 entry:eid_ref_fasta -> pbsmrtpipe.tasks.align.2
                                 entry:eid_ref_fasta -> pbsmrtpipe.tasks.cmph5_to_sam.0
                                 entry:eid_ref_fasta -> pbsmrtpipe.tasks.coverage_report.1
                                 entry:eid_ref_fasta -> pbsmrtpipe.tasks.call_variants_with_fastx.0
                                 entry:eid_ref_fasta -> pbsmrtpipe.tasks.gff_to_vcf.1
                                 entry:eid_ref_fasta -> pbsmrtpipe.tasks.write_reference_contig_chunks.0
                                 entry:eid_ref_fasta -> pbsmrtpipe.tasks.variants_report.0
                                 entry:eid_ref_fasta -> pbsmrtpipe.tasks.top_variants_report.0
