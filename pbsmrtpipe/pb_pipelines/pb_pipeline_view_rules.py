#!/usr/bin/env python

import logging
import sys

from pbcommand.models import (FileTypes, DataStoreViewRule,
                              PipelineDataStoreViewRules)

log = logging.getLogger(__name__)

REGISTERED_VIEW_RULES = {}


def _to_view_rule(args):
    return DataStoreViewRule(*args)


def load_pipeline_view_rules(registered_views_d, pipeline_id, smrtlink_version,
                             view_rules):
    pvr = PipelineDataStoreViewRules(
        pipeline_id="pbsmrtpipe.pipelines.{p}".format(p=pipeline_id),
        smrtlink_version=smrtlink_version,
        rules=[_to_view_rule(r) for r in view_rules])
    registered_views_d[pipeline_id] = pvr
    return registered_views_d


def register_pipeline_rules(pipeline_id, smrtlink_version):
    def deco_wrapper(func):
        if pipeline_id in REGISTERED_VIEW_RULES:
            log.warn("'{i}' already has view rules defined".format(
                     i=pipeline_id))
        rules = func()
        load_pipeline_view_rules(REGISTERED_VIEW_RULES, pipeline_id,
                                 smrtlink_version, rules)

        def wrapper(*args, **kwds):
            return func(*args, **kwds)
    return deco_wrapper

#define files rules

def _mapping_report_rules():
    return [
        ("pbreports.tasks.mapping_stats-out-0", FileTypes.REPORT, True),
        ("pbreports.tasks.coverage_report-out-0", FileTypes.REPORT, True)
    ]


def _variant_report_rules():
    return [
        ("pbreports.tasks.variants_report-out-0", FileTypes.REPORT, True),
        ("pbreports.tasks.top_variants-out-0", FileTypes.REPORT, True)
    ]

def _isoseq_report_rules():
    return [
        ("pbtranscript.tasks.classify-out-3", FileTypes.JSON, True),
        ("pbreports.tasks.isoseq_classify-out-0", FileTypes.REPORT, True)
    ]

def _ccs_report_rules():
    return [
        ("pbreports.tasks.ccs_report-out-0", FileTypes.REPORT, True),
    ]

#define pipeline rules

@register_pipeline_rules("hgap_fat", "3.2")
def hgap_view_rules():
    return   [
        ("falcon_ns.tasks.task_hgap_prepare-out-0", FileTypes.JSON, True),
        ("falcon_ns.tasks.task_hgap_prepare-out-1", FileTypes.JSON, True),
        ("falcon_ns.tasks.task_hgap_prepare-out-2", FileTypes.LOG, True),
        ("falcon_ns.tasks.task_hgap_run-out-1", FileTypes.REPORT, True),
        ("falcon_ns.tasks.task_hgap_run-out-2", FileTypes.REPORT, True),
        ("falcon_ns.tasks.task_hgap_run-out-3", FileTypes.LOG, True)
    ]


@register_pipeline_rules("sa3_ds_isoseq", "3.2")
def isoseq_view_rules():
    return  _isoseq_report_rules() + _ccs_report_rules() + [
        ("pbtranscript.tasks.separate_flnc-out-0", FileTypes.PICKLE, True),
        ("pbtranscript.tasks.create_chunks-out-0", FileTypes.PICKLE, True),
        ("pbtranscript.tasks.create_chunks-out-1", FileTypes.PICKLE, True),
        ("pbtranscript.tasks.create_chunks-out-2", FileTypes.PICKLE, True),
        ("pbtranscript.tasks.combine_cluster_bins-out-7", FileTypes.PICKLE, True),
        ("pbtranscript.tasks.gather_ice_partial_cluster_bins_pickle-out-0", FileTypes.TXT, True),
        ("pbtranscript.tasks.cluster_bins-out-0", FileTypes.TXT, True),
        ("pbtranscript.tasks.ice_partial_cluster_bins-out-0", FileTypes.TXT, True),
        ("pbtranscript.tasks.ice_polish_cluster_bins-out-0", FileTypes.TXT, True),
        ("pbtranscript.tasks.gather_polished_isoforms_in_each_bin-out-0", FileTypes.TXT, True),
        ("pbtranscript.tasks.ice_cleanup-out-0", FileTypes.TXT, True),
        ("pbtranscript.tasks.combine_cluster_bins-out-1", FileTypes.JSON, True),
        ("pbreports.tasks.isoseq_cluster-out-0", FileTypes.REPORT, True)

    ]

@register_pipeline_rules("sa3_ds_isoseq_classify", "3.2")
def isoseq_classify_view_rules():
    return  _isoseq_report_rules() + _ccs_report_rules() + [

    ]

@register_pipeline_rules("sa3_sat", "3.2")
def sat_view_rules():
    return _mapping_report_rules() + _variant_report_rules() + [
        ("pbreports.tasks.sat_report-out-0", FileTypes.REPORT, True),
        ("pbalign.tasks.pbalign-out-0", FileTypes.XML, True)
    ]

@register_pipeline_rules("sa3_ds_resequencing_fat", "3.2")
def resequencing_view_rules():
    return _mapping_report_rules() + _variant_report_rules() + [
        ("pbalign.tasks.pbalign-out-0", FileTypes.XML, True)
    ]


def main(argv):
    logging.basicConfig(level=logging.INFO)
    for pipeline_id, rules in REGISTERED_VIEW_RULES.iteritems():
        file_name = "pipeline_datastore_view_rules-{p}.json".format(p=pipeline_id)
        log.info("Writing {f}".format(f=file_name))
        rules.write_json(file_name)
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
