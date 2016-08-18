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

def _log_file_rules():
    return [
        ("pbsmrtpipe::master.log", FileTypes.LOG, False),
        ("pbsmrtpipe::pbsmrtpipe.log", FileTypes.LOG, False)
    ]

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


@register_pipeline_rules("sa3_sat", "3.2")
def sat_view_rules():
    return _mapping_report_rules() + _variant_report_rules() + _log_file_rules() + [
        ("pbreports.tasks.sat_report-out-0", FileTypes.REPORT, True)
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
