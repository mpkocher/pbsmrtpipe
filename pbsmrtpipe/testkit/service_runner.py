
"""
Utility for running a testkit job through services as an alternative to
pbtestkit-runner.
"""

import xml.etree.ElementTree as ET
import argparse
import logging
import sys

from pbcommand.cli import (get_default_argparser_with_base_opts,
                           pacbio_args_runner)
from pbcommand.utils import setup_log
from pbcommand.services import ServiceAccessLayer, ServiceEntryPoint
from pbcommand.services.cli import run_analysis_job
import pbcommand.services

from pbsmrtpipe.testkit.butler import config_parser_to_butler
from pbsmrtpipe.testkit.loader import parse_cfg_file
from pbsmrtpipe.testkit.runner import run_butler_tests

log = logging.getLogger(__name__)


# FIXME(nechols)(2016-01-22): these utility functions should live elsewhere
def get_entrypoints(testkit_cfg):
    parsed_cfg = config_parser_to_butler(testkit_cfg)
    entrypoints = parsed_cfg.entry_points
    return entrypoints


def dtype_and_uuid_from_dataset_xml(dataset_xml):
    tree = ET.parse(dataset_xml)
    root = tree.getroot()
    metatype = root.attrib['MetaType']
    unique_id = root.attrib['UniqueId']
    return metatype, unique_id


def entrypoints_dicts(entrypoints):
    """
    Extract dataset info from a list of entrypoints.
    """
    eps = []
    for entrypoint, dataset_xml in entrypoints.iteritems():
        dtype, unique_id = dtype_and_uuid_from_dataset_xml(dataset_xml)
        entry = {"_comment": "pbservice auto-job",
                 "datasetId": "{u}".format(u=unique_id),
                 "entryId": "{k}".format(k=entrypoint),
                 "fileTypeId": "{t}".format(t=dtype)}
        eps.append(entry)
    return eps


def pipeline_id_from_testkit_cfg(testkit_cfg):
    parsed_cfg = config_parser_to_butler(testkit_cfg)
    parsed_dict = parsed_cfg.__dict__
    tree = ET.parse(parsed_dict['workflow_xml'])
    root = tree.getroot()
    pipeline_id = root[0].attrib['id']
    return pipeline_id


def job_id_from_testkit_cfg(testkit_cfg):
    parsed_cfg = config_parser_to_butler(testkit_cfg)
    return parsed_cfg.job_id


def run_services_testkit_job(host, port, testkit_cfg,
                             xml_out="test-output.xml",
                             ignore_test_failures=False,
                             time_out=1800):
    """
    Given a testkit.cfg and host/port parameters:
        1. convert the .cfg to a JSON file
        2. connect to the SMRTLink services and start the job, then block
           until it finishes
        3. run the standard test suite on the job output
    """
    butler = config_parser_to_butler(testkit_cfg)
    test_cases = parse_cfg_file(testkit_cfg)
    entrypoints = get_entrypoints(testkit_cfg)
    pipeline_id = pipeline_id_from_testkit_cfg(testkit_cfg)
    job_id = job_id_from_testkit_cfg(testkit_cfg)
    log.info("job_id = {j}".format(j=job_id))
    log.info("pipeline_id = {p}".format(p=pipeline_id))
    log.info("url = {h}:{p}".format(h=host, p=port))
    sal = ServiceAccessLayer(host, port)
    service_entrypoints = [ServiceEntryPoint.from_d(x) for x in
                           entrypoints_dicts(entrypoints)]
    for ep, dataset_xml in entrypoints.iteritems():
        log.info("Importing {x}".format(x=dataset_xml))
        sal.run_import_local_dataset(dataset_xml)
    log.info("starting anaylsis job...")
    engine_job = run_analysis_job(sal, job_id, pipeline_id,
                                  service_entrypoints, block=True,
                                  time_out=time_out)
    job_output_path = engine_job.path
    log.info("running tests...")
    exit_code = run_butler_tests(
        test_cases=test_cases,
        output_dir=job_output_path,
        output_xml=xml_out,
        job_id=job_id)
    if ignore_test_failures and engine_job.was_successful():
        return 0
    return exit_code


def args_runner(args):
    return run_services_testkit_job(
        host=args.host,
        port=args.port,
        testkit_cfg=args.testkit_cfg,
        xml_out=args.xml_out,
        ignore_test_failures=args.ignore_test_failures,
        time_out=args.time_out)


def _get_parser():
    p = get_default_argparser_with_base_opts(
        version="0.1",
        description=__doc__)
    p.add_argument("testkit_cfg")
    p.add_argument("-u", "--host", dest="host", action="store",
                   default="http://localhost")
    p.add_argument("-p", "--port", dest="port", action="store", type=int,
                   default=8081, help="Port number")
    p.add_argument("-x", "--xunit", dest="xml_out", default="test-output.xml",
                   help="Output XUnit test results")
    p.add_argument("-t", "--timeout", dest="time_out", type=int, default=1800,
                   help="Timeout for blocking after job submission")
    p.add_argument("--ignore-test-failures", dest="ignore_test_failures",
                   action="store_true",
                   help="Only exit with non-zero return code if the job "+
                        "itself failed, regardless of test outcome")
    return p


def main(argv=sys.argv):
    return pacbio_args_runner(
        argv=argv[1:],
        parser=_get_parser(),
        args_runner_func=args_runner,
        alog=log,
        setup_log_func=setup_log)

if __name__ == "__main__":
    sys.exit(main())
