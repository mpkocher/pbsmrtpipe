"""IO classes for pbsmrtpipe"""
from __future__ import absolute_import
import os
import logging
import warnings
from xml.etree.ElementTree import ElementTree, ParseError

from pbcommand.models.report import Report, Attribute

log = logging.getLogger(__name__)


def _to_report(nfiles, attribute_id, report_id):
    # this should have version of the bax/bas files, chemistry
    attributes = [Attribute(attribute_id, nfiles)]
    return Report(report_id, attributes=attributes)


def fofn_to_report(nfofns):
    return _to_report(nfofns, "nfofns", "fofn_report")
