"""IO classes for pbsmrtpipe"""
from __future__ import absolute_import
import os
import logging
import warnings
from xml.etree.ElementTree import ElementTree, ParseError

from pbsmrtpipe.report_model import Report, Attribute

log = logging.getLogger(__name__)


class InputXml(object):

    def __init__(self, movie_files):
        """
        :param movie_files:
        :type movie_files: list
        """
        # absolute path of movie files
        # this should really be a list of Movie instances
        self._movies = movie_files

    def __str__(self):
        return "{n} movies".format(n=len(self.movies))

    def __repr__(self):
        return "".join(["<", str(self), ">"])

    @property
    def movies(self):
        return self._movies

    @staticmethod
    def from_file(file_name):
        movies, job_id = _parse_input_xml(file_name)
        return InputXml(movies)


def _to_report(nfiles, attribute_id, report_id):
    # this should have version of the bax/bas files, chemistry
    attributes = [Attribute(attribute_id, nfiles)]
    return Report(report_id, attributes=attributes)


def input_xml_to_report(input_xml):
    # this should have version of the bax/bas files, chemistry
    return _to_report(len(input_xml.movies), "nfofns", "input_xml_report")


def fofn_to_report(nfofns):
    return _to_report(nfofns, "nfofns", "fofn_report")


def _parse_input_xml(file_name):
    """Get the movies and job_id from an input xml.

    The movies must be given as absolute path.
    The job_id is optional (None if not found)

    :returns: tuple of (movies, job_id)
    """

    if not os.path.exists(file_name):
        msg = "Unable to find InputXML file {x}".format(x=file_name)
        raise IOError(msg)

    t = ElementTree(file=file_name)

    r = t.getroot()

    job_id = None
    # try to get header for jobId
    header_elts = r.findall('header')
    if header_elts:
        if len(header_elts) != 1:
            warnings.warn("Malformed XML {f}. Multiple headers ({n}) elements found.".format(n=len(header_elts), f=file_name))

        header_el = header_elts[0]

        if 'id' in header_el.attrib:
            job_id = header_el.attrib['id']
        else:
            warnings.warn("Malformed XML {f}. 'id' attr is not in header".format(f=file_name))

    movies = []
    for i in r.findall('dataReferences'):
        # this format is way too loose.
        if i.findall('url'):
            for j in i.findall('url'):
                for k in j.findall('location'):
                    # print i, j, k.text
                    movies.append(k.text)
        elif i.findall('data'):
            if i.findall('data'):
                for j in i.findall('data'):
                    for k in j.findall('location'):
                        # print i, j, k.text
                        movies.append(k.text)
        else:
            msg = "Malformed XML. url, or data are expected in dataReferences."
            log.error(msg)
            raise ParseError(msg)

    if len(movies) == 0:
        log.warn("Successfully parsed '{x}', but unable to find any movies.".format(x=file_name))

    if not all([os.path.exists(m) for m in movies]):
        outs = ["Error loading InputXml file '{f}'".format(f=file_name)]
        for m in movies:
            if not os.path.exists(m):
                outs.append("Unable to find '{m}'".format(m=m))

        msg = "\n".join(outs)
        log.error(msg)
        raise IOError(msg)

    return movies, job_id
