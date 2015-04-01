"""IO Layer for accessing data in the Metadata.rdf file

Originally written by Rudy, modified and repurposed my MK.
"""
__author__ = 'rrico'
__date__ = 'March 6, 2010'

import os
import sys
import logging
import collections
import warnings

import rdflib

rdflib.plugin.register('sparql', rdflib.query.Processor,
                       'rdfextras.sparql.processor', 'Processor')
rdflib.plugin.register('sparql', rdflib.query.Result,
                       'rdfextras.sparql.query', 'SPARQLQueryResult')

log = logging.getLogger(__name__)


RDF_DELIM = "/"

RDF_DCTERMS = "dcterms"
RDF_SMRTOWL = "smrtowl"
RDF_DC = "dc"
RDF_RDF = "ref"
RDF_BASE = "base"

RDF_URI_DCTERMS = "http://purl.org/dc/terms/"
RDF_URI_SMRTOWL = "http://www.pacificbiosciences.com/SMRT.owl#"
RDF_URI_DC = "http://purl.org/dc/elements/1.1/"
RDF_URI_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDF_URI_BASE = "http://www.pacificbiosciences.com/SMRT/SecondaryAnalysisJob/"

RDF_NS_DCTERMS = rdflib.Namespace(RDF_URI_DCTERMS)
RDF_NS_SMRTOWL = rdflib.Namespace(RDF_URI_SMRTOWL)
RDF_NS_DC = rdflib.Namespace(RDF_URI_DC)
RDF_NS_RDF = rdflib.Namespace(RDF_URI_RDF)
RDF_NS_BASE = rdflib.Namespace(RDF_URI_BASE)

RDF_NS_STRINGS = [RDF_DCTERMS, RDF_SMRTOWL, RDF_DC, RDF_RDF, RDF_BASE]
RDF_NS_NAMESPACES = [RDF_NS_DCTERMS, RDF_NS_SMRTOWL, RDF_NS_DC, RDF_NS_RDF,
                     RDF_NS_BASE]
RDF_NS_DICT = dict(zip(RDF_NS_STRINGS, RDF_NS_NAMESPACES))

#elements & attributes
RDF_DESC = "%s:Description" % RDF_RDF
RDF_ABOUT = "%s:about" % RDF_RDF
RDF_NODE = "%s:nodeID" % RDF_RDF

RDF_TITLE = "%s:title" % RDF_DC
RDF_CREATED = "%s:created" % RDF_DCTERMS

RDF_REPORT = "%s:hasReport" % RDF_SMRTOWL
RDF_DATA = "%s:hasData" % RDF_SMRTOWL
RDF_AUX = "%s:hasAuxiliaryFile" % RDF_SMRTOWL
RDF_LOG = "%s:hasAnalysisLog" % RDF_SMRTOWL
RDF_VALUE = "%s:hasValue" % RDF_SMRTOWL
RDF_MEASUREDVALUE = "%s:hasMeasuredValue" % RDF_SMRTOWL
RDF_JOBSTATE = "%s:hasJobState" % RDF_SMRTOWL
RDF_REF = "%s:usesReference" % RDF_SMRTOWL
RDF_RESOURCE = "%s:resource" % RDF_RDF
RDF_ABOUT = "%s:about" % RDF_RDF
RDF_ANALYSIS_VERSION = "%s:hasAnalysisVersion" % RDF_RDF
RDF_SMRTPIPE_CONFIG = "%s:hasSmrtPipeConfig" % RDF_RDF

RDF_ATTR_ABOUT_ATTRIBUTE = "attribute"  # contained in some "about" attribute
# values

RDF_RAW_LOCAL = "local"
RDF_RAW_FORMAT = "format"
RDF_LOCAL = "%s:%s" % (RDF_SMRTOWL, RDF_RAW_LOCAL)
RDF_FORMAT = "%s:%s" % (RDF_DC, RDF_RAW_FORMAT)
RDF_GROUPINFO_LOCAL = "{%s}%s" % (RDF_URI_SMRTOWL, RDF_RAW_LOCAL)
RDF_GROUPINFO_FORMAT = "{%s}%s" % (RDF_URI_DC, RDF_RAW_FORMAT)

RDF_ISFORRPTDISPLAY = "%s:isForReportsDisplay" % RDF_SMRTOWL

RDF_INDATAGROUP = "%s:%s" % (RDF_SMRTOWL, "inDataGroup")
RDF_INRPTGROUP = "%s:%s" % (RDF_SMRTOWL, "inReportGroup")
RDF_HASLINK = "%s:hasLink" % RDF_SMRTOWL

RDF_SECJOBSTATE = "%s:secondaryAnalysisJobState" % RDF_SMRTOWL
RDF_SECERROR = "%s:secondaryAnalysisErrorMessage" % RDF_SMRTOWL


# expected results
RDF_FORMAT_H5 = "application/x-hdf"
RDF_FORMAT_GFF = "application/x-gff3"
RDF_FORMAT_TXT = "text/plain"
RDF_FORMAT_JNLP = "application/x-java-jnlp-file"
RDF_FORMAT_VCF = RDF_FORMAT_TXT
RDF_FORMAT_BED = RDF_FORMAT_TXT
RDF_FORMAT_HTML = "text/html"
RDF_FORMAT_SAM = RDF_FORMAT_TXT
RDF_FORMAT_BAM = "bam"
RDF_FORMAT_BAI = "bam.bai"
RDF_FORMAT_GZ = "application/x-gzip"
RDF_FORMAT_XML = "text/xml"
RDF_FORMAT_FASTA = RDF_FORMAT_TXT
RDF_FORMAT_CSV = "text/csv"
RDF_FORMAT_ACE = "ace"
RDF_FORMAT_GML = "gml"
RDF_FORMAT_FASTQGZ = "fastq.gz"
RDF_FORMAT_CSVGZ = "csv.gz"

#------------------------
# file extensions
#------------------------

BAS_EXT = ".bas.h5"
PLS_EXT = ".pls.h5"
H5_EXT = ".h5"
TXT_EXT = ".txt"
GFF_EXT = ".gff"
LOG_EXT = ".log"
XML_EXT = ".xml"
JNLP_EXT = ".jnlp"
VCF_EXT = ".vcf"
PNG_EXT = ".png"
THMB_PNG_EXT = "_thmb.png"
BED_EXT = ".bed"
HTML_EXT = ".html"
SAM_EXT = ".sam"
BAM_EXT = ".bam"
BAI_EXT = ".bai"
GZ_EXT = ".gz"
FASTA_EXT = ".fasta"
CSV_EXT = ".csv"
ACE_EXT = ".ace"
GML_EXT = ".gml"

RDF_FORMAT_MAP = {H5_EXT: RDF_FORMAT_H5,
                  GFF_EXT: RDF_FORMAT_GFF,
                  LOG_EXT: RDF_FORMAT_TXT,
                  TXT_EXT: RDF_FORMAT_TXT,
                  JNLP_EXT: RDF_FORMAT_JNLP,
                  VCF_EXT: RDF_FORMAT_VCF,
                  BED_EXT: RDF_FORMAT_BED,
                  HTML_EXT: RDF_FORMAT_HTML,
                  SAM_EXT: RDF_FORMAT_SAM,
                  BAM_EXT: RDF_FORMAT_BAM,
                  BAI_EXT: RDF_FORMAT_BAI,
                  GZ_EXT: RDF_FORMAT_GZ,
                  XML_EXT: RDF_FORMAT_XML,
                  FASTA_EXT: RDF_FORMAT_FASTA,
                  CSV_EXT: RDF_FORMAT_CSV,
                  ACE_EXT: RDF_FORMAT_ACE,
                  GML_EXT: RDF_FORMAT_GML
                  }

RDF_FORMATS_GZS = [RDF_FORMAT_FASTQGZ,
                   RDF_FORMAT_CSVGZ]  # gz files that don't use RDF_FORMAT_GZ

# indices for file format tuples
RDF_FILE_PATH_INDEX = 0
RDF_FILE_FORMAT_INDEX = 1


# expected RDF data
#RDF_JOBSTATE_PASSED = "Passed"
# RDF_AUX_FILES = [os.path.join(filelayout.FOLDERNAME_RESULTS,
# filelayout.OVERVIEW_FILE),\
#                 filelayout.JOB_JNLP_FILE]


class MetaDataRDF(object):

    '''
    Object representation of the metadata.rdf file generated by SMRTPipe
    '''

    def __init__(self, file_path):
        '''
        constructor
        file_path: path to metadata file
        '''
        self._file_path = file_path
        if os.path.exists(file_path):
            self._graph = rdflib.Graph()
            self._graph.parse(file_path)

        self._jobid = self.get_job_id()

    def get_job_id(self):
        '''
        Get job id
        returns: 6-digit job id; None if not found
        '''
        jobid = None
        search_string = str(RDF_NS_BASE)
        for subj in self._graph.subjects():
            subj = str(subj)
            if subj.find(search_string) == 0:
                fragment_identifier = subj.replace(search_string, "")
                jobid = fragment_identifier.split(RDF_DELIM)[0]
                break
        return jobid

    def get_job_name(self):
        '''
        Get job name
        '''
        query_string = 'SELECT ?jobname WHERE {?id %s ?jobname; %s ?created' \
                       '.}' % (
                           RDF_TITLE, RDF_CREATED)
        job_name = self._process_list_query(query_string)
        if len(job_name) > 0:
            return job_name[0]
        else:
            return None

    def _get_reference_id(self):
        """
        get reference name

        MK: This should be deprecated. It's often just reference.info.xml due
        to the 1.4 changes in ref repo.

        .. note:: This method will be Deprecated.
        """
        warnings.warn("This method should not be used. It will be deprecated",
                      DeprecationWarning)

        ref_name = None
        query_string = 'SELECT ?refname WHERE {?id %s ?refname.}' % RDF_REF
        full_ref_name = self._process_list_query(query_string)

        #import ipdb; ipdb.set_trace()

        # if full_ref_name is not None:
        if full_ref_name:
            ref_name = str(full_ref_name[0]).split(RDF_DELIM)[-1]
        else:
            log.warn("Unable to find reference in {f}. using query {q}".format(f=self._file_path, q=query_string))
        return ref_name

    def get_reference(self):
        """
        Get reference full path from metadata.rdf. If no usesReference exists
        in the metadata.rdf, then None is returned.

        """
        ref_name = None
        query_string = 'SELECT ?refname WHERE {?id %s ?refname.}' % RDF_REF
        full_ref_name = self._process_list_query(query_string)

        # if full_ref_name is not None:
        if full_ref_name:
            ref_name = full_ref_name[0]
        else:
            log.warn("Unable to find reference in {f}. using query {q}".format(f=self._file_path, q=query_string))
        return ref_name

    def get_formats(self):
        '''
        Get dictionary of files and respective formats
        returns: dictionary; 1 entry per Description entry corresponding to
        a file;
        key=file relative path, value=format
        '''

        result = {}
        file_ids = [RDF_DATA, RDF_REPORT]
        for file_id in file_ids:
            query_string = 'SELECT ?filename ?format WHERE {?id %s ?a. ?a %s' \
                           ' ?b. ?b %s ?filename; %s ?format.}' % (
                               file_id, RDF_HASLINK, RDF_LOCAL, RDF_FORMAT)
            this_result = self._process_kv_query(query_string)
            result = dict(result.items() + this_result.items())

        file_ids = [RDF_AUX, RDF_LOG]
        for file_id in file_ids:
            query_string = 'SELECT ?filename ?format WHERE {?id %s ?a. ?a %s' \
                           ' ?filename; %s ?format.}' % (
                               file_id, RDF_LOCAL, RDF_FORMAT)
            this_result = self._process_kv_query(query_string)
            result = dict(result.items() + this_result.items())
        return result

    def get_log_paths(self):
        """Return a {name:path} of all the log files"""
        query_string = 'SELECT ?filename WHERE {?id %s ?a. ?a %s ?filename.}' % (RDF_LOG, RDF_LOCAL)
        log.debug(query_string)
        result = self._process_kv_query(query_string)
        return result

    def get_file_paths(self, run_path):
        '''
        get paths of all file resources
        run_path: root of run in file system
        return: list of full file paths
        '''
        result = []
        file_ids = [RDF_DATA, RDF_REPORT]
        for file_id in file_ids:
            query_string = 'SELECT ?filename WHERE {?id %s ?a. ?a %s ?b. ?b ' \
                           '%s ?filename.}' % (
                               file_id, RDF_HASLINK, RDF_LOCAL)
            this_result = self._process_list_query(query_string)
            result.extend(this_result)

        file_ids = [RDF_AUX, RDF_LOG]
        for file_id in file_ids:
            query_string = 'SELECT ?filename WHERE {?id %s ?a. ?a %s ' \
                           '?filename.}' % (
                               file_id, RDF_LOCAL)
            this_result = self._process_list_query(query_string)
            result.extend(this_result)

        result = [os.path.join(run_path, c) for c in result]
        return result

    def get_all_files(self):
        """Grab all files and return a list"""

        results = []
        file_ids = [RDF_DATA, RDF_REPORT]

        for file_id in file_ids:
            query_string = 'SELECT ?filename WHERE {?id %s ?a. ?a %s ?b. ?b ' \
                           '%s ?filename.}' % (
                               file_id, RDF_HASLINK, RDF_LOCAL)
            this_result = self._process_list_query(query_string)
            results.extend(this_result)

        file_ids = [RDF_AUX, RDF_LOG]
        for file_id in file_ids:
            query_string = 'SELECT ?filename WHERE {?id %s ?a. ?a %s ' \
                           '?filename.}' % (
                               file_id, RDF_LOCAL)
            this_result = self._process_list_query(query_string)
            results.extend(this_result)

        return results

    def get_state(self):
        """
        Get state of job
        """
        query_string = 'SELECT ?state WHERE {?id %s ?a. ?a %s ?state}' % (RDF_JOBSTATE, RDF_SECJOBSTATE)
        job_state = self._process_list_query(query_string)
        if len(job_state) > 0:
            return job_state[0]
        else:
            return None

    def get_version(self):
        query = 'SELECT ?version WHERE {?id smrtowl:hasAnalysisVersion ?a. ?a smrtowl:secondaryAnalysisVersion ?version}'
        r = self._graph.query(query, initNs=RDF_NS_DICT)
        results = list(r)
        if results:
            return results[0].encode()
        else:
            return None

    def get_changelist(self):
        query = 'SELECT ?version WHERE {?id smrtowl:hasAnalysisVersion ?a. ?a smrtowl:secondaryAnalysisBuild ?version}'
        r = self._graph.query(query, initNs=RDF_NS_DICT)
        results = list(r)
        if results:
            return int(results[0].encode())
        else:
            return -1

    def was_job_successful(self):
        """Return True/False if state = 'Passed' and error messages is None"""
        state = True if self.get_state() == 'Passed' else False
        return state

    def get_error(self):
        '''
        Get error associated with job state
        '''
        query_string = 'SELECT ?msg WHERE {?id %s ?a. ?a %s ?msg}' % (
            RDF_JOBSTATE, RDF_SECERROR)
        msg = self._process_list_query(query_string)
        if len(msg) > 0:
            return msg[0]
        else:
            return None

    def get_log_info(self):
        '''
        Gets log info
        returns: tuple - (path, format)
        '''
        query_string = 'SELECT ?filename ?format WHERE {?id %s ?a. ?a %s ?filename; %s ?format.}' % (RDF_LOG, RDF_LOCAL, RDF_FORMAT)
        result = self._process_kv_query(query_string)
        result = result.items()
        if result is not None:
            return result[0]
        else:
            return None

    def get_aux_info(self):
        '''
        Gets auxiliary file info
        returns: list of tuples - (path, format)
        '''
        query_string = 'SELECT ?filename ?format WHERE {?id %s ?a. ?a %s ' \
                       '?filename; %s ?format.}' % (
                           RDF_AUX, RDF_LOCAL, RDF_FORMAT)
        result = self._process_kv_query(query_string)
        return result.items()

    def get_group_info(self, group, type):
        '''
        Get info for a given group
        group: specific group to find
        type: data or report group
        return: dictionary of title:linksList for a group
        '''
        results = collections.defaultdict(list)

        if type == RDF_INDATAGROUP:
            datatype = RDF_DATA
        else:
            datatype = RDF_REPORT

        query_string = 'SELECT ?title ?local WHERE {?id %s ?a. ?a %s ?title;' \
                       ' %s "%s"; %s ?b. ?b %s ?local.}' % (
                           datatype, RDF_TITLE, type, group, RDF_HASLINK, RDF_LOCAL)
        qres = self._graph.query(query_string, initNs=RDF_NS_DICT)
        for row in qres.result:
            (this_title, this_local) = [str(c).strip() for c in row]
            results[this_title].append(this_local)

        return dict(results)

    def get_group_info_with_format(self, group, type):
        '''
        Get info for a given group
        group: specific group to find
        type: data or report group
        return: dictionary of title:{link, format} for a group
        '''
        results = collections.defaultdict(list)

        if type == RDF_INDATAGROUP:
            datatype = RDF_DATA
        else:
            datatype = RDF_REPORT

        query_string = 'SELECT ?title ?local ?format WHERE {?id %s ?a. ?a %s' \
                       ' ?title; %s "%s"; %s ?b. ?b %s ?local; %s ?format.}' \
                       % (
                           datatype, RDF_TITLE, type, group, RDF_HASLINK, RDF_LOCAL, RDF_FORMAT)
        qres = self._graph.query(query_string, initNs=RDF_NS_DICT)
        for row in qres.result:
            (this_title, this_local, this_format) = [str(c).strip() for c in
                                                     row]
            results[this_title].append({RDF_GROUPINFO_LOCAL: this_local,
                                        RDF_GROUPINFO_FORMAT: this_format})

        return dict(results)

    def get_attribute_values(self):
        '''
        Returns attributes
        returns: dictionary of attribute id:value
        '''
        query_string = 'SELECT ?key ?value WHERE {?id %s ?key. ?key %s ' \
                       '?value.}' % (
                           RDF_MEASUREDVALUE, RDF_VALUE)
        untrimmed_results = self._process_kv_query(query_string)
        results = {}
        for untrimmed_key in untrimmed_results.keys():
            key = untrimmed_key.split(RDF_DELIM)[-1]
            results[key] = untrimmed_results[untrimmed_key]
        return results

    def get_attribute_titles(self):
        '''
        Returns attributes
        returns: dictionary of attribute id:value

        SELECT ?key ?value WHERE {
                                    ?id %s ?key.
                                    ?key %s ?value.
                                    }

        '''
        query_string = 'SELECT ?key ?value WHERE {?id %s ?key. ?key %s ' \
                       '?value.}' % (
                           RDF_MEASUREDVALUE, RDF_TITLE)
        untrimmed_results = self._process_kv_query(query_string)
        results = {}
        for untrimmed_key in untrimmed_results.keys():
            key = untrimmed_key.split(RDF_DELIM)[-1]
            results[key] = untrimmed_results[untrimmed_key]
        return results

    def get_attribute_title_values(self):
        '''
        Get dictionary of attribute title:value; deprecated - use get_metrics
        '''
        return self.get_metrics()

    def get_metrics(self):
        '''
        Get job metrics
        '''
        query_string = 'SELECT ?metric ?value WHERE {?id %s ?a. ?a %s ' \
                       '?value; %s ?metric.}' % (
                           RDF_MEASUREDVALUE, RDF_VALUE, RDF_TITLE)
        result = self._process_kv_query(query_string)
        return result

    def get_displayed_metrics(self):
        '''
        Get job metrics that should be displayed
        '''
        query_string = 'SELECT ?metric ?value WHERE {?id %s ?a. ?a %s ' \
                       '?value; %s ?metric; %s "true".}' % (
                           RDF_MEASUREDVALUE, RDF_VALUE, RDF_TITLE, RDF_ISFORRPTDISPLAY)
        result = self._process_kv_query(query_string)
        return result

    def to_dict(self):
        """Convert to dict of primitives"""
        metrics = self.get_displayed_metrics()
        status = self.get_state()
        error = self.get_error()
        job_id = self.get_job_id()
        job_name = self.get_job_name()
        reference_id = self._get_reference_id()
        reference = self.get_reference()

        d = dict(metrics=metrics, status=status, error=error, job_id=job_id,
                 job_name=job_name, reference_id=reference_id,
                 reference=reference)
        return d

    def __str__(self):
        """This is terse summary of the Metadata.rdf"""

        dir_name = os.path.dirname(self._file_path)
        header = "-------------"
        pad = 12
        outs = list()
        outs.append(header)
        outs.append("MetadataRdf : {s}".format(s=self._file_path))
        outs.append(header)
        outs.append("job dir     : {s}".format(s=dir_name))
        outs.append("job id      : %s" % self.get_job_id())
        outs.append("job name    : %s" % self.get_job_name())
        outs.append("ref name    : %s" % self._get_reference_id())
        outs.append("ref path    : {s}".format(s=self.get_reference()))
        outs.append("state       : %s" % self.get_state())
        outs.append("error       : %s" % self.get_error())
        outs.append("metrics     : %s" % len(self.get_metrics()))
        outs.append("attribute values : %s" % len(self.get_attribute_values()))
        outs.append("number of General reports : %s" % str(self.get_group_info("General", RDF_INRPTGROUP)))

        #        outs.append("formats: %s" % self.get_formats())
        #        outs.append("file paths: %s" % self.get_file_paths(dir_name))
        #        outs.append("aux: %s" % str(self.get_aux_info()))
        #        outs.append("log: %s" % str(self.get_log_info()))
        #        outs.append("reports General %s" % str(self.get_group_info("General", RDF_INRPTGROUP)))
        #        outs.append("reports General %s" % str(self.get_group_info_with_format("General", RDF_INRPTGROUP)))
        #        outs.append("data Resequencing %s" % str(self.get_group_info_with_format("Resequencing", RDF_INDATAGROUP)))
        #        outs.append("attribute values %s" % self.get_attribute_values())
        #        outs.append("attribute titles %s" % self.get_attribute_titles())
        #        outs.append("attribute titles/values %s" % self.get_attribute_title_values())
        #        outs.append("metrics: %s" % self.get_metrics())
        #        outs.append("metrics (display): %s" % self.get_displayed_metrics())

        return "\n".join(outs)

    def summary(self):
        """Full Summary of MetaDataRdf"""

        dir_name = os.path.dirname(self._file_path)
        header = "-------------"
        pad = 2

        outs = []
        outs.append(header)
        outs.append("MetadataRdf : {s}".format(s=self._file_path))
        outs.append(header)
        outs.append("Job dir           : {s}".format(s=dir_name))
        outs.append("Job id            : %s" % self.get_job_id())
        outs.append("Job name          : %s" % self.get_job_name())
        outs.append("Ref name          : %s" % self._get_reference_id())
        outs.append("Ref path          : %s" % self.get_reference())
        outs.append("State             : %s" % self.get_state())
        outs.append("Error Message     : %s" % self.get_error())
        outs.append("Metrics           : %s" % len(self.get_metrics()))
        outs.append("Attribute values  : %s" % len(self.get_attribute_values()))
        outs.append("# General reports : %s" % len(self.get_group_info("General", RDF_INRPTGROUP)))

        #outs.append("formats: %s" % self.get_formats())
        #outs.append("file paths: %s" % self.get_file_paths(dir_name))
        outs.append("State             : %s" % self.get_state())
        outs.append("Error             : %s" % self.get_error())
        outs.append("Aux               : %s" % str(self.get_aux_info()))
        outs.append("Log               : %s" % str(self.get_log_info()))

        reports = self.get_group_info("General", RDF_INRPTGROUP)

        if reports:
            outs.append(header)
            outs.append("General Reports for {x}".format(x=RDF_INRPTGROUP))
            outs.append(header)
            _max = max(len(x) for x in reports.keys())
            _pad = 4
            for report_category, report_files in reports.iteritems():
                #s = "\t{n} : {fs}".format(n=report_category, fs=report_files)
                s = " ".join(["\t", report_category.rjust(_max + _pad), " : ", str(report_files)])
                outs.append(s)
        else:
            outs.append("No general reports found for {x}".format(x=RDF_INRPTGROUP))

        #outs.append("Reports General   : %s" % str(self.get_group_info("General", RDF_INRPTGROUP)))
        #outs.append("Reports General   :%s" % str(self.get_group_info_with_format("General", RDF_INRPTGROUP)))

        # This has an very nested structure {name:[{},{}]}
        # The lists of dict are:
        # {http://www.pacificbiosciences.com/SMRT.owl#}local : data/coverage.bed
        #   {http://purl.org/dc/elements/1.1/}format : text/plain
        # {http://www.pacificbiosciences.com/SMRT.owl#}local : data/alignment_summary.gff
        #   {http://purl.org/dc/elements/1.1/}format : application/x-gff3

        rq_data = self.get_group_info_with_format("Resequencing", RDF_INDATAGROUP)

        if rq_data:
            outs.append(header)
            outs.append("Resquencing Data for for {x}".format(x=RDF_INDATAGROUP))
            outs.append(header)
            for name, list_of_dicts in rq_data.iteritems():
                outs.append("\t{x}".format(x=name))
                for d in list_of_dicts:

                    # pad based on the values
                    _max = max(len(x) for x in d.values())
                    pad = 4

                    for k, v in d.iteritems():
                        # switching the k-> v. k is http://www.pacificbiosciences.com/SMRT.owl#}local
                        #s = "\t\t{v} : {k}".format(k=v, v=k)
                        s = " ".join(["\t\t", v.rjust(_max + pad), " : ", k])
                        outs.append(s)
        else:
            outs.append("No Resquencing Data found for {x}".format(x=RDF_INDATAGROUP))

        #outs.append("Data Resequencing : %s" % str(self.get_group_info_with_format("Resequencing", RDF_INDATAGROUP)))

        outs.append(header)
        outs.append("Attributes:")
        outs.append(header)

        # Get the max length for formatting
        get_max_length = lambda d: max([len(k) for k in d.keys()])
        # key, value, max length
        to_string = lambda k_, v_, m_: " : ".join([str(k_).rjust(m_ + pad), str(v_)])

        #attrs_dict = self.get_attribute_values()
        attrs_dict = self.get_attribute_title_values()
        max_length = get_max_length(attrs_dict)

        for k, v in self.get_attribute_values().iteritems():
            s = to_string(k, v, max_length)
            outs.append(s)

        #outs.append("attribute values %s" % self.get_attribute_values())
        #outs.append("attribute titles %s" % self.get_attribute_titles())
        #outs.append("attribute titles/values %s" % self.get_attribute_title_values())

        attrs_dict = self.get_metrics()
        max_length = get_max_length(attrs_dict)
        outs.append(header)
        outs.append("Metrics")
        outs.append(header)
        for k, v in self.get_metrics().iteritems():
            s = to_string(k, v, max_length)
            outs.append(s)
            #outs.append("metrics: %s" % self.get_metrics())

        #outs.append("metrics (display): %s" % self.get_displayed_metrics())

        return "\n".join(outs)

    def full_summary(self):
        """Very Verbose Summary of MetaDataRdf"""
        dir_name = os.path.dirname(self._file_path)
        header = "-------------"
        pad = 2

        outs = list()
        outs.append(header)
        outs.append("MetadataRdf : {s}".format(s=self._file_path))
        outs.append(header)
        outs.append("job dir     : {s}".format(s=dir_name))
        outs.append("job id      : %s" % self.get_job_id())
        outs.append("job name    : %s" % self.get_job_name())
        outs.append("ref name    : %s" % self._get_reference_id())
        outs.append("ref path    : %s" % self.get_reference())
        outs.append("state       : %s" % self.get_state())
        outs.append("error       : %s" % self.get_error())
        outs.append("metrics     : %s" % len(self.get_metrics()))
        outs.append("attribute values : %s" % len(self.get_attribute_values()))
        outs.append("number of General reports : %s" % str(self.get_group_info("General", RDF_INRPTGROUP)))

        outs.append("formats: %s" % self.get_formats())
        outs.append("file paths: %s" % self.get_file_paths(dir_name))
        outs.append("state: %s" % self.get_state())
        outs.append("error: %s" % self.get_error())
        outs.append("aux: %s" % str(self.get_aux_info()))
        outs.append("log: %s" % str(self.get_log_info()))
        outs.append("reports General %s" % str(self.get_group_info("General", RDF_INRPTGROUP)))
        outs.append("reports General %s" % str(self.get_group_info_with_format("General", RDF_INRPTGROUP)))
        outs.append("data Resequencing %s" % str(self.get_group_info_with_format("Resequencing", RDF_INDATAGROUP)))
        outs.append(header)
        outs.append("Attributes:")
        outs.append(header)

        # Get the max length for formatting
        get_max_length = lambda d: max([len(k) for k in d.keys()])
        # key, value, max length
        to_string = lambda k_, v_, m_: " : ".join([str(k_).rjust(m_ + pad), str(v_)])

        #attrs_dict = self.get_attribute_values()
        attrs_dict = self.get_attribute_title_values()
        max_length = get_max_length(attrs_dict)

        for k, v in self.get_attribute_values().iteritems():
            s = to_string(k, v, max_length)
            outs.append(s)

        #outs.append("attribute values %s" % self.get_attribute_values())
        #outs.append("attribute titles %s" % self.get_attribute_titles())
        #outs.append("attribute titles/values %s" % self.get_attribute_title_values())

        attrs_dict = self.get_metrics()
        max_length = get_max_length(attrs_dict)
        outs.append(header)
        outs.append("Metrics")
        outs.append(header)
        for k, v in self.get_metrics().iteritems():
            s = to_string(k, v, max_length)
            outs.append(s)
            #outs.append("metrics: %s" % self.get_metrics())

        #outs.append("metrics (display): %s" % self.get_displayed_metrics())

        return "\n".join(outs)

    #
    # internal methods
    #

    def _process_list_query(self, query_string, ns=RDF_NS_DICT):
        """
        run rdf query that returns a list
        query_string: sparql query with 1 param
        ns: namespace dictionary
        returns: list; empty if no match
        """
        result = []

        log.debug("Running query {q}.".format(q=query_string))

        qres = self._graph.query(query_string, initNs=ns)

        for row in qres:
            result.append("%s" % row)

        results = [c.strip() for c in result]
        log.debug(results)

        return results

    def _process_kv_query(self, query_string, ns=RDF_NS_DICT):
        '''
        run rdf query that returns key/value pair and convert to a dictionary
        query_string: sparql query with 2 params - key and value
        ns: namespace dictionary
        returns: key-value dictionary
        '''
        result = {}
        qres = self._graph.query(query_string, initNs=ns)
        for row in qres.result:
            #print ('row', row,)
            (key, value) = row
            result[str(key).strip()] = str(value).strip()
        return result


def metadata_rdf_to_html(metadata_rdf):
    """Returns a html string of the metadata.rdf metrics"""
    md = metadata_rdf
    outs = list()
    outs.append("<html>")
    outs.append("<head>")
    outs.append("<title>Job and Metric summary </title>")
    outs.append("</head>")
    outs.append("<body>")

    outs.append("<h1>MetadataRdf Summary</h1>")

    to_td = lambda x: "<td>{x}</td>".format(x=x)

    summary = [("Job directory", md._file_path),
               ("Job id", md.get_job_id()),
               ("Job name", md.get_job_name()),
               ("Ref name", md._get_reference_id()),
               ("Ref path", md.get_reference()),
               ("State", md.get_state()),
               ("Error Message", md.get_error())]

    outs.append("<h3>Job Summary</h3>")
    outs.append("<table border=\"1\">")

    for k, v in summary:
        outs.append("<tr>")
        s = to_td(k) + to_td(v)
        outs.append(s)
        outs.append("</tr>")
    outs.append("</table>")

    # Print attributes
    outs.append("<h3>Attributes</h3>")
    outs.append("<table border=\"1\">")
    for k, v in md.get_attribute_values().iteritems():
        outs.append("<tr>")
        s = to_td(k) + to_td(v)
        outs.append(s)
        outs.append("</tr>")
    outs.append("</table>")

    # Print Metrics
    outs.append("<h3>Metrics</h3>")
    outs.append("<table border=\"1\">")
    for k, v in md.get_metrics().iteritems():
        outs.append("<tr>")
        s = to_td(k) + to_td(v)
        outs.append(s)
        outs.append("</tr>")
    outs.append("</table>")

    outs.append("</body>")
    outs.append("</html>")
    return "\n".join(outs)


def _main():
    dir_name = os.path.join(os.path.dirname(__file__), 'tests', 'data', 'job_01')
    srcfile = os.path.join(dir_name, 'metadata.rdf')
    rdf = MetaDataRDF(srcfile)
    print ("job id: %s" % rdf.get_job_id())
    print ("job name: %s" % rdf.get_job_name())
    print ("ref name: %s" % rdf._get_reference_id())
    print ("formats: %s" % rdf.get_formats())
    #print ("file paths: %s" % rdf.get_file_paths(s))
    print ("state: %s" % rdf.get_state())
    print ("error: %s" % rdf.get_error())
    print ("aux: %s" % str(rdf.get_aux_info()))
    print ("log: %s" % str(rdf.get_log_info()))
    print (
        "reports General %s" % str(rdf.get_group_info("General", RDF_INRPTGROUP)))
    print ("reports General %s" % str(
        rdf.get_group_info_with_format("General", RDF_INRPTGROUP)))
    print ("data Resequencing %s" % str(
        rdf.get_group_info_with_format("Resequencing", RDF_INDATAGROUP)))
    print ("attribute values %s" % rdf.get_attribute_values())
    print ("attribute titles %s" % rdf.get_attribute_titles())
    print ("attribute titles/values %s" % rdf.get_attribute_title_values())
    print ("metrics: %s" % rdf.get_metrics())
    print ("metrics (display): %s" % rdf.get_displayed_metrics())
    return 0

if __name__ == '__main__':
    sys.exit(_main())
