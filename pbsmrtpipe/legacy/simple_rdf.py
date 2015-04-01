__doc__ = """Module for writing a RDF/XML file, specifically tailored to the
metadata.rdf file in smrtpipe.

Taken from pbpy.SMRTpipe.SimpleRdf
"""
import os
import time
import mimetypes
import logging

from xml.etree.ElementTree import tostring, Element, ElementTree

log = logging.getLogger(__name__)

NAMESPACES = {
    'smrtowl': 'http://www.pacificbiosciences.com/SMRT.owl#',
    'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'dcterms': 'http://purl.org/dc/terms/'
}

RDF_NODE_COMMENT = "<!-- Meta-data providing a machine-readable description of a secondary analysis job -->"
DEFAULT_URI_HOST_NAME = "http://www.pacificbiosciences.com"
ANALYSIS_JOB_NAMESPACE = "SecondaryAnalysisJob"
LANGUAGE_FOR_DESCRIPTORS = 'en-US'
SMRTOWL_NS = 'smrtowl:'

#
# these mime types are used for guessing the mime type when it's not
# otherwise specified and for exposing a dc:format in RDF
#
MIME_TYPES = {
    'bed': 'text/plain',
    'cmp.h5': 'application/x-hdf',
    'csv': 'text/csv',
    'fa': 'text/plain',
    'fasta': 'text/plain',
    'fastq': 'text/plain',
    'gff': 'application/x-gff3',
    'gz': 'application/x-gzip',
    # technically gff.gz should have its own mimetype (subclass of x-gzip)
    'gff.gz': 'application/x-gzip',
    'html': 'text/html',
    'h5': 'application/x-hdf',
    'jnlp': 'application/x-java-jnlp-file',
    'sam': 'text/plain',
    'sql': 'text/x-sql',
    'tsv': 'text/tab-separated-values',
    'txt': 'text/plain',
    # if we don't register vcf then the default mimetype is x-vcard (whoops!)
    'vcf': 'text/plain',
    'xml': 'text/xml',
    'png': 'image/png',
    '__default__': 'text/plain',
    'scf.fasta': 'text/plain',
    'bridgemapper.gz': 'application/x-gzip',
    # http://stackoverflow.com/a/477819
    'json': 'application/json'
}

last_id = 0


def _id_generator():
    """Used internally for generating
    node IDs.  Not thread-safe"""
    global last_id
    last_id += 1
    return 'S%04X' % last_id


def mime_type(mime_format):
    """Return the best guessed mime type for a particular
    mime_format (extension).
    mime_format is the extension of the file (no leading '.')"""
    if mime_format in MIME_TYPES:
        return MIME_TYPES[mime_format]
        # on the off chance that we don't have a pre-selected mime type
    # but there's still a good mime type choice
    mime_type, encoding = mimetypes.guess_type('.' + mime_format)
    if mime_type is not None:
        return mime_type
    return mime_format


def ns(name):
    return SMRTOWL_NS + name


class MetaDataGraph(object):

    """Parent class for modeling an RDF graph containing
    meta-data information about a SMRTpipe job.  The design
    is written purely towards simple marshalling to string (XML)."""

    def __init__(self):
        self._uriHostName = DEFAULT_URI_HOST_NAME
        self._elements = []

    @property
    def uriHostName(self):
        return self._uriHostName

    def setUriHostName(self, uriHostName):
        self._uriHostName = uriHostName

    def _startRdfNode(self):
        outs = list()
        outs.append(RDF_NODE_COMMENT)
        outs.append('<rdf:RDF')
        for ns, urn in NAMESPACES.iteritems():
            outs.append('  xmlns:%s="%s"' % (ns, urn))
        outs.append('  xml:base="%s/SMRT/%s/">' % (self.uriHostName, ANALYSIS_JOB_NAMESPACE))
        return os.linesep.join(outs)

    def addElement(self, element):
        "element is an object which implements toXml()"
        self._elements.append(element)

    def _endRdfNode(self):
        return '</rdf:RDF>'

    def toXml(self):
        outs = list()
        outs.append('<?xml version="1.0" encoding="UTF-8"?>')
        outs.append(self._startRdfNode())
        # MK Note:
        # SmrtFiles with different state get registered as the
        # the same URI. This adds duplicate links in smrtportal.
        # This might not generate exactly what is expected.
        # Fundamentally, there's no XML constraint on having
        # the same XML element.
        for e in set(self._elements):
            outs.append(e.toXml())
        outs.append(self._endRdfNode())
        return os.linesep.join(outs)


class SimpleXmlNode(object):

    """
    Wrapper around an Element from the ElementTree API. Allows non-Element
    objects to be added to a MetaDataGraph (if the need arises).
    """

    def __init__(self, element):
        """

        :param element: ElementTree.Element instance
        """
        self._element = element

    def addSimpleElement(self, tag, attrib):
        """Adds a simple XML element with
        name tag and attributes from the attrib dictionary

        :param tag: str
        :param attrib: value

        """
        e = Element(tag, attrib)
        self._element.append(e)

    def addTextElement(self, tag, text, attrib=None):
        """Adds a simple XML element with name tag and text"""

        if attrib is None:
            attrib = {}

        e = Element(tag, attrib)
        e.text = text
        self._element.append(e)

    def toXml(self):
        return tostring(self._element)

    def __eq__(self, other):
        """Overwrite equal to be if the XML str is the same

        .. note :: This may not work as expected in subclasses due to
        automatic assignment of nodeID via _id_generator
        """
        return self.toXml() == other.toXml()

    def __ne__(self, other):
        return not self.__eq__(other)


class SimpleRdfNode(SimpleXmlNode):

    def __init__(self, about=None):
        SimpleXmlNode.__init__(self, Element('rdf:Description'))
        self.id = _id_generator()
        self.about = about
        if self.about:
            self._element.attrib['rdf:about'] = self.about
        else:
            self._element.attrib['rdf:nodeID'] = self.id


class CompoundRdfNode(SimpleRdfNode):

    def __init__(self, parent, about=None):
        """

        :param parent: MetaDataGraph
        :param about: str
        """
        SimpleRdfNode.__init__(self, about=about)
        self._parent = parent

    def addTriple(self, predicate, obj, isGlobal=False):
        if obj.about:
            self.addSimpleElement(
                ns(predicate), {'rdf:resource': obj.about})
        else:
            self.addSimpleElement(ns(predicate), {'rdf:nodeID': obj.id})

        if not isGlobal:
            self._parent.addElement(obj)


class MainJobNode(CompoundRdfNode):

    def __init__(self, parent, jobId):
        CompoundRdfNode.__init__(self, parent, about=jobId)

    def setJobName(self, jobName):
        self.addTextElement('dc:title', jobName)

    def setCreated(self, timestamp):
        """timestamp should be a 9-valued tuple [i.e. from time module]"""
        ts_str = time.strftime('%Y-%m-%dT%H:%M:%S', timestamp)
        self.addTextElement('dcterms:created', ts_str)

    def addReport(self, report):
        """Adds a reference to this report node and adds the report
        node to the overall RDF graph."""
        self.addTriple('hasReport', report)

    def addData(self, data):
        self.addTriple('hasData', data)

    def addInputs(self, inputs):
        self.addTriple('hasInputs', inputs)

    def addSmrtpipeConfigs(self, settingNode):
        self.addTriple('hasSmrtPipeConfigs', settingNode)


class FileNode(SimpleRdfNode):

    def __init__(self, jobId, idx, local, format_, title=None):
        """

        :param jobId: JobId assigned from SmrtPortal or Martin
        :param id: Unique identifier used in the "about"= of rdf
        :param local: local path to file "01234/results/report.xml"
        :param format: xml, fasta
        :param title:
        """
        SimpleRdfNode.__init__(self, about='%s/%s.%s' %
                                           (jobId, idx, format_))
        if title:
            self.addTextElement('dc:title', title,
                                {'xml:lang': LANGUAGE_FOR_DESCRIPTORS})
        self.addTextElement(ns('local'), local)
        self.addTextElement('dc:format', mime_type(format_))


class MeasuredValueNode(SimpleRdfNode):

    def __init__(self, jobId, idx, title, value, hidden=False):
        SimpleRdfNode.__init__(self, about='%s/attribute/%s' % (jobId, idx))
        self.addTextElement('dc:title', title,
                            {'xml:lang': LANGUAGE_FOR_DESCRIPTORS})
        self.addTextElement(ns('hasValue'), value)
        self.addTextElement(ns('isForReportsDisplay'), str(not hidden).lower())


class JobStateNode(SimpleRdfNode):

    def __init__(self, jobState, errorMsg=''):
        SimpleRdfNode.__init__(self)
        self.addTextElement(ns('secondaryAnalysisJobState'), jobState)
        self.addTextElement(ns('secondaryAnalysisErrorMessage'), errorMsg)


class ReportNode(CompoundRdfNode):

    def __init__(self, parent, title, group='Default'):
        """

        :param parent: (MetaDataGraph)
        :param title: (str) Title of report
        :param group: (str) Which group the report will be displayed in
        """
        CompoundRdfNode.__init__(self, parent)
        self.addTextElement('dc:title', title,
                            {'xml:lang': LANGUAGE_FOR_DESCRIPTORS})
        self.addTextElement(ns('inReportGroup'), group)

        # this might not be the best idea to store this...
        # it's really only for the repr
        self._group = group
        self._title = title

    def addLink(self, fileNode):
        self.addTriple('hasLink', fileNode)

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.id,
                  t=self._title,
                  g=self._group)
        return "<{k} {i} group:{g} title:{t} >".format(**_d)


class DataNode(CompoundRdfNode):

    def __init__(self, parent, title, group='Default'):
        CompoundRdfNode.__init__(self, parent)
        self.addTextElement('dc:title', title,
                            {'xml:lang': LANGUAGE_FOR_DESCRIPTORS})
        self.addTextElement(ns('inDataGroup'), group)

    def addLink(self, fileNode):
        self.addTriple('hasLink', fileNode)


class InputsNode(CompoundRdfNode):

    def __init__(self, parent, jobId):
        CompoundRdfNode.__init__(self, parent, about='%s/inputs' % jobId)

    def addInput(self, inputNode):
        self.addTriple('hasInput', inputNode)


class InputNode(SimpleRdfNode):

    def __init__(self, format="", local="", source="", sourceVersion=""):
        SimpleRdfNode.__init__(self)
        if format:
            self.addTextElement('dc:format', format)
        if local:
            self.addTextElement(ns('local'), local)
        if source:
            self.addTextElement(ns('source'), source)
        if sourceVersion:
            self.addTextElement(ns('sourceVersion'), sourceVersion)


class SmrtPipeConfigNode(SimpleRdfNode):

    def __init__(self, jobId, key, value):
        SimpleRdfNode.__init__(self, about='%s/smrtPipeConfig/%s' % (jobId, key))
        self.addTextElement('dc:title', key,
                            {'xml:lang': LANGUAGE_FOR_DESCRIPTORS})
        self.addTextElement(ns('hasValue'), value)


class SmrtPipeConfigsNode(CompoundRdfNode):

    def __init__(self, parent, jobId):
        about = '%s/smrtPipeConfigs/' % jobId
        CompoundRdfNode.__init__(self, parent, about=about)

    def addNode(self, smrtpipeConfigNode):
        self.addTriple('hasSmrtPipeConfig', smrtpipeConfigNode)


class SmrtAnalysisVersionNode(SimpleRdfNode):

    def __init__(self, version, build):
        SimpleRdfNode.__init__(self)
        self.addTextElement(ns('secondaryAnalysisVersion'), version)
        self.addTextElement(ns('secondaryAnalysisBuild'), build)


def rdfns(s):
    return "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}%s" % s


def dcns(s):
    return "{http://purl.org/dc/elements/1.1/}%s" % s


def owlns(s):
    return "{http://www.pacificbiosciences.com/SMRT.owl#}%s" % s


def measuredValuesFromFile(fn):
    """
    Putting this in now, we'll eventually want to build the full parser
    and use that instead.
    """
    tree = ElementTree()
    tree.parse(fn)
    for element in tree.findall(rdfns("Description")):
        if rdfns("about") in element.attrib and "attribute" in element.attrib[rdfns("about")].split("/"):
            attr = "/".join(element.attrib[rdfns("about")].split("/")[2:])
            k, v = None, None
            for child in element:
                if child.tag == dcns("title"):
                    k = child.text
                if child.tag == owlns("hasValue"):
                    v = child.text
            if k is not None and v is not None:
                yield attr, k, v


def test():
    graph = MetaDataGraph()
    jobId = '000001'
    jobNode = MainJobNode(graph, jobId)
    graph.addElement(jobNode)
    jobNode.setJobName('C elegans fosmid accuracy bakeoff - Orb')
    jobNode.setCreated(time.localtime())

    report1 = ReportNode(graph, 'Sequencing Quality Report', group='Basic')
    # reports need to be filled out before added
    report1h = FileNode(jobId, 'results/quality_report',
                        'results/composite.pseduo_quality.html', 'html')
    report1x = FileNode(jobId, 'results/quality_report',
                        'results/composite.pseduo_quality.xml', 'xml')
    report1.addLink(report1h)
    report1.addLink(report1x)
    jobNode.addReport(report1)

    data1 = DataNode(graph, 'Quality Statistics by Movie', group='Quality')
    data1c = FileNode(jobId, 'data/composite_pseudo_z_qv',
                      'data/composite_pseudo_z_qv.csv', 'csv')
    data1h = FileNode(jobId, 'data/composite_pseudo_z_qv',
                      'data/composite_pseudo_z_qv.h5', 'cmp.h5')
    data1.addLink(data1c)
    data1.addLink(data1h)
    jobNode.addData(data1)

    log = FileNode(jobId, 'log/smrtpipe', 'log/smrtpipe.log', 'txt')
    jobNode.addTriple('hasAnalysisLog', log)

    jnlp = FileNode(jobId, 'vis', 'vis.jnlp', 'jnlp')
    jobNode.addTriple('hasAuxiliaryFile', jnlp)

    input1 = FileNode(jobId, 'input', 'input.xml', 'xml')
    jobNode.addTriple('hasAnalysisInputs', input1)

    settings1 = FileNode(jobId, 'settings', 'settings.xml', 'xml')
    jobNode.addTriple('hasAnalysisSettings', settings1)

    reference = SimpleRdfNode(
        'http://www.pacificbiosciences.com/SMRT/Reference/1_WRM0639cE06')
    jobNode.addTriple('usesReference', reference, isGlobal=True)

    state = JobStateNode(
        'Failed', "Can't find m060809_Jan_110021_b30.pls.h5")
    jobNode.addTriple('hasJobState', state)

    jobNode.addTriple('hasMeasuredValue',
                      MeasuredValueNode(jobId, 'mean_readlength', 'Mean Readlength', '100'))

    inputs = InputsNode(graph, jobId)
    inputNode = InputNode(format='h5', local='m231111_211142_Jan_s0_p1.bas.h5')
    inputs.addInput(inputNode)
    jobNode.addInputs(inputs)

    smrtpipeConfigs = SmrtPipeConfigsNode(graph, jobId)
    c1 = SmrtPipeConfigNode(jobId, 'NJOBS', str(12))
    c2 = SmrtPipeConfigNode(jobId, 'CLUSTER_MANANGER', 'BASH')
    c3 = SmrtPipeConfigNode(jobId, 'NPROC', str(1234))
    cs = [c1, c2, c3]
    for c in cs:
        smrtpipeConfigs.addNode(c)
    jobNode.addSmrtpipeConfigs(smrtpipeConfigs)

    smrtAnalysisVersion = SmrtAnalysisVersionNode('v2.1.0', '12345')
    jobNode.addTriple('hasAnalysisVersion', smrtAnalysisVersion)

    xml = graph.toXml()

    file_name = 'metadata.rdf'
    with open(file_name, 'w+') as f:
        f.write(xml)
    print xml

if __name__ == '__main__':
    test()
