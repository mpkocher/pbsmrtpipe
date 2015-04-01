#
# $Id: //depot/software/bioinformatics/tools/SMRTpipe/SMRTpipe/SmrtReportService.py#1 $
#
__doc__ = """Provides support for various common functions related to
generating HTML output reports."""

import os
import string
import json
import logging
import shutil
import datetime
import warnings
import urlparse
import xml.sax.saxutils as sx
import xml.etree.cElementTree as etree
from xml.etree.ElementTree import ElementTree, ParseError
import cPickle as pickle

import h5py

#from SMRTpipe.pbpy.io.XmlUtils import xslTransform
#from pbpy.io.MovieHDF5IO import getSoftwareVersion

SMRT_DATA_CMDLINE = 'smrt.data.cmdline'
SMRT_DATA_PLS_FOFN = 'smrt.data.pls.fofn'
DATA_ITEMS_STORE = "data.items.pickle"

from pbsmrtpipe.legacy.simple_rdf import (SimpleRdfNode, FileNode, MainJobNode,
                                          JobStateNode, ReportNode, DataNode,
                                          InputNode, InputsNode, MetaDataGraph,
                                          MIME_TYPES, MeasuredValueNode,
                                          SmrtAnalysisVersionNode,
                                          SmrtPipeConfigsNode, SmrtPipeConfigNode)

from pbsmrtpipe.legacy.input_xml import GraphAttribute
DEEP_DEBUG = False

log = logging.getLogger(__name__)


ATTRS_ORDER = ['not_ready', 'jobId', 'jobName', 'primaryAnalysisProtocol',
               'input', 'settings', 'jobComment', 'xxx']

ATTRS_DISPLAY_NAMES = {
    'not_ready': 'Results not ready',
    'jobId': 'Job ID',
    'jobName': 'Job Name',
    'primaryAnalysisProtocol': 'Primary Protocol',
    'jobComment': 'Job Comment',
    'input': 'Input',
    'settings': 'Settings',
    'advanced': '--'
}

# Defines the set of attributes which determine the state of this class
SMRT_REPORT_STATE = ["reports", "data", "auxiliaryFiles", "extLink",
                     "attributes", "message"]

SAXON_PATH = 'saxonb9'


def xslTransform(xmlFileName, xslFileName, outputFileName, dontRun=False):
    # using saxon for XSLT 2 support
    cmdLine = '%s -xsl:%s -s:%s -o:%s' % (SAXON_PATH,
                                          xslFileName, xmlFileName, outputFileName)
    if dontRun:
        return cmdLine
    else:
        os.system(cmdLine)

    # The following code works fine with libxml2 bindings
    # ...but can't handle XSLT 2
    #styleDoc = libxml2.parseFile( xslFileName )
    #style = libxslt.parseStylesheetDoc( styleDoc )

    #resultsDoc = libxml2.parseFile( xmlFileName )
    #reportDoc = style.applyStylesheet( resultsDoc, None )

    # if isinstance( outputFile, str ):
    #    outputFile = open( outputFile, 'w' )
    #style.saveResultToFd( outputFile.fileno(), reportDoc )

    # reportDoc.freeDoc()
    # resultsDoc.freeDoc()
    # styleDoc.freeDoc()
    # if outputFile.fileno()!=sys.stdout.fileno() and \
    #    outputFile.fileno()!=sys.stderr.fileno():
    #    outputFile.close()


def getSoftwareVersion(h5file):
    """Much faster utility method for
    retrieving the software name and version (pulse2base) which generated
    this file
    """
    DEFAULT_SOURCE = 'PulseToBase'
    groupName = "/PulseData/BaseCalls"
    if h5file.endswith("ccs.h5"):
        DEFAULT_SOURCE = "PulseToCCS"
        groupName = "/PulseData/ConsensusBaseCalls"
    h5 = h5py.File(h5file, 'r')
    version = h5[groupName].attrs['ChangeListID']
    source = h5[groupName].attrs['SoftwareVersion'] if\
        'SoftwareVersion' in h5[groupName].attrs else DEFAULT_SOURCE
    # this attribute seems to be 0-length these days
    if len(source) == 0:
        source = DEFAULT_SOURCE
    else:
        source = source.replace(' ', '_')
    h5.close()
    return source, version


def makeRelative(path):
    """Convert the path to relative path.

    This will never be robust without knowing the job root directory.

    ..note There is a hack to support the nested data directories for
    celera-assembler

    data/9-terminator/celera-assembler.scf.fasta
    """
    if 'data/9-terminator' in path:
        relativePath = os.path.join(*path.split(os.sep)[-3:])
    else:
        root, fileName = os.path.split(path)
        relativePath = os.path.join(os.path.basename(root), fileName)
    return relativePath


def getMimeType(fileName):
    """
    This logic doesn't handle extensions that have a dot in
    them (like cmp.h5) special case cmp.h5
    """

    specialCases = ['cmp.h5', 'scf.fasta', 'bridgemapper.gz']
    for case in specialCases:
        if fileName.endswith(case):
            return MIME_TYPES[case]

    name, ext = os.path.splitext(os.path.basename(fileName))

    ext = ext.lstrip('.').lower()

    if ext in MIME_TYPES:
        return MIME_TYPES[ext]
    else:
        log.debug("Using default MIME type for '{e}' from file: '{f}'".format(f=fileName, e=ext))
        return MIME_TYPES['__default__']


# Temporarily Putting these here.
class VersionParseError(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return "{c} : {m}".format(c=self.__class__, m=self.msg)

    def __repr__(self):
        return str(self)


def parse_smrtanalysis_config(file_name):

    sa_version = "Unknown"
    sa_changelist = 0

    et = ElementTree(file=file_name)

    root = et.getroot()

    cs = root.findall('components')

    if len(cs) != 1:
        raise VersionParseError("Unable to find 'components' in {f}".format(f=file_name))

    sa_version = cs[0].attrib.get('version', None)
    sa_changelist = cs[0].attrib.get('build', None)

    # sometimes the build is 'None' (str)
    if sa_changelist == 'None':
        sa_changelist = 0

    try:
        sa_changelist = int(sa_changelist)
    except TypeError:
        sa_changelist = 0

    return sa_version, sa_changelist


class SmrtReportService(object):

    """
    Provides support for various common functions related to generating
    HTML output reports.
    """

    # stylesheets for transforming XML to HTML
    TOC_REPORT_XSL = 'toc.xsl'
    GRAPH_REPORT_XSL = 'graphReport.xsl'

    TOC_XML_NAME = 'toc.xml'
    METADATA_RDF_NAME = 'metadata.rdf'

    # relative to ASSEMBLY_HOME
    JNLP_TEMPLATE_DEFAULT = '../common/etc/vis.jnlp.in'
    JNLP_FILE_NAME = 'vis.jnlp'

    def __init__(self, context, message='', reports=(), data=(),
                 auxilaryFiles=(), extLink=(), attributes=None):
        """

        :param context: (SmrtPipeContext)
        :param message: (str) status message that will be written to html
        :param reports: (list of ReportItem)
        :param data: (list of DataItem)
        :param auxilaryFiles: (list of AuxilaryItem)
        :param extLink:
        :param attributes:

        Notes: This needs to be refactored.
        """
        # list of ReportItem instances
        self.reports = reports if reports else []
        # list of DataItem instances
        self.data = data if data else []
        # list of AuxiliaryItem instances
        self.auxiliaryFiles = auxilaryFiles if auxilaryFiles else []
        #
        self.extLink = []
        self.attributes = {}

        # modules can supply a top-level message to be reflected in the TOC
        # page
        self.message = ''

        # MK the getJobService from context is used from the SmrtPipeContext
        self._context = context

        # MK: Hacky Adding Special files
        # These should be assigned after the P*Modules are initialized in
        # SmrtPipeContext. These files should also be converted to SMRTFiles
        # These should be full paths as requested by Johann and Marco
        # refUrl = os.path.join(os.path.abspath(self._context.getFile('reference')), 'reference.info.xml')
        self.reference = None
        self.barcodeFasta = None

        # put in dummy data for now
        self.__dummyData()

    def getState(self):
        """Returns an object that contains the state of this service"""
        return {name: getattr(self, name) for name in SMRT_REPORT_STATE}

    def setState(self, state):
        """Loads the state information generated by a call to getState( )"""
        # self.__dict__.update(state)
        if state:
            for k, v in state.iteritems():
                log.debug("Setting state to {s}".format(s=state))
                setattr(self, k, v)
        else:
            log.warn("Unable to set state '{s}'".format(s=state))

    def setMessage(self, message):
        """Sets a top-level message to be reflected in the TOC page"""
        self.message = message

    def __dummyData(self):
        self.attributes['jobId'] = '000221002'
        self.attributes['jobDescription'] = "This would be a job description"

    def put(self, key, value):
        self.attributes[key] = value

    def get(self, key):
        return self.attributes[key]

    def addReport(self, report):
        log.debug("Adding report {r} to ReportService".format(r=report))
        self.reports.append(report)

    def addData(self, data):
        """Registers a DataItem for the current context.
        Only adds new data if the name of the data is unique."""

        self.data.append(data)
        self.__addDataToStore()

    def merge_from_serialized_datastore(self):
        """Attempts to merge the datastore from disk with in memory datastore

        :note: The entire DataStore model needs to be rewritten.
        """
        dataStoreFileName = os.path.join(self._context.getDataService().getDataPath(), DATA_ITEMS_STORE)
        data_item_store = None

        if os.path.exists(dataStoreFileName):
            with open(dataStoreFileName, 'r') as f:
                data_item_store = pickle.load(f)
        else:
            log.debug("Unable to find DataStore {f} for merging.".format(f=dataStoreFileName))

        # merge values from the data.items.pickle with the data in self.
        if data_item_store is not None:
            for item in data_item_store:
                log.debug("{m} merging item {x}".format(x=item, m=DATA_ITEMS_STORE))
                items = [(d.name, d.group) for d in self.data]
                if (item.name, item.group) in items:
                    # go over links and add new data
                    log.debug((item.name, item.group))
                else:
                    self.data.append(item)
        return dataStoreFileName

    def __addDataToStore(self):
        """
        This is executed whether or not smrtpipe is run in distributed
        mode or not.

        However, the functionality only applies to a distributed run too.

        In a non-distributed mode, the store gets written out,
        but nothing is done with it.

        Every time a new data item gets added to the data list, the datastore
        on disk gets overwritten!
        """
        dataStoreFileName = os.path.join(self._context.getDataService().getDataPath(), DATA_ITEMS_STORE)

        # Attempt to merge data for disk is the data.items.pickle exists
        self.merge_from_serialized_datastore()

        with open(dataStoreFileName, 'w') as f:
            log.debug("Writing DataStore to {d} with {n} items".format(d=dataStoreFileName, n=len(self.data)))
            pickle.dump(self.data, f)

        # Dirty generation of experimental json datastore
        jsonDataStore = []
        for d in self.data:
            if hasattr(d, 'name'):
                jsonDataStore.append(d.toDict())
            else:
                jsonDataStore.append(str(d))
                log.warn("Unable to correctly serialize {d} type {x} to json. Defaulting to str".format(d=d, x=type(d)))

        jsonDataStoreFileName = dataStoreFileName.replace('.pickle', '.json')
        with open(jsonDataStoreFileName, 'w') as f:
            f.write(json.dumps(jsonDataStore))
            log.debug("Writing DataStore to {f}".format(f=jsonDataStoreFileName))

    def addAuxiliaryFile(self, aux):
        """Registers an auxiliary file. It's not a report.

        It's not data.It's auxiliary!

        aux is an AuxiliaryItem."""

        if aux.id in [af.id for af in self.auxiliaryFiles]:
            log.debug("Auxiliary file {x} is already in datastore Auxiliary Files".format(x=aux))
        else:
            self.auxiliaryFiles.append(aux)

    def addExtLink(self, extLink):
        self.extLink.append(extLink)

    def _toRdf(self, createJnlp=True):
        """
        constructs RDF graph describing the job outputs

        :param (bool) Flag to add the vis.jnlp to the RDF

        :return: MetaDataGraph instance
        """
        graph = MetaDataGraph()

        jobId, jobName = self._context.getJobInfo()[:2]
        jobNode = MainJobNode(graph, jobId)
        jobNode.setJobName(jobName)
        jobNode.setCreated(self._context.creationTimeStamp)
        graph.addElement(jobNode)

        for report in self.reports:
            reportNode = ReportNode(graph, report.name, group=report.group)
            for link in report.links:
                fileNode = FileNode(jobId, report.getId(), link.local,
                                    link.format)
                reportNode.addLink(fileNode)

            log.debug("Adding report node {r} to metadata.rdf.".format(r=reportNode))
            jobNode.addReport(reportNode)

        # Bug 21650
        # Sometimes links are added multiple times due to
        # SmrtFiles having multiple states
        # Manually Keep track of the links added.
        links_added = []

        # data is a list of DataItem instances.
        for data in self.data:
            dataNode = DataNode(graph, data.name, group=data.group)

            # Link is FileItemLink instance
            for link in data.links:
                # use this tuple as the unique id for keeping track
                # of links added to the rdf.
                x = (jobId, link.id, link.local, link.format)

                #log.debug("Data name {n} with group {g} adding link {t} with {x}".format(n=data.name, x=x, g=data.group, t=type(link)))
                fileNode = FileNode(jobId, link.id, link.local, link.format)

                if x in links_added:
                    #log.debug("Link {x} was already added. Skipping...".format(x=x, f=fileNode))
                    pass
                else:
                    #log.debug("Adding link {x} to {f}".format(f=fileNode, x=x))
                    dataNode.addLink(fileNode)

                # local list of what has been added
                links_added.append(x)

            jobNode.addData(dataNode)

        log.debug("Adding Auxiliary Files to RDF.")
        for aux in self.auxiliaryFiles:
            fileNode = FileNode(jobId, aux.id, aux.local, aux.format)
            jobNode.addTriple('hasAuxiliaryFile', fileNode)

        _lf = FileItemLink(self._context.logFileName)
        logNode = FileNode(jobId, _lf.id, _lf.local, 'txt')
        jobNode.addTriple('hasAnalysisLog', logNode)

        stateNode = JobStateNode(self._context.getJobState(), self.message)
        jobNode.addTriple('hasJobState', stateNode)

        if 'reference' in self._context:
            # MK Bug 21201.
            # Update this to use if self.reference is not None:
            #refName = os.path.splitext(os.path.basename(self._context['reference']))[0]

            refUrl = os.path.join(os.path.abspath(self._context.getFile('reference')), 'reference.info.xml')

            log.debug("Setting Reference URL in metadata.rdf to {b}".format(b=refUrl))

            jobNode.addTriple('usesReference', SimpleRdfNode(refUrl), isGlobal=True)

        # Hack to Add otf-reference to metadata.rdf file. HGAP will write the
        # reference to {job-dir}/reference. If the reference.info.xml file
        # exists, we blindly add it.
        try:
            #hasOftRef = self._context.settings.get('otfReference', False)
            refUrl = os.path.join(os.path.abspath(self._context.outputDirName),
                                  'reference', 'reference.info.xml')

            if os.path.exists(refUrl):
                log.debug("Setting On-the-fly Reference URL in metadata.rdf to {b}".format(b=refUrl))
                jobNode.addTriple('usesReference', SimpleRdfNode(refUrl), isGlobal=True)

        except Exception as e:
            log.warn("Unable to add On-the-fly Reference. Raise exception {e}.".format(e=e))

        # MK Bug 22405
        # This is really hacky place to put this
        if self.barcodeFasta is None:
            log.debug("No barcodeFasta to add to ReportService")
        else:
            try:
                jobNode.addTriple('usesBarcode', SimpleRdfNode(self.barcodeFasta), isGlobal=True)
            except Exception as e:
                log.error("Unable to add Barcode {f} to metadata.rdf. Due to {e}".format(f=self.barcodeFasta, e=e))

        # Store JNLP as an auxiliary file
        # this is a special case - can it be pushed up to the module level?
        # Auxiliary files should
        # just call addAuxiliaryItem on the service
        if createJnlp:
            jobNode.addTriple('hasAuxiliaryFile', FileNode(jobId, 'vis', 'vis.jnlp', 'jnlp'))
        else:
            #log.debug("Not adding vis.jnlp to RDF!")
            pass

        attrs = self._pullReportAttributes()

        for attr in attrs:
            valueNode = MeasuredValueNode(jobId, attr._id, attr._name,
                                          str(attr._numberValue), attr._hidden)
            jobNode.addTriple('hasMeasuredValue', valueNode)

        inputs = self._retrieveInputs()

        if len(inputs) > 0:
            inputsNode = InputsNode(graph, jobId)
            for i in inputs:
                inputNode = InputNode(format=i[0], local=i[1],
                                      source=i[2], sourceVersion=i[3])
                inputsNode.addInput(inputNode)
            jobNode.addInputs(inputsNode)
        #
        try:
            # add setting.xml/workflow.xml path
            #settingsPath = os.path.splitext(self._context.settings.settingsPath)
            settingsPath = os.path.basename(os.path.abspath(self._context.settings.settingsPath))
            settingsNode = FileNode(jobId, 'workflowXml', settingsPath, 'xml')
            jobNode.addTriple('hasAnalysisSettings', settingsNode)
        #
        # add the input.xml file
            url_obj = urlparse.urlparse(self._context.dataUrl)
        # FIXME this needs to be an abspath? Maybe to this on initialization.
            inputXml = url_obj.path
            inputXmlNode = FileNode(jobId, 'inputXml', inputXml, 'xml')
            jobNode.addTriple('hasAnalysisInputs', inputXmlNode)
        #
        # Add SmrtAnalysis Version data
            seymourHome = os.environ['SEYMOUR_HOME']
            smrtAnalysisConfigFileName = os.path.join(seymourHome, 'etc', 'config.xml')
        #
            version, changelist = parse_smrtanalysis_config(smrtAnalysisConfigFileName)
        #
            smrtAnalysisVersion = SmrtAnalysisVersionNode(version, str(changelist))
            jobNode.addTriple('hasAnalysisVersion', smrtAnalysisVersion)
        #
        # Add Resolve SmrtPipeRc Settings
            smrtpipeConfigs = SmrtPipeConfigsNode(graph, jobId)
            configNodes = []
            for key, value in self._context.config.iteritems():
                # skip the keys that have _VERSION
                if not key.endswith('_VERSION'):
                    c = SmrtPipeConfigNode(jobId, key, str(value))
                    configNodes.append(c)
        #
            if configNodes:
                for c in configNodes:
                    smrtpipeConfigs.addNode(c)
                    jobNode.addSmrtpipeConfigs(smrtpipeConfigs)
        except Exception as e:
            log.warn("Unable to add secondary config or settings to metadata.rdf due to error {e}".format(e=e))
        return graph

    def _pullReportAttributes(self):
        """
        Retrieve all of the report attributes from generated reports.
        Relies on (standard) convention that reports are contained
        under the same directory


        .. note :: This is used to build the metadata.rdf
        """

        # reportsDir = self._context.getDataService().getReportPath()
        # reportFiles = glob.glob( '%s/*.xml' % reportsDir )

        reportFiles = [report.xml for report in self.reports if report.xml is not None]
        reportFiles += [aux.xml for aux in self.auxiliaryFiles if aux.xml is not None]

        allAttributes = []
        for reportFile in reportFiles:
            allAttributes.extend(self._pullAttributes(reportFile))

        return allAttributes

    def _pullAttributes(self, reportFile):
        """ """
        attrs = []
        if reportFile.endswith('.json'):
            attrs = self._pullJsonAttributes(reportFile)
            return attrs
        try:
            tree = etree.ElementTree(file=reportFile)
            if tree.getroot().tag != 'report':
                log.warn('Skipping non-report XML file %s' % reportFile)
                return []
            attributes = tree.find('attributes')
            for attribute in attributes.findall('attribute'):
                attr = GraphAttribute.fromElement(attribute)
                attrs.append(attr)
            return attrs
        except Exception, e:
            # FIXME This should raise an exception
            log.error('Encountered exception while extracting attributes from %s:\n %s' % (reportFile, str(e)))
            return []

    def _pullJsonAttributes(self, json_file):
        """Grab attributes from pbreports Json Report file"""
        graph_attributes = []
        if not os.path.exists(json_file):
            log.warn("Unable to find {f}. Unable to add attributes".format(f=json_file))
            return graph_attributes

        try:
            with open(json_file, 'r') as f:
                # report dict
                report_dct = json.loads(f.read())

            if 'attributes' in report_dct:
                for attr in report_dct['attributes']:
                    name = attr['name']
                    id_ = attr['id']
                    value = attr['value']
                    g_attr = GraphAttribute(name, value, attributeId=id_)
                    graph_attributes.append(g_attr)
            else:
                log.debug("No attributes found in {j}.".format(j=json_file))

        except Exception as e:
            log.error(e)

        log.debug("Json Graph attributes: {a}".format(a=graph_attributes))
        return graph_attributes

    def _retrieveInputs(self):
        if SMRT_DATA_PLS_FOFN not in self._context.getDataService():
            return []

        plsFofn = self._context.getDataService()[SMRT_DATA_PLS_FOFN].toLocal(multiplicity=1)

        infile = open(plsFofn, 'r')
        inputs = []

        for line in infile:
            h5fn = line.strip()
            format_ = 'application/x-hdf'
            local = h5fn
            source, version = getSoftwareVersion(h5fn)
            inputs.append((format_, local, source, version))

        infile.close()
        return inputs

    def _toXml(self):
        """
        Generate the toc.xml which can be used to generate the index.html

        This should be deleted.
        """

        #xmlFile = self._context.createTemporaryFile()
        # JMS - 04.24.09 - it turns out that lots of people could
        # take advantage of this XML file (before it's converted to HTML)
        # So don't make it temporary.

        # This Whole mess should be removed.
        xmlFile = self._context.getDataService().getOutputRootPath(SmrtReportService.TOC_XML_NAME)

        outs = []
        outs.append('<?xml version="1.0"?>')
        outs.append('<toc>')

        if self.message:
            outs.append("<message>%s</message>" % self.message)

        outs.append('<attributes>')

        # Begin with the Version of SmrtPipe
        if 'smrtpipe_version' in self.attributes:
            outs.append("<attribute name=\"{n}\">{v}</attribute>".format(n='SmrtPipe version', v=self.attributes['smrtpipe_version']))

        for attr in ATTRS_ORDER:
            if attr in self.attributes and self.attributes[attr] is not None:
                outs.append('<attribute name="%s">%s</attribute>' %
                            (ATTRS_DISPLAY_NAMES[attr], sx.escape(self.attributes[attr])))

        outs.append('<attribute name="advanced">')
        outs.append('<apParameters>')

        # pretty print the settings.
        setting_group_names = [name.split('.')[0] for name in self._context.settings.keys() if '.' in name]

        for group_name in set(setting_group_names):
            for key, value in self._context.settings.iteritems():
                if key.startswith(group_name):
                    outs.append("<apParam name=\"{k}\" >{v}</apParam>".format(k=key, v=value))

        # Old code
        # for adv_item in sorted(self._context.settings.iteritems(), key=lambda(k, v): (v, k)):
        #    outs.append('<apParam name="%s" >%s</apParam>' % (adv_item[0], adv_item[1]))

        outs.append('</apParameters>')
        outs.append('</attribute>')

        # End attributes
        outs.append('</attributes>')
        outs.append('<reports>')

        for report in self.reports:
            outs.append(report.toXml())

        outs.append('</reports>')
        outs.append('<dataFiles>')

        for data in self.data:
            outs.append(data.toXml())

        outs.append('</dataFiles>')

        outs.append('<extlinks>')
        for extLink in self.extLink:
            outs.append(extLink.toXml())

        outs.append('</extlinks>')

        outs.append('<log file="%s" />' % makeRelative(self._context.logFileName))
        outs.append('</toc>')

        return "\n".join(outs)

    def _generateIndexHtml(self, htmlFileName, notReady=False):
        """Used by generateFinalReport"""

        if notReady:
            # hacky way to set the status
            self.attributes['not_ready'] = 'CLICK REFRESH'
            self.message = "Job results are not all ready yet."\
                           "  The status of the job can"\
                           " be checked in the log file."\
                           "  Click refresh to retrieve the latest results."
        else:
            if 'not_ready' in self.attributes:
                del self.attributes['not_ready']

            if self.message.startswith("Job results are not all ready"):
                self.message = ''

        tocFileName = self._context.getDataService().getOutputRootPath(SmrtReportService.TOC_XML_NAME)
        tocXml = self._toXml()

        with open(tocFileName, 'w') as f:
            log.debug("Writing Report XML to {f}".format(f=tocFileName))
            f.write(tocXml)

        stylesheet = self._context.config.findStylesheet(SmrtReportService.TOC_REPORT_XSL)

        reportOut = self._context.getDataService().getOutputRootPath(htmlFileName)

        cmd = xslTransform(tocFileName, stylesheet, reportOut, dontRun=True)

        log.debug("Generating html file from xsl {s}".format(s=stylesheet))
        self._context.exe(cmd)

    def generateFinalReport(self, jobInfo, dataUrl, paramsFile, notReady=False):
        """
        1. Creates jnlp if necessary
        2. generates index.html
        3. writes metadata.rdf

        :param jobInfo: (JobInfo instance)
        :param dataUrl: (str) Data url
        :param paramsFile: (str) path to workflow.xml file
        :param notReady: (bool) Updates the message in
        """
        log.debug("jobInfo {i} dataUrl {d} paramsFile {f}".format(i=jobInfo, d=dataUrl, f=paramsFile))

        if jobInfo:
            self.put('jobId', jobInfo.id)
            self.put('jobName', jobInfo.name)
            self.put('jobComment', jobInfo.comment)
            self.put('primaryAnalysisProtocol', jobInfo.primaryAnalysisProtocol)

            inputs = [str(u) for u in
                      self._context.getDataService()[SMRT_DATA_CMDLINE].getData()]

            self.put('input', ",".join(inputs))
        else:
            self.put('input', dataUrl)

        self.put('settings', paramsFile)

        #
        doesExist = lambda x: os.path.exists(self._context.getDataService().getDataPath(x))
        possibleCmph5Files = ["aligned_reads.cmp.h5", "assembled_reads.cmp.h5"]

        hasCmpH5 = any([doesExist(fName)for fName in possibleCmph5Files])

        if hasCmpH5:
            log.debug("Found cmpH5 file, generating JNLP file from job_id {i}".format(i=jobInfo))

        createJnlp = False
        if jobInfo and hasCmpH5:
            try:
                self._generateJnlp(jobInfo.id)
                createJnlp = True
            except Exception, e:
                log.info("Unable to make jnlp template substitutions: %s" % jobInfo.id)
                log.info("%s" % str(e))
        else:
            log.info("Skipping JNLP creation because no job id or no cmp.h5")

        self._generateIndexHtml(self._context.config['TOC_HTML_NAME'], notReady)

        # Need to explicitly pass this so jnlp doesn't get added to the
        # metadata.rdf
        rdfGraph = self._toRdf(createJnlp=createJnlp)

        rdfFileName = self._context.getDataService().getOutputRootPath(SmrtReportService.METADATA_RDF_NAME)

        with open(rdfFileName, 'w') as f:
            log.debug("Writing Metadata XML to {f}".format(f=rdfFileName))
            f.write(rdfGraph.toXml())

    def _getJnlpTemplateFile(self):
        """
        Get a reference-specific jnlp template file, if it exists.
        Else, get the path to the template under common etc.
        """
        if 'reference' in self._context:
            refSpecificJnlp = os.path.join(self._context.getFile('reference'), 'templates/smrtview/vis.jnlp.in')
            if os.path.exists(refSpecificJnlp):
                return refSpecificJnlp

        return os.path.join(self._context.config['ASSEMBLY_HOME'], SmrtReportService.JNLP_TEMPLATE_DEFAULT)

    def _generateJnlp(self, job_id):
        """
        Generate a JNLP file for downstream visualization through
        SMRTview.
        """

        templateFile = self._getJnlpTemplateFile()

        if not os.path.exists(templateFile):
            log.info("Skipping jnlp generation because template %s can't be found" % templateFile)
            return
        else:
            log.info("Generating jnlp {i} using template file {s}.".format(i=job_id, s=templateFile))

        template = JnlpTemplate(templateFile)

        jnlpOut = self._context.getDataService().getOutputRootPath(SmrtReportService.JNLP_FILE_NAME)

        jobRef = "%s/%s" % (str(job_id)[:3], str(job_id))

        if self._context.config['VIS_NAMESPACE']:
            jobRef = '%s/%s' % (
                self._context.config['VIS_NAMESPACE'], jobRef)

        params = {
            'httpport': self._context.config['VIS_PORT'],
            'hostname': self._context.config['VIS_HOST'],
            'jobid': jobRef}

        template.construct(params)
        template.save(jnlpOut)

        # let index.html, metadata.rdf, etc... know about the jnlp
        # Removed the hasData link for the jnlp, using hasAuxiliaryFile
        # instead. JLB.
        # self.addData( DataItem( "Visualize in SMRT View", links=[\
        #    FileItemLink( \
        # this is a super hack so that FileItemLink works with a file
        # that's in the top directory (not a subdirectory)
        # os.path.join(os.path.dirname(jnlpOut),'.',os.path.basename(jnlpOut)),
        #        'vis', 'jnlp' ) ] ) )

        log.info("Created JNLP file %s" % jnlpOut)


class ReportItem(object):

    def __init__(self, name, link, group='default', xml=None):
        self._name = name
        self._xml = xml
        self.links = []
        self.links.append(FileItemLink(link))
        self.group = group

        if xml:
            self.links.append(FileItemLink(xml))
        self.id = ''

    def __repr__(self):
        return "<ReportItem name: {n} xml: {x} links.".format(n=self.name, x=self.xml)

    def getId(self):
        if self.id:
            return self.id
        return os.path.splitext(self.links[0].local)[0]

    @property
    def name(self):
        """Grabs the initially specified name, unless it is None
        and we can find a replacement in the XML."""
        if self._name is not None or self._xml is None:
            return self._name
        return self._titleFromXml(self._xml)

    def _titleFromXml(self, xml):
        """Extracts the report title from the XML for the report."""
        log.debug("Parsing title from file {x}".format(x=xml))
        reportElt = etree.parse(xml)
        titleElt = reportElt.find('title')
        if titleElt is None:
            return None
        return titleElt.text

    @property
    def xml(self):
        return self._xml

    def toXml(self):
        """for serialization into older toc.xml format"""
        outs = []

        if self.group:
            outs.append('<report group="%s">' % self.group)
        else:
            outs.append('<report>')

        outs.append('<name>%s</name>' % self.name)

        for link in self.links:
            mimeType = getMimeType(link.local)
            outs.append('<link rel="report" type="%s">%s</link>'
                        % (mimeType, link.local))

        outs.append('</report>')

        return os.linesep.join(outs)

    def toDict(self):
        """Convert to python dict of primitives"""
        outs = dict(group=self.group, name=self.name)

        links = []
        for link in self.links:
            mimeType = getMimeType(link.local)
            l = dict(rel='report', type=mimeType, localPath=link.local)
            links.append(l)
        outs['links'] = links
        return outs

    def toJson(self):
        return json.dumps(self.toDict())


class FileItemLink(object):

    def __init__(self, filePath, id=None, format=None):
        """
        By default, the id is generated from the base name without the ext.
        The format will be assigned by grabbing the ext. Files with multiple
        extensions (e.g., file.cmp.h5) need to handled specially.

        :param filePath: (str) File path
        :param id: (str)
        :param format: (str) cmp.h5, csv, etc...
        :return:
        """
        self.local = makeRelative(filePath)
        self.id = id if id else self._guessId()
        self.format = format if format else self._guessFormat()

    def _guessId(self):
        # return os.path.splitext(self.local)[0]
        # Made specific to our extensions (ex .cmp.h5)
        return self.local.split(".")[0]

    def _guessFormat(self):
        # return
        # os.path.splitext(os.path.basename(self.local))[1].lstrip('.').lower()
        # Made specific to our extensions (ex .cmp.h5)
        return ".".join(self.local.split(".")[1:]).lower()

    def __eq__(self, other):
        if isinstance(other, FileItemLink):
            if (other.id, other.format, other.local) == (self.id, self.format, self.local):
                return True
            else:
                return False
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)


class DataItem(object):

    def __init__(self, name, formats=None, links=None, group='default'):
        """
        Old usage:
            DataItem("Aligned Reads", [('cmp.h5','data/alignment.cmp.h5')])

        New usage:
            DataItem( "Aligned Reads", links=[
              FileItemLink( 'data/alignment.cmp.h5',
                            'aligned_reads', 'cmp.h5' ) ] )

        Newer attribute links will override formats.
        """
        self.name = name
        # list of FileItemLink
        self.links = [] if links is None else links
        # list of tuples (formatType, localPath)
        self.formats = [] if formats is None else formats

        # Group name which will be shown in Report
        self.group = group

        # deprecated
        self.files = []

        if len(links) > 0:
            self.links = links
            for link in self.links:
                self.addFormat((link.format, link.local))
        else:
            # deprecated
            warnings.warn("'formats' keyword arg in DataItem is deprecated", DeprecationWarning)
            for format in formats:
                self.addFormat(format)
                self.files.append(FileItemLink(format[1], format=format[0].lower()))
        self.id = ''

    def getId(self):
        if self.id:
            return self.id
        return os.path.splitext(os.path.splitext(self.links[0].local)[0])[0]

    def addFormat(self, format):
        """Format is a tuple ('format_type','file_path')"""

        formatType, path = format

        # normalize path
        #self.formats.append((format[0], makeRelative(format[1])))
        self.formats.append((formatType, makeRelative(path)))

    def toXml(self):
        """for serialization into older toc.xml format"""
        outs = []
        outs.append('<data group="%s">' % self.group)
        outs.append('<name>%s</name>' % self.name)

        # SmrtFiles can have multiple states which show up
        # as separate links, but point to the same file.
        # This will fix the duplicate link issue in Martin.
        for formatType, path in set(self.formats):
            mimeType = getMimeType(path)
            outs.append('<link rel="data" type="%s" format="%s">%s</link>'
                        % (mimeType, formatType, path))
        outs.append('</data>')
        return os.linesep.join(outs)

    def toDict(self):
        """Return a dictionary version representation"""
        formats = []
        for formatType, filePath in self.formats:
            mimeType = getMimeType(filePath)
            f = {'mimeType': mimeType, 'formatType': formatType, 'path': filePath}
            formats.append(f)
        outs = {'data_group': self.group, 'name': self.name, 'formats': formats}
        return outs

    def toJson(self):
        return json.dumps(self.toDict())


class AuxiliaryItem(object):

    """This is wrapper for FileNode. It can be instantiated without
    a job id"""

    def __init__(self, id, local, format, title=None):
        self.id = id
        self.path = local
        self.local = makeRelative(local)
        self.format = format
        self.title = title

    @property
    def xml(self):
        return self.path if self.format == 'xml' else None


class ExtLinkItem(object):

    def __init__(self, name, URL):
        self.name = name
        self.URL = URL

    def toXml(self):
        outs = []
        outs.append('<extLink>')
        outs.append('<name>%s</name>' % self.name)
        outs.append('<url>%s</url>' % self.URL)
        outs.append('</extLink>')
        return os.linesep.join(outs)

    def toDict(self):
        return dict(name=self.name, url=self.URL)

    def toJson(self):
        return json.dumps(self.toDict())


class JnlpTemplate(string.Template):
    delimiter = '@'

    def __init__(self, fileName):
        self.fileName = fileName
        self._load(self.fileName)

    def _load(self, fn):
        infile = open(fn, 'r')
        script = ''.join(infile.readlines())
        infile.close()
        string.Template.__init__(self, script)

    def construct(self, parameters):
        self.output = self.substitute(parameters)

    def save(self, fileName):
        with open(fileName, 'w') as f:
            f.write(self.output)
#
