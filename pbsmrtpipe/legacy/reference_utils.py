import os
import logging
import time
import functools

import xml.etree.cElementTree as ET

from pbcommand.models.report import Report, Attribute

log = logging.getLogger(__name__)


class ConstantsReferenceIndexTypes(object):
    INDEXER = 'indexer'
    SA_WRITER = 'sawriter'
    GATK_DICT = 'gatk_dict'
    SAM_IDX = 'sam_idx'

    @classmethod
    def ALL(cls):
        return cls.INDEXER, cls.GATK_DICT, cls.SA_WRITER, cls.SAM_IDX


def load_reference_entry(refdir_or_info_xml_or_fasta):
    """
    This will try to load the reference from a reference dir,
    reference.info.xml, or a fasta file in /sequence/X.fasta reference directory

    This is a bit too magical.
    """
    # assume it's given as a directory

    log.debug("Attempting to load reference from '{x}'".format(x=refdir_or_info_xml_or_fasta))

    d = refdir_or_info_xml_or_fasta
    if os.path.isfile(refdir_or_info_xml_or_fasta):
        if refdir_or_info_xml_or_fasta.endswith('reference.info.xml'):
            d = os.path.dirname(refdir_or_info_xml_or_fasta)
        elif refdir_or_info_xml_or_fasta.endswith('.fasta'):
            sdir = os.path.dirname(refdir_or_info_xml_or_fasta)
            name = os.path.basename(sdir)
            if name == 'sequence':
                d = os.path.dirname(sdir)

    if os.path.exists(d):
        r = ReferenceEntry.from_ref_dir(d)
        log.info("Loaded reference {r}".format(r=r))
        return r
    else:
        raise IOError("Unable to load reference from '{x}'".format(x=d))


def _to_attr(attr_id, value, name):
    return Attribute(attr_id, value, name=name)


def get_reference_entry_index_type(reference_entry, index_type_id):
    """
    :type reference_entry: ReferenceEntry
    :type index_type_id: str
    """
    # {index_type_id:value}
    index_type_id_d = {d['type']: d['value'] for d in reference_entry.reference_info.reference.index_files}

    return index_type_id_d.get(index_type_id, None)


def get_mean_contig_length(reference_entry):
    """
    :type reference_entry: ReferenceEntry
    """
    lengths = [c.length for c in reference_entry.reference_info.contigs]
    ncontigs = float(len(lengths))
    return int(sum(lengths) / ncontigs)


def reference_to_report(reference_entry):

    attrs = [("ncontigs", len(reference_entry.reference_info.contigs), "Number of Contigs"),
             ('version', reference_entry.reference_info.version, "Version"),
             ('max_contig_length', reference_entry.reference_info.reference.max_contig_length, "Max Contig Length")]

    _to_i = lambda x: "ref_index_type_" + x

    for reference_index_type_id in ConstantsReferenceIndexTypes.ALL():
        idx_value = get_reference_entry_index_type(reference_entry, reference_index_type_id)
        if idx_value is not None:
            idx_value = os.path.join(reference_entry._ref_dir, idx_value)
        else:
            # TODO FIXME Pbreports doesn't support None is an attribute value
            # I don't know why this is so
            idx_value = 'None'
        attrs.append((_to_i(reference_index_type_id), idx_value, "Index Type " + reference_index_type_id))

    attrs.append(('mean_contig_length', get_mean_contig_length(reference_entry), "Mean Contig Length"))

    attributes = [_to_attr(*a) for a in attrs]
    return Report("reference_entry_report", attributes=attributes)


def run_ref_to_report(refdir_or_info_xml, output_json):

    refenence_entry = load_reference_entry(refdir_or_info_xml)

    report = reference_to_report(refenence_entry)
    report.write_json(output_json)
    log.info("wrote json report to {f}".format(f=output_json))
    return 0


def _reference_entry_to_contig_attr(attr_name, reference_entry):
    contigs = reference_entry.reference_info.contigs
    return [getattr(contig, attr_name) for contig in contigs]


def _reference_entry_to_contig_attr_with_tranform(attr_name, transform_func, reference_entry):
    xs = _reference_entry_to_contig_attr(attr_name, reference_entry)
    return [transform_func(x) for x in xs]


reference_entry_to_contig_ids = functools.partial(_reference_entry_to_contig_attr, 'id')
reference_entry_to_contig_names = functools.partial(_reference_entry_to_contig_attr, 'name')
# THIS IS A GIANT FUCKING MESS. The reference.info.xml is not consistent with pbcore's definition of an 'id'
reference_entry_to_contig_headers = functools.partial(_reference_entry_to_contig_attr, 'header')
reference_entry_to_contig_idx = functools.partial(_reference_entry_to_contig_attr_with_tranform, 'header', lambda x: x.split(" ")[0])

# This is taken from pbsystem.io.Reference


def _check_exists(path):
    """
    Raises an IOError if path does not exist
    """
    if not os.path.exists(path):
        raise IOError('{p} does not exist.'.format(p=path))


class ReferenceEntry(object):

    """ReferenceEntry models an individual reference entry,
    which contains a metadata xml document (reference.info.xml) and a hierarchy of data files."""

    def __init__(self, ref_dir):
        """
        Callers should not use this directly. Use static from_ref_dir
        :param ref_dir: (str) Path to a reference directory
        """
        self._ref_dir = ref_dir

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, d=self._ref_dir)
        return "<{k} dir:{d} >".format(**_d)

    @staticmethod
    def from_ref_dir(ref_dir):
        """
        Parses the reference.info.xml document.
        Example usage to get metadata:
        re = from_ref_dir('/my/ref')
        ri = re.reference_info

        :param ref_dir: (str) Path to a reference directory
        """
        ref = ReferenceEntry(ref_dir)
        ref._load()
        return ref

    @staticmethod
    def get_file(ref_dir):
        """
        Convenience method that returns the abs path to the fasta file.
        """
        ref_info_xml = os.path.join(ref_dir, 'reference.info.xml')
        dom = ET.parse(ref_info_xml)
        f = dom.getroot().find('reference/file').text
        p = os.path.abspath(os.path.join(ref_dir, f))
        _check_exists(p)
        return p

    def _load(self):
        """
        Deserialize xml
        """
        ref_info_xml = os.path.join(self._ref_dir, 'reference.info.xml')
        dom = ET.parse(ref_info_xml)
        self._ref_info = ReferenceInfo(dom.getroot(), self._ref_dir)

    @property
    def reference_info(self):
        """
        Returns a ReferenceInfo object, which models the reference_info xml element.
        """
        return self._ref_info


class ReferenceInfo(object):

    """Models the reference_info xml element."""

    def __init__(self, reference_info, ref_dir):
        """
        :param reference_info (xml.etree.ElementTree.Element) xml chunk
        :param ref_dir (string) path to reference dir
        """
        self._reference = Reference(reference_info.find('reference'), ref_dir)
        self._version = reference_info.attrib['version']
        self._id = reference_info.attrib['id']
        self._last_modified = time.strptime(reference_info.attrib['last_modified'][:-5], '%Y-%m-%dT%H:%M:%S')

        e = reference_info.find('organism/name')
        if e is None:
            self._organism = None
        else:
            self._organism = {'name': e.text}
            e = reference_info.find('organism/ploidy')
            if e is not None:
                self._organism['ploidy'] = e.text

        self._cache_id = {}
        self._cache_digest = {}
        self._cache_header = {}
        self._contigs = []

        idx = 0
        for c in reference_info.findall('contigs/contig'):
            ctg = Contig(c)

            self._cache_id[ctg.id] = idx
            self._cache_digest[ctg.digest['value']] = idx
            self._cache_header[ctg.header] = idx

            self._contigs.append(ctg)

            idx += 1

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.id,
                  n=len(self.contigs))
        return "<{k} id:{i} ncontigs:{n} >".format(**_d)

    @property
    def version(self):
        """
        Returns a version string, as in "2.2.0"
        """
        return self._version

    @property
    def reference(self):
        """
        Returns a Reference object
        """
        return self._reference

    @property
    def organism(self):
        """
        Returns null if no organism element exists, or it is empty.
        Else return a dict:
        'name' : the name of the organism, guaranteed to exist
        'ploidy': may not exist in the dict. Else, 'haploid' or 'diploid'
        """
        return self._organism

    @property
    def contigs(self):
        """
        Returns a list of Contig objects
        """
        return self._contigs

    @property
    def id(self):
        """
        Returns (string) the reference id
        """
        return self._id

    @property
    def last_modified(self):
        """
        TODO - this needs fixing. The UTC offset is not handled correctly
        Returns (struct_time) the last modified attribute
        """
        return self._last_modified

    def get_contig(self, key):
        """
        Fast lookup.
        Return a contig by key.
        :param key (string) key can be one of: id, header, or digest.
        """
        i = self._get_contig_idx(key, 'id')
        if i is not None:
            return self._contigs[i]
        i = self._get_contig_idx(key, 'header')
        if i is not None:
            return self._contigs[i]
        i = self._get_contig_idx(key, 'digest')
        if i is not None:
            return self._contigs[i]
        return None

    def _get_contig_idx(self, key, cache):
        """
        Look in the specficied cache for the key. Return None if not found.
        :param key (string) - key
        :param cache (string) - one of "id", "header", "digest"
        """
        if cache is 'id':
            try:
                return self._cache_id[key]
            except KeyError:
                return None
        if cache is 'header':
            try:
                return self._cache_header[key]
            except KeyError:
                return None
        if cache is 'digest':
            try:
                return self._cache_digest[key]
            except KeyError:
                return None
        raise ValueError('Unknown cache {c}'.format(c=cache))


class Reference(object):

    """Models the reference xml element."""

    def __init__(self, reference, ref_dir):
        """
        :param reference (xml.etree.ElementTree.Element) xml chunk
        :param ref_dir (string) path to reference dir
        """
        self._ref_dir = ref_dir
        self._description = reference.find('description').text
        e = reference.find('file')
        self._file = {'value': e.text, 'format': e.attrib['format']}
        self._index_files = []
        for e in reference.findall('index_file'):
            self._index_files.append({'value': e.text, 'type': e.attrib['type']})
        self._max_contig_length = int(reference.find('max_contig_length').text)
        self._num_contigs = int(reference.find('num_contigs').text)
        self._type = reference.find('type').text

    def __repr__(self):
        _d = dict(k=self.__class__.__name__)
        return "<{k} >".format(**_d)

    @property
    def description(self):
        """
        Returns the description string
        """
        return self._description

    @property
    def file(self):
        """
        Returns a dict the describes the file element.
        Raises IOError if the file does not exist.
        'value' : path to the file, relative to the reference dir
        'format': the file format, typically "text/fasta"
        """
        _check_exists(os.path.join(self._ref_dir, self._file['value']))
        return self._file

    @property
    def index_files(self):
        """
        Returns a list of dicts. Each dict describes an index_file element.
        Raises an IOError if any one of the index files does not exist.
        'value' : path to the file, relative to the reference dir
        'type': the type_element
        """
        for f in self._index_files:
            _check_exists(os.path.join(self._ref_dir, f['value']))

        return self._index_files

    @property
    def max_contig_length(self):
        """
        Returns (int) the max_contig_length
        """
        return self._max_contig_length

    @property
    def num_contigs(self):
        """
        Returns (int) the num_contigs
        """
        return self._num_contigs

    @property
    def type(self):
        """
        Returns (string) the type, as in "sample" or "control"
        """
        return self._type


class Contig(object):

    """Models the contig xml element."""

    def __init__(self, contig):
        """
        :param contig (xml.etree.ElementTree.Element) xml chunk
        """
        self._length = int(contig.attrib['length'])
        self._id = contig.attrib['id']
        self._display_name = contig.attrib['displayName']
        e = contig.find('digest')
        self._digest = {'type': e.attrib['type'], 'value': e.text}
        self._header = contig.find('header').text

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.id,
                  n=self.display_name)
        return "<{k} id:{i} {n} >".format(**_d)

    @property
    def length(self):
        """
        Returns (int)  the length of the contig
        """
        return self._length

    @property
    def id(self):
        """
        Returns (string) the PacBio internal id
        """
        return self._id

    @property
    def display_name(self):
        """
        Returns (string) the contig display name. This is not guaranteed to be unique.
        """
        return self._display_name

    @property
    def digest(self):
        """
        Returns (dict) a dict with two keys:
        'type': (string) hash type, md5
        'value': (string) hash value
        """
        return self._digest

    @property
    def header(self):
        """
        Returns (string) the contig header
        """
        return self._header
