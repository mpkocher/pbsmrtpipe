import os
import re
import abc
import logging
import h5py
import json
import collections
import gzip
from tarfile import TarFile
from xml.etree.ElementTree import ElementTree

from pbcore.io.FastaIO import FastaReader
from pbcore.io.FastqIO import FastqReader
from pbcore.io.GffIO import GffReader

from pbsmrtpipe.utils import fofn_to_files

log = logging.getLogger(__name__)

_REQUIRED_JSON_REPORT_KEYS = 'attributes id plotGroups tables'.split()

ChemistryMovie = collections.namedtuple('ChemistryMovie', 'name chemistry')


def validator_factory(validator_type, **kwargs):
    """Create a function that returns a validator based on a path."""
    def f(path, **kwargs_f):
        all_kwargs = dict(kwargs.items() + kwargs_f.items())
        return validator_type(path, **all_kwargs)
    return f


class ValidatorBase(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, path):
        if not isinstance(path, str):
            _d = dict(k=self.__class__.__name__, t=type(path), p=path)
            raise TypeError(
                "{k} require path '{p}' to be provided as a str. Got type {t}".format(**_d))
        self._path = path

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, p=self.path)
        return "<{k} path:{p} >".format(**_d)

    @property
    def path(self):
        return self._path

    @property
    def name(self):
        return self.__class__.__name__

    def validate(self):
        return self.validate_file(self.path)

    @abc.abstractmethod
    def validate_file(self, path):
        pass


def _validate_bti(path):
    """Validate a bamtools index (BTI) file. Not much we can do for now,
    but at least check that the magic string is present.
    """
    BTI_MAGIC = 'BTI\x01'
    bti_file = open(path, 'rb')
    if bti_file.read(4) != BTI_MAGIC:
        raise ValueError("BTI file {b} is invalid.".format(b=path))
    return True


def _validate_bai(path):
    """Validate a samtools index (BAI) file."""
    BAI_MAGIC = 'BAI\x01'
    bai_file = open(path, 'rb')
    if bai_file.read(4) != BAI_MAGIC:
        raise ValueError("BAI file {b} is invalid.".format(b=path))
    return True


def _validate_csv(path, required_fields):
    """Test to see if the required fields are in the header of the CSV file"""
    with open(path, 'r') as f:
        header = f.readline()

    msg = "Invalid CSV file."
    if ',' in header:
        header_fields = header.rstrip().split(',')
        for required_field in required_fields:
            if required_field not in header_fields:
                msg += "Unable to find header field '{f}' in {p}".format(
                    f=required_field, p=path)
                log.error(msg)
                raise ValueError(msg)

    # Maybe should make sure there's more than one record in the CSV file?
    return True


def _validate_json(path):
    """Simple smoke test to validate the json is well formed"""
    with open(path, 'r') as f:
        s = json.loads(f.read())
    return True


def _validate_json_report(path):
    """Smoke Test to make sure pbreports JSON file has the required
    root level keys.
    """
    with open(path, 'r') as f:
        s = json.loads(f.read())

    for key in _REQUIRED_JSON_REPORT_KEYS:
        if key not in s:
            _d = dict(s=key, p=path)
            msg = "Unable to find {s} in {p}".format(**_d)
            log.error(msg)
            raise KeyError(msg)

    return True


def get_chemisty_movies(path):
    """Get list of ChemistryMovie instances

    :type path: str

    :rtype: list

    This is more a general IO layer, doesn't really belong here.
    """

    movies = []

    t = ElementTree(file=path)

    msg = "Invalid chemistry mapping XML {p}".format(p=path)
    mapping_nodes = t.findall('Mapping')

    if len(mapping_nodes) == 0:
        msg += " Unable to find 'Mapping' in XML"
        raise ValueError(msg)
    else:
        required_keys = ["Movie", "SequencingChemistry"]
        for node in mapping_nodes:
            datum = []
            for key in required_keys:
                if node.find(key) is None:
                    msg += " Unable to find '{k}' in XML".format(k=key)
                    raise KeyError(msg)
                else:
                    datum.append(node.find(key).text)
            movie = ChemistryMovie(*datum)
            log.debug(movie)
            movies.append(movie)

    return movies


def _validate_chemistry_mapping(path, chemistries):
    movies = get_chemisty_movies(path)
    movie_chemistries = [m.chemistry for m in movies]

    if len(chemistries) != len(movie_chemistries):
        raise ValueError(
            "Expected {n} got {m}".format(n=movie_chemistries, m=chemistries))

    for chemistry in chemistries:
        if chemistry not in movie_chemistries:
            _d = dict(c=chemistry, m=movies)
            raise ValueError(
                "Unable to find chemsitry {c} in movies {m}".format(**_d))

    return True


def _validate_chemistry_mapping_xml(path):
    movies = get_chemisty_movies(path)
    # If the file was parsed correctly, the file is valid
    return True


def _validate_hdf5(path, required_groups, required_dset_basenames):
    """Ensure that h5py can open the file and the required groups are
    present. Then check that there are datasets ending with basenames.
    """
    f = h5py.File(path, 'r')
    h5_elements = []
    f.visit(h5_elements.append)

    for basename in required_dset_basenames:
        matching_elements = [k for k in h5_elements if k.endswith(basename)]
        if not matching_elements:
            raise KeyError("HDF5 file {f} is missing dataset ending with {d}."
                           .format(f=path, d=basename))
        for el in matching_elements:
            d = f.get(el)
            if not isinstance(d, h5py.Dataset):
                raise TypeError("HDF5 file {f} element {d} should be a dataset "
                                "not a {t}."
                                .format(f=path, d=d, t=type(d)))
    for group in required_groups:
        g = f.get(group)
        if g is None:
            raise KeyError("HDF5 file {f} is missing group {g}."
                           .format(f=path, g=group))
        elif not isinstance(g, h5py.Group):
            raise TypeError("HDF5 file {f} {g} should be a group, but "
                            "it is a {t}."
                            .format(f=path, g=group, t=type(g)))
    return True


class ReadError(Exception):
    pass


def _validate_read_type(reads, expected_read_type):
    total = 0
    pattern = re.compile('ccs')
    if expected_read_type == 'ccs':
        read_type_list = [True if pattern.search(
            record.name) else False for record in reads]

        if all(read_type_list) != True:
            log.debug(
                "Parsed {n} reads, but not all names indicate that they are CCS".format(n=total))
            raise ReadError("Not all reads are detected as CCS")
        else:
            log.debug("Parsed {n} CCS reads".format(n=total))
            return True

    if expected_read_type == 'subread':
        read_type_list = [True if not pattern.search(
            record.name) else False for record in reads]

        if all(read_type_list) != True:
            log.debug("Parsed {n} Subreads".format(n=total))
            raise ReadError("Not all reads are detected as Subreads")
        else:
            log.debug("Parsed {n} Subreads".format(n=total))
            return True


class CsvValidator(ValidatorBase):

    def __init__(self, path, fields):
        super(CsvValidator, self).__init__(path)
        # list of expected CSV headers
        self.fields = fields

    def validate_file(self, path):
        return _validate_csv(path, self.fields)


class GzipValidator(ValidatorBase):

    def validate_file(self, path):
        return True


class JsonValidator(ValidatorBase):

    def validate_file(self, path):
        with open(path, 'r') as f:
            json.loads(f.read())
        return True


class JsonReportValidator(ValidatorBase):

    """Smoke test to pbreports created json files

    This should be fleshed out more.
    """

    def validate_file(self, path):
        return _validate_json_report(path)


class XmlValidator(ValidatorBase):

    def validate_file(self, path):
        t = ElementTree(file=path)
        r = t.getroot()
        return True


class FofnValidator(ValidatorBase):

    def validate_file(self, path):
        fs = fofn_to_files(path)
        return True


class MappingChemistryXmlValidator(ValidatorBase):

    """ Should have this scheme

    <Map><Mapping><Movie>m130306_023456_42129_c100422252550000001523053002121396_s1_p0</Movie><SequencingChemistry>C2</SequencingChemistry></Mapping></Map>
    """

    def validate_file(self, path):
        return _validate_chemistry_mapping_xml(path)


class MappingChemistryValidator(MappingChemistryXmlValidator):

    def __init__(self, path, chemistries):
        """Validate that the chemistries are correctly determined.

        :param path: Path to xml files
        :param chemistries: Unordered list of chemistries

        This is primarily intended to be used in User tests
        """
        super(MappingChemistryValidator, self).__init__(path)
        self.chemistries = chemistries

    def validate_file(self, path):
        p = super(MappingChemistryXmlValidator, self).validate_file(path)
        return _validate_chemistry_mapping(p, self.chemistries)


class _PbcoreReaderValidator(ValidatorBase):
    READER_CLASS = FastaReader

    def validate_file(self, path):
        total = 0
        with self.READER_CLASS(path) as f:
            for record in f:
                total += 1

        # if we've got here, assume the file is valid
        log.debug("{r} parsed {n} Records from {p}".format(
            n=total, p=path, r=self.READER_CLASS.__name__))
        return True


class _SubreadFastxValidator(ValidatorBase):
    READER_CLASS = FastaReader

    def validate_file(self, path):
        with self.READER_CLASS(path) as f:
            return _validate_read_type(f, 'subread')


class _CcsreadFastxValidator(ValidatorBase):
    READER_CLASS = FastaReader

    def validate_file(self, path):
        with self.READER_CLASS(path) as f:
            return _validate_read_type(f, 'ccs')


class _BarcodedSubreadTypeValidator(ValidatorBase):
    READER_CLASS = FastqReader

    def validate_file(self, path):
        with TarFile.open(path, 'r') as fastq_files:
            members = fastq_files.getmembers()
            extract_dir = os.path.dirname(path)
            for barcode in members:
                fastq_files.extract(barcode, path=extract_dir)
            barcode_list = [os.path.join(extract_dir, member.name) for member in members]
            summary = [_validate_read_type(FastqReader(fastq), 'subread')
                       for fastq in barcode_list]

        if all(summary) != True:
            raise ReadError("Not all barcoded reads detected as Subreads!")
        else:
            return True


class _BarcodedCcsTypeValidator(ValidatorBase):
    READER_CLASS = FastqReader

    def validate_file(self, path):
        with TarFile.open(path, 'r') as fastq_files:
            members = fastq_files.getmembers()
            extract_dir = os.path.dirname(path)
            for barcode in members:
                fastq_files.extract(barcode, path=extract_dir)
            barcode_list = [os.path.join(extract_dir, member.name) for member in members]
            summary = [_validate_read_type(FastqReader(fastq), 'ccs')
                       for fastq in barcode_list]

        if all(summary) != True:
            raise ReadError("Not all barcoded reads detected as ccs!")
        else:
            return True


class SubreadFastqValidator(_SubreadFastxValidator):
    READER_CLASS = FastqReader


class SubreadFastaValidator(_SubreadFastxValidator):
    READER_CLASS = FastaReader


class CcsreadFastqValidator(_CcsreadFastxValidator):
    READER_CLASS = FastqReader


class CcsreadFastaValidator(_CcsreadFastxValidator):
    READER_CLASS = FastaReader


class BarcodedSubreadFastqValidator(_BarcodedSubreadTypeValidator):
    pass


class BarcodedCcsFastqValidator(_BarcodedCcsTypeValidator):
    pass


class FastaValidator(_PbcoreReaderValidator):
    READER_CLASS = FastaReader


class FastqValidator(_PbcoreReaderValidator):
    READER_CLASS = FastqReader


class GffValidator(_PbcoreReaderValidator):
    READER_CLASS = GffReader


class _GzippedPbcoreReaderValidator(ValidatorBase):
    READER_CLASS = FastaReader

    def validate_file(self, path):
        total = 0
        with gzip.open(path) as f:
            with self.READER_CLASS(f) as r:
                for record in r:
                    total += 1

        log.debug(
            "{k} validated {p} with {n} Records".format(k=self.name, p=path, n=total))
        return True


class GzippedFastaValidator(_GzippedPbcoreReaderValidator):
    READER_CLASS = FastaReader


class GzippedFastqValidator(_GzippedPbcoreReaderValidator):
    READER_CLASS = FastqReader


class GzippedGffValidator(_GzippedPbcoreReaderValidator):
    READER_CLASS = GffReader


class Hdf5Validator(ValidatorBase):

    def __init__(self, path, required_groups=None, required_dset_basenames=None):
        super(Hdf5Validator, self).__init__(path)
        self.required_groups = required_groups
        self.required_dset_basenames = required_dset_basenames

    def validate_file(self, path):
        return _validate_hdf5(path, self.required_groups, self.required_dset_basenames)


class BtiValidator(ValidatorBase):

    """Validator for bamtools index (BTI) files."""

    def validate_file(self, path):
        return _validate_bti(path)


class BaiValidator(ValidatorBase):

    """Validator for samtools index (BAI) files."""

    def validate_file(self, path):
        return _validate_bai(path)
