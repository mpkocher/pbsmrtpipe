import json
import logging
import os
import random
import string
import tempfile

from pbcore.io import FastaRecord, FastqRecord, FastaWriter, FastqWriter

import pbsmrtpipe.pb_io as IO
import pbsmrtpipe.bgraph as B

from pbsmrtpipe.models import TaskStates, FileTypes

from pbsmrtpipe.models import RunnableTask
from pbsmrtpipe.report_model import Report, Attribute

log = logging.getLogger(__name__)


def _load_env_preset(env_var):
    path = os.environ.get(env_var, None)

    if path is None:
        return None
    else:
        return IO.parse_pipeline_preset_xml(path)



class Constants(object):
    SEQ = ('A', 'C', 'G', 'T')


def _random_dna_sequence(min_length=100, max_length=1000):
    n = random.choice(list(xrange(min_length, max_length)))
    return "".join([random.choice(Constants.SEQ) for _ in xrange(n)])


def _to_fasta_record(header, seq):
    return FastaRecord(header, seq)


def _to_fastq_record(header, seq):
    quality = [ord(random.choice(string.ascii_letters)) for _ in seq]
    return FastqRecord(header, seq, quality=quality)


def __to_fastx_records(n, _to_seq_func, _to_record_func):
    """

    :param n:
    :param _to_seq_func: () => DNA seq
    :param _to_record_func: (header, dna_seq) => Record
    :return: Fastq/Fasta Record
    """
    for i in xrange(n):
        header = "record_{i}".format(i=i)
        seq = _to_seq_func()
        r = _to_record_func(header, seq)
        yield r


def to_fasta_records(n):
    return __to_fastx_records(n, _random_dna_sequence, _to_fasta_record)


def to_fastq_records(n):
    return __to_fastx_records(n, _random_dna_sequence, _to_fastq_record)


def write_fastx_records(fastx_writer_klass, records, path):
    n = 0
    with fastx_writer_klass(path) as w:
        for record in records:
            n += 1
            w.writeRecord(record)

    log.debug("Completed writing {n} records to {p}".format(n=n, p=path))
    return 0


def write_random_fasta_records(path, nrecords=100):
    return write_fastx_records(FastaWriter, to_fasta_records(nrecords), path)

def write_random_fastq_records(path, nrecords=100):
    return write_fastx_records(FastqWriter, to_fastq_records(nrecords), path)


def _to_random_tmp_fofn(nrecords):
    def _to_f(name):
        suffix = "".join([name, '.fofn'])
        t = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        t.close()
        return t.name

    paths = []
    for x in xrange(nrecords):
        path = _to_f("random_{i}".format(i=x))
        paths.append(path)

    return paths


def write_fofn(path, file_paths):
    with open(path, 'w') as w:
        w.write("\n".join([str(f) for f in file_paths]))
    return 0


def write_random_fofn(path, nrecords):
    """Write a generic fofn"""

    fofns = _to_random_tmp_fofn(nrecords)
    write_fofn(path, fofns)
    return fofns


def write_random_report(path, nrecords):

    attributes = [Attribute("mock_attr_{i}".format(i=i), i, name="Attr {i}".format(i=i)) for i in xrange(nrecords)]
    r = Report("mock_report", attributes=attributes)
    r.write_json(path)
    return r


def write_generic_txt_file(path, nrecords):
    with open(path, 'w') as w:
        for i in xrange(nrecords):
            w.write("Record-{i}".format(i=i))

    return 0


def write_mock_file_by_type(path, nrecords):
    _, ext = os.path.splitext(path)
    _d = {".fastq": write_random_fastq_records,
          ".fasta": write_random_fasta_records,
          ".json": write_random_report,
          ".fofn": write_random_fofn}
    func = _d.get(ext, write_generic_txt_file)
    func(path, nrecords)
    return 0

def _mock_task_exe_runner(task_dir, output_files):

    log.debug("Running Mock TASK {d}".format(d=task_dir))

    stderr = os.path.join(task_dir, 'stderr')
    stdout = os.path.join(task_dir, 'stdout')

    def _write_mock_file(p, message="MOCK_FILE"):
        log.debug("Writing mock file {p}".format(p=p))
        with open(p, 'w') as w:
            w.write(message + "\n")

    for x in [stderr, stdout]:
        _write_mock_file(x)

    for mock_file in output_files:
        # just to make sure json files are well-formed
        if mock_file.endswith('.json'):
            d = dict(data="MOCK DATA")
            s = json.dumps(d, indent=4)
            _write_mock_file(mock_file, s)
        else:
            _write_mock_file(mock_file, "MOCK OUTPUT FILE")

    B.write_mock_task_report(task_dir)

    return TaskStates.SUCCESSFUL, "", random.randint(1, 1000)


def mock_run_task_manifest(path):
    output_dir = os.path.dirname(path)
    rt = RunnableTask.from_manifest_json(path)
    return _mock_task_exe_runner(output_dir, rt.output_files)
