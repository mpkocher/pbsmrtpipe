from collections import namedtuple
import json
import csv
import os
import unittest
import logging

import pbsmrtpipe.tools.gather as G

from base import get_temp_file, get_temp_dir

log = logging.getLogger(__name__)


class Record(object):
    def __init__(self, idx, alpha):
        self.idx = idx
        self.alpha = alpha

    def to_dict(self):
        return dict(id=self.idx, alpha=self.alpha)


def _to_n_records(nrecords):
    for i in xrange(nrecords):
        r = Record(i, 90)
        yield r


def _write_records_to_csv(records, output_csv):
    fields = records[0].to_dict().keys()
    with open(output_csv, 'w') as w:
        writer = csv.DictWriter(w, fieldnames=fields)
        writer.writeheader()
        writer.writerows([r.to_dict() for r in records])


class TestCsvGather(unittest.TestCase):

    def test_smoke(self):
        t = get_temp_file(suffix="-records-1.csv")
        _write_records_to_csv(list(_to_n_records(100)), t)

        t2 = get_temp_file(suffix="-records-2.csv")
        _write_records_to_csv(list(_to_n_records(57)), t2)

        tg = get_temp_file(suffix="records-gather.csv")
        G.gather_csv([t, t2], tg)

        nrecords = 0
        with open(tg, 'r') as r:
            reader = csv.DictReader(r)
            log.debug(reader.fieldnames)
            for _ in reader:
                nrecords += 1

        self.assertEqual(nrecords, 157)


def _write_stats_to_json(stats, output_json):
    with open(output_json, 'w') as w:
        w.write(json.dumps(stats))


class TestJsonGather(unittest.TestCase):

    def test_smoke(self):
        t = get_temp_file(suffix="-stats-1.json")
        _write_stats_to_json({'n_reads':549,'n_zmws':100}, t)
        t2 = get_temp_file(suffix="-stats-2.json")
        _write_stats_to_json({'n_reads':733,'n_zmws':100}, t2)

        tg = get_temp_file(suffix="stats-gather.json")
        G.gather_json_stats([t, t2], tg)

        with open(tg, 'r') as f:
            stats = json.loads(f.read())
            self.assertEqual(stats['n_reads'], 549+733)
            self.assertEqual(stats['n_zmws'], 200)
