import os
import unittest
import logging

import pbsmrtpipe.tools.chunker as CH

from base import get_temp_file, get_temp_dir

log = logging.getLogger(__name__)


def _to_test_fofn(n, file_base_name):
    """return a path to fofn"""

    fofn_name = ".".join([file_base_name, ".fofn"])
    fofn = get_temp_file(suffix=fofn_name)

    fofn_files = []
    for i in xrange(n):
        name = "-".join([file_base_name, str(i)])
        f = get_temp_file(name)
        fofn_files.append(f)

    with open(fofn, 'w') as f:
        f.write("\n".join(fofn_files))

    return fofn


class TestToolChunker(unittest.TestCase):

    def test_run_movie(self):

        nfiles = 5
        fofn = _to_test_fofn(nfiles, "movie_")

        chunks = CH.chunk_movie_fofn(fofn)
        log.debug(chunks)
        self.assertEqual(nfiles, len(chunks))
