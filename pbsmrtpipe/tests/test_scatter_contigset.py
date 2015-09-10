
import re

from pbcommand.pb_io.common import load_pipeline_chunks_from_json
from pbcore.io import ContigSet
import pbcommand.testkit.core

from base import get_temp_file

class TestScatterContigSet(pbcommand.testkit.core.PbTestScatterApp):
    DRIVER_BASE = "python -m pbsmrtpipe.tools_dev.scatter_contigset"
    INPUT_FILES = [
        get_temp_file(suffix=".contigset.xml")
    ]
    MAX_NCHUNKS = 12
    RESOLVED_MAX_NCHUNKS = 12
    CHUNK_KEYS = ("$chunk.contigset_id",)

    @classmethod
    def setUpClass(cls):
        super(TestScatterContigSet, cls).setUpClass()
        fasta_file = re.sub(".contigset.xml", ".fasta", cls.INPUT_FILES[0])
        rec = [ ">chr%d\nacgtacgtacgt"%x for x in range(251) ]
        with open(fasta_file, "w") as f:
            f.write("\n".join(rec))
        cs = ContigSet(fasta_file)
        cs.write(cls.INPUT_FILES[0])

    def run_after(self, rtc, output_dir):
        json_file = rtc.task.output_files[0]
        chunks = load_pipeline_chunks_from_json(json_file)
        n_rec = 0
        with ContigSet(self.INPUT_FILES[0]) as f:
            n_rec = len([ r for r in f ])
        n_rec_chunked = 0
        for chunk in chunks:
            d = chunk.chunk_d
            with ContigSet(d["$chunk.contigset_id"]) as cs:
                n_rec_chunked += len([ r for r in cs ])
        self.assertEqual(n_rec_chunked, n_rec)
