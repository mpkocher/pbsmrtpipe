import logging
import os
import unittest

from base import TEST_DATA_DIR, get_temp_file

import pbsmrtpipe.loader
import pbsmrtpipe.pb_io as IO
from pbsmrtpipe.models import PipelineChunk, Constants

REGISTERED_TASKS, REGISTERED_FILE_TYPES, REGISTERED_CHUNK_OPERATORS, REGISTERED_PIPELINES = pbsmrtpipe.loader.load_all()


log = logging.getLogger(__name__)


class TestParsingPresetXml(unittest.TestCase):
    FILE_NAME = 'cli_preset_01.xml'

    def setUp(self):
        self.path = os.path.join(TEST_DATA_DIR, self.FILE_NAME)

    def test_01(self):
        preset_record = IO.parse_pipeline_preset_xml(self.path)
        self.assertIsInstance(preset_record, IO.PresetRecord)

    def _to_wopts(self, file_path):
        preset_record = IO.parse_pipeline_preset_xml(file_path)
        d = dict(preset_record.workflow_options)
        wopts = IO.WorkflowLevelOptions.from_id_dict(d)
        return wopts

    def test_to_wopts(self):
        wopts = self._to_wopts(self.path)
        self.assertIsInstance(wopts, IO.WorkflowLevelOptions)

    def test_to_dict(self):
        preset_record = IO.parse_pipeline_preset_xml(self.path)
        d = dict(preset_record.workflow_options)
        wopts = IO.WorkflowLevelOptions.from_id_dict(d)
        self.assertIsInstance(wopts.to_dict(), dict)

    def test_workflow_level_opts_from_defaults(self):
        w = IO.WorkflowLevelOptions.from_defaults()
        self.assertIsInstance(w, IO.WorkflowLevelOptions)

    def test_workflow_level_opts_from_dict(self):
        d = {'pbsmrtpipe.options.max_nproc': 12}
        w = IO.WorkflowLevelOptions.from_id_dict(d)
        self.assertIsInstance(w, IO.WorkflowLevelOptions)


class TestParsingFetchPresetXml(TestParsingPresetXml):
    FILE_NAME = 'fetch_preset.xml'


class TestWriteSchemaOptsToXML(unittest.TestCase):

    def _to_opts(self):
        return IO.REGISTERED_WORKFLOW_OPTIONS

    def test_to_xml(self):
        wopts = self._to_opts()
        xml = IO.schema_workflow_options_to_xml(wopts)
        log.debug(xml)
        self.assertIsNotNone(xml)

    def test_load_preset(self):
        xml = IO.schema_workflow_options_to_xml(self._to_opts())
        preset_xml = get_temp_file(suffix="_preset.xml")
        log.debug(preset_xml)
        with open(preset_xml, 'w') as w:
            w.write(str(xml))

        preset_record = IO.parse_pipeline_preset_xml(preset_xml)
        workflow_level_opts = preset_record.to_workflow_level_opt()
        self.assertTrue(len(workflow_level_opts), len(self._to_opts()))


class TestBindingParsing(unittest.TestCase):
    FILE_NAME = 'hello_world_workflow.xml'
    NBINDINGS = 3
    NENTRY_POINTS = 1

    def _to_f(self):
        return os.path.join(TEST_DATA_DIR, self.FILE_NAME)

    def _to_builder_record(self):
        return IO.parse_pipeline_template_xml(self._to_f(), REGISTERED_PIPELINES)

    def test_nbindings(self):
        builder_record = self._to_builder_record()
        emsg = "Number of Bindings are incorrect"
        self.assertEqual(len(builder_record.bindings), self.NBINDINGS, emsg)


class TestWriteChunk(unittest.TestCase):

    def test_write_chunks(self):

        def f(i):
            return {"{c}movie_fofn_id".format(c=Constants.CHUNK_KEY_PREFIX): "/path/to_movie-{i}.fofn".format(i=i),
                    "{c}region_fofn_id".format(c=Constants.CHUNK_KEY_PREFIX): "/path/rgn_{i}.fofn".format(i=i)}

        to_i = lambda i: "chunk-id-{i}".format(i=i)
        to_p = lambda i: PipelineChunk(to_i(i), **f(i))

        nchunks = 5
        pipeline_chunks = [to_p(i) for i in xrange(nchunks)]
        log.debug(pipeline_chunks)
        tmp_name = get_temp_file("_chunk.json")

        IO.write_pipeline_chunks(pipeline_chunks, tmp_name, "Example chunk file")

        pchunks = IO.load_pipeline_chunks_from_json(tmp_name)
        self.assertEquals(len(pchunks), nchunks)


class TestLoadOperators(unittest.TestCase):

    def test_sanity_load_installed_operators(self):
        import pbsmrtpipe.loader as L

        rtasks, rfile_types, chunk_operators, pipelines = L.load_all()

        self.assertTrue(len(chunk_operators) > 0)

