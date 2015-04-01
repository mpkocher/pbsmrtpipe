import unittest
import logging

import pbsmrtpipe.loader as L
import pbsmrtpipe.bgraph as B

log = logging.getLogger(__name__)


class TestPipelineSanity(unittest.TestCase):
    def test_all_sane(self):
        """Test that all pipelines are well defined"""
        errors = []
        rtasks, rfiles_types, chunk_operators, pipelines = L.load_all()

        for pipeline_id, pipeline in pipelines.items():
            emsg = "Pipeline {p} is not valid.".format(p=pipeline_id)
            log.debug("Checking Sanity of registered Pipeline {i}".format(i=pipeline_id))
            log.info(pipeline_id)
            log.debug(pipeline)
            try:
                bg = B.binding_strs_to_binding_graph(rtasks, pipeline.all_bindings)
                B.validate_binding_graph_integrity(bg)
                log.info("Pipeline {p} is valid.".format(p=pipeline_id))
            except Exception as e:
                m = emsg + "Error " + e.message
                log.error(m)
                errors.append(emsg)
                log.error(emsg)
                log.error(e)

        msg = "\n".join(errors) if errors else ""
        self.assertEqual([], errors, msg)
