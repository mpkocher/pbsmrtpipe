import os
import unittest
import logging

from pbsmrtpipe.utils import (Singleton, validate_movie_file_name, which,
                              HTML_TEMPLATE_ENV)
from pbsmrtpipe.legacy.input_xml import InputXml
from base import TEST_DATA_DIR, SIV_TEST_DATA_DIR


log = logging.getLogger(__name__)


@unittest.skipIf(not os.path.exists(SIV_TEST_DATA_DIR), "Unable to find test {s}".format(s=SIV_TEST_DATA_DIR))
class TestUtils(unittest.TestCase):

    def test_input_xml(self):
        name = 'rs_input.xml'
        file_name = os.path.join(TEST_DATA_DIR, name)
        input_xml = InputXml.from_file(file_name)

        self.assertEqual(len(input_xml.movies), 2)

    def test_which(self):
        exe = 'python'
        path = which(exe)
        self.assertIsNotNone(path)

    def test_bad_which(self):
        exe = 'pythonz'
        path = which(exe)
        self.assertIsNone(path)


class TestValidateMovie(unittest.TestCase):

    def test_bas(self):
        m = 'm120201_042231_42129_c100275262550000001523007907041260_s1_p0.bas.h5'
        self.assertTrue(validate_movie_file_name(m))

    def test_bad_bas(self):
        m = 'm_BAD_120201_042231_42129_c100275262550000001523007907041260_s1_p0.bas.h5'
        self.assertFalse(validate_movie_file_name(m))

    def test_bax(self):
        m = 'm130306_023456_42129_c100422252550000001523053002121396_s1_p0.2.bax.h5'
        self.assertTrue(validate_movie_file_name(m))

    def test_bad_bax(self):
        m = 'm_BAD_130306_023456_42129_c100422252550000001523053002121396_s1_p0.2.bax.h5'
        self.assertFalse(validate_movie_file_name(m))


class TestSingleton(unittest.TestCase):

    def test_basic(self):
        class Lithium(object):
            __metaclass__ = Singleton

            def __init__(self):
                self.name = 'Lithium'
                self.number = 3

        a = Lithium()
        b = Lithium()
        self.assertEqual(id(a), id(b))


class TestLoadJinjaTemplate(unittest.TestCase):

    def test_can_find_and_load_template(self):
        template_name = 'index.html'
        t = HTML_TEMPLATE_ENV.get_template(template_name)
        self.assertIsNotNone(t)

    @unittest.skip
    def test_render_index_html(self):
        template_name = 'index.html'
        t = HTML_TEMPLATE_ENV.get_template(template_name)
        self.assertIsNotNone(t)

        d = dict(value=1)
        html = t.render(**d)
        self.assertIsInstance(html, basestring)
