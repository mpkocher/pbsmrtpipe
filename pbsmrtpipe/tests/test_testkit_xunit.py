
from xml.etree import ElementTree
import unittest

from pbcommand.testkit import pb_requirements

from pbsmrtpipe.testkit import xunit as X


def _get_and_run_test_suite():
    class MyTestClass(unittest.TestCase):
        @pb_requirements("SL-1")
        def test_1(self):
            self.assertTrue(True)
        @pb_requirements("SL-2")
        def test_2(self):
            self.assertTrue(False)
        @pb_requirements("SL-3")
        @unittest.skip("Skipped")
        def test_3(self):
            self.assertTrue(True)
    tests = unittest.TestLoader().loadTestsFromTestCase(MyTestClass)
    suite = unittest.TestSuite(tests)
    result = unittest.TestResult()
    suite.run(result)
    return suite, result


class TestXunitOutput(unittest.TestCase):

    def test_convert_to_xunit(self):
        suite, result = _get_and_run_test_suite()
        x = X.convert_suite_and_result_to_xunit([suite], result,
            name="pbsmrtpipe.tests.test_xunit_output")
        print x
        root = ElementTree.fromstring(str(x))
        self.assertEqual(root.tag, "testsuite")
        self.assertEqual(root.attrib["failures"], "1")
        self.assertEqual(root.attrib["skip"], "1")
        tests = list(root.findall("testcase"))
        self.assertEqual(len(tests), 3)
        requirements = []
        for el in root.findall("properties"):
            for p in el.findall("property"):
                requirements.append(p.attrib["value"])
        self.assertEqual(requirements, ["SL-1","SL-2","SL-3"])
