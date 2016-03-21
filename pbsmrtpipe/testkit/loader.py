"""Dynamically load test cases from a cfg file"""

import xml.etree.ElementTree as ET
import importlib
import logging
import ConfigParser
import types
import unittest
import warnings

import pbsmrtpipe.testkit.base


log = logging.getLogger(__name__)


def _parse_tests(s):
    x = s.split(',') if ',' in s else [s]
    return [i.strip() for i in x]


def _get_section_items(cf, section_name):
    names = cf.options(section_name)
    items = []
    for name in names:
        value = cf.get(section_name, name)
        items.append((name, value))
    return items


def _load_test_cases_from_module(module_instance):
    """Manually load TestCases from module_instance.

    :param module_instance: Instance of python module_instance which contains
    TestCase classes

    :returns: List of TestCase instances

    This is done explicitly to allow disabling tasks and to deal with
    programmatically calling nose.core.TestProgram(). Manually building a
    TestSuite and passing it to nose.core.TestProgram causes issues with how
    nose loads the plugins (i.e., wraps the context). See the itertools.chain
    usage in nose.core.TestProgram().
    """
    assert isinstance(module_instance, types.ModuleType)

    test_class_names = []
    for name in dir(module_instance):
        a = getattr(module_instance, name)

        # this is a really hacky, but works. Using isinstance(a, unittest.TestCase)
        # doesn't work
        if isinstance(a, type):
            # The _is_test the is magic class var that labels class as Containers that
            # should be loaded as test classes.
            if hasattr(a, '_is_test'):
                test_class_names.append(name)

    # Explicitly load TestCases
    test_cases = []
    for test_class_name in test_class_names:
        test_case = unittest.TestLoader().loadTestsFromName(test_class_name,
                                                            module_instance)
        test_cases.append(test_case)

    if not test_class_names:
        msg = "Unable to find any TestCase instances in module_instance {m}".format(m=module_instance)
        warnings.warn(msg + "\n")

    # print "test cases", test_cases
    return test_cases


def parse_cfg_file(path):

    p = ConfigParser.ConfigParser()
    p.read(path)

    test_cases = []

    if p.has_section('tests'):
        xs = _get_section_items(p, 'tests')

        for module_base_, raw_tests in xs:
            for t in _parse_tests(raw_tests):
                module_name = ".".join([module_base_, t])
                m = importlib.import_module(module_name)
                ts = _load_test_cases_from_module(m)
                for x in ts:
                    # Ignore empty
                    if x.countTestCases > 0:
                        test_cases.append(x)

    return test_cases


def dtype_and_uuid_from_dataset_xml(dataset_xml):
    tree = ET.parse(dataset_xml)
    root = tree.getroot()
    metatype = root.attrib['MetaType']
    unique_id = root.attrib['UniqueId']
    return metatype, unique_id
