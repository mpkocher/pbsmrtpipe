import os
import logging
import functools
import types

from pbsmrtpipe.testkit.validators import ValidatorBase

log = logging.getLogger(__name__)


class Constants(object):
    # Class constants used in TestBase
    FILES = 'FILES'
    DIRS = 'DIRS'

    HTML_DIRS = "HTML_DIRS"
    HTML_FILES = 'HTML_FILES'

    WORKFLOW_FILES = 'WORKFLOW_FILES'


def _sanitize_test_name(name):
    return name.replace('.', '_').replace('\\', '_').replace('-', '_')


def __test_job_resource(func, name, dir_name):

    def wrapper(self):
        path = os.path.join(self.job_dir, dir_name)
        emsg = "Unable to find '{p}'".format(p=path)
        self.assertTrue(func(path), emsg)

    wrapper.__name__ = name
    return wrapper


def _test_job_resource_dir_exists(dir_name):
    return __test_job_resource(os.path.isdir, 'test_job_resource_dir_exists', dir_name)


def _test_job_resource_file_exists(file_name):
    return __test_job_resource(os.path.isfile, 'test_job_resource_file_exists', file_name)


def _bolt_on_test_func(cls, test_func, method_name):
    """Bolt function dynamically on Class"""
    if not isinstance(test_func, types.FunctionType):
        _d = dict(f=test_func, t=type(test_func), g=types.FunctionType)

        raise TypeError("Unable to bolt-on function '{f}'. Got type {t} expected type {g}".format(**_d))

    f = types.MethodType(test_func, None, cls)
    # log.debug("Adding Test Method '{f}' to class {c}".format(f=method_name, c=cls.__name__))
    setattr(cls, method_name, f)
    cls._was_monkey_patched = True


def _bolt_on_resources(validation_func_wrapper, cls, class_constant_id):
    """

    validation_func_wrapper is func func(resource_name_or_validator) -> func(self)
    where self is func that will be bolted on to the cls.

    :param validation_func_wrapper:
    :param cls:
    :param class_constant_id:
    :return:
    """

    # log.debug("Patching {f} on class {c}".format(f=class_constant_id, c=cls.__name__))

    for file_name_or_validator in getattr(cls, class_constant_id):
        if isinstance(file_name_or_validator, str):
            func = validation_func_wrapper(file_name_or_validator)
            fname = "_".join([func.__name__, _sanitize_test_name(file_name_or_validator)])
            _bolt_on_test_func(cls, func, fname)
        elif isinstance(file_name_or_validator, ValidatorBase):
            log.warn("Validators are not yet supported")
        else:
            raise ValueError("Unsupported {f} type {x}".format(f=class_constant_id))


def _bolt_on_file_existence(cls, class_constant_id):
    return _bolt_on_resources(_test_job_resource_file_exists, cls, class_constant_id)


def _bolt_on_dir_existence_test(cls, class_constant_id):
    return _bolt_on_resources(_test_job_resource_dir_exists, cls, class_constant_id)


def _test_job_resource_html_dirs(dir_name):
    return __test_job_resource(os.path.isdir, 'test_job_resource_html', os.path.join('html', dir_name))


def _bolt_on_resource_html_dir(cls, class_constant_id):
    return _bolt_on_resources(_test_job_resource_html_dirs, cls, class_constant_id)


def _test_job_resource_workflow_dir_file(file_name):
    return __test_job_resource(os.path.isfile, 'test_job_resource_workflow_files', os.path.join('workflow', file_name))


def _bolt_on_resource_workflow_dir_file(cls, class_constant_id):
    return _bolt_on_resources(_test_job_resource_workflow_dir_file, cls, class_constant_id)


def monkey_patch(cls):
    cls._was_monkey_patched = True

    if hasattr(cls, Constants.FILES):
        _bolt_on_file_existence(cls, Constants.FILES)

    if hasattr(cls, Constants.DIRS):
        _bolt_on_dir_existence_test(cls, Constants.DIRS)

    if hasattr(cls, Constants.HTML_DIRS):
        _bolt_on_resource_html_dir(cls, Constants.HTML_DIRS)

    if hasattr(cls, Constants.WORKFLOW_FILES):
        _bolt_on_resource_workflow_dir_file(cls, Constants.WORKFLOW_FILES)

    log.info("Completed monkey_patch on {c}".format(c=cls.__name__))
    return cls