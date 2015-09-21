import os
import re
from setuptools import find_packages

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

version = __import__('pbsmrtpipe').get_version()

_REQUIREMENTS_FILE = 'REQUIREMENTS.txt'
_README = 'README.md'


def _get_local_file(file_name):
    return os.path.join(os.path.dirname(__file__), file_name)


def _get_description(file_name):
    with open(file_name, 'r') as f:
        _long_description = f.read()
    return _long_description


def _get_requirements(file_name):
    with open(file_name, 'r') as f:
        lines = f.readlines()
    rx = re.compile('^[A-z]')
    requirements = [l for l in lines if rx.match(l) is not None]
    if "READTHEDOCS" in os.environ:
        requirements = [r for r in requirements if not "pbcore" in r]
    return requirements

setup(
    name='pbsmrtpipe',
    version=version,
    package_dir={'': '.'},
    packages=find_packages('.'),
    license='BSD',
    author='mpkocher',
    author_email='mkocher@pacificbiosciences.com',
    description='PacBio workflow engine for scientific computing.',
    setup_requires=['nose>=1.0'],
    # Maybe the pbtools-* should really be done in a subparser style
    entry_points={'console_scripts': ['pbsmrtpipe = pbsmrtpipe.cli:main',
                                      'pbtools-gather = pbsmrtpipe.tools.gather:main',
                                      'pbtools-chunker = pbsmrtpipe.tools.chunker:main',
                                      'pbtools-runner = pbsmrtpipe.tools.runner:main',
                                      'pbtestkit-runner = pbsmrtpipe.testkit.runner:main',
                                      'pbtestkit-multirunner = pbsmrtpipe.testkit.multirunner:main',
                                      'pbtools-report = pbsmrtpipe.tools.report_to_html:main']},
    install_requires=_get_requirements(_get_local_file(_REQUIREMENTS_FILE)),
    tests_require=['nose'],
    long_description=_get_description(_get_local_file(_README)),
    classifiers=['Development Status :: 4 - Beta'],
    include_package_data=True,
    zip_safe=False,
    # I don't really understand the package_data semantics.
    package_data={'pbsmrtpipe': ['cluster_templates/*/*.tmpl',
                                 'chunk_operators/*.xml',
                                 'schemas/*.avsc',
                                 'registered_tool_contracts_sa3/*.json',
                                 'registered_tool_contracts/*.json',
                                 'html_templates/*.html',
                                 'html_templates/*.css',
                                 'html_templates/js/*.js',
                                 'html_templates/css/*.css',
                                 'tests/data/*.xml',
                                 'tests/data/*.cfg']}
)
