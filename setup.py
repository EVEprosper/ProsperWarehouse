"""Wheel for ProsperWarehouse project"""
from codecs import open
import importlib
from os import path, listdir

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

HERE = path.abspath(path.dirname(__file__))
__package_name__ = 'ProsperWarehouse'
__library_name__ = 'warehouse'

def get_version(package_name):
    """find __version__ for making package

    Args:
        package_path (str): path to _version.py folder (abspath > relpath)

    Returns:
        str: __version__ value

    """
    module = 'prosper.' + package_name + '._version'
    package = importlib.import_module(module)

    version = package.__version__

    return version

def hack_find_packages(include_str):
    """patches setuptools.find_packages issue

    setuptools.find_packages(path='') doesn't work as intended

    Returns:
        (:obj:`list` :obj:`str`) append <include_str>. onto every element of setuptools.find_pacakges() call

    """
    new_list = [include_str]
    for element in find_packages(include_str):
        new_list.append(include_str + '.' + element)

    return new_list

def include_all_subfiles(*args):
    """Slurps up all files in a directory (non recursive) for data_files section

    Note:
        Not recursive, only includes flat files

    Returns:
        :obj:`list`: list of all non-directories in a file

    """
    file_list = []
    for path_included in args:
        local_path = path.join(HERE, path_included)

        for file in listdir(local_path):
            file_abspath = path.join(local_path, file)
            if path.isdir(file_abspath):    #do not include sub folders
                continue
            file_list.append(path_included + '/' + file)

    return file_list

class PyTest(TestCommand):
    """PyTest cmdclass hook for test-at-buildtime functionality

    http://doc.pytest.org/en/latest/goodpractices.html#manual-integration

    """
    user_options = [('pytest-args=', 'a', 'Arguments to pass to pytest')]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = [
            'tests',
            '-rx',
            '-v',
            '--cov=prosper/' + __library_name__,
            '--cov-report=term-missing',
            '--cov-config=.coveragerc'
        ]    #load defaults here

    def run_tests(self):
        import shlex
        # import here, because outside the eggs aren't loaded
        import pytest
        pytest_commands = []
        try:
            pytest_commands = shlex.split(self.pytest_args)
        except AttributeError:
            pytest_commands = self.pytest_args
        errno = pytest.main(pytest_commands)
        exit(errno)

class QuickTest(PyTest):
    """wrapper for quick-testing for devs"""
    user_options = [('pytest-args=', 'a', 'Arguments to pass to pytest')]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = [
            'tests',
            '-rx',
            '-n',
            '4',
            '--cov=prosper/' + __library_name__,
            '--cov-report=term-missing',
            '--cov-config=.coveragerc'
        ]

with open('README.rst', 'r', 'utf-8') as f:
    readme = f.read()

setup(
    name=__package_name__,
    description='Database connector and nosql validator for Prosper projects',
    long_description=readme,
    author='John Purcell',
    author_email='prospermarketshow@gmail.com',
    url='https://github.com/EVEprosper/' + __package_name__,
    download_url='https://pypi.python.org/pypi/ProsperWarehouse',
    version=get_version(__library_name__),
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
    ],
    keywords='prosper eveonline api database',
    packages=hack_find_packages('prosper'),
    include_package_data=True,
    data_files=[

    ],
    package_data={
        '': ['LICENSE', 'README.rst'],
        'prosper':[
            'warehouse/schemas/*',
            'warehouse/warehouse.cfg',
            'warehouse/master.schema'
        ]
    },
    install_requires=[
        'prospercommon',
        'jsonschema',
        'pymongo',
        'tinymongo',
        'tinydb_serialization',
        #'PyYAML~=3.12',
        'pandas',
        'ujson',
        'semantic_version'
    ],
    tests_require=[
        'pytest',
        'pytest_cov',
        'pytest-xdist'
    ],
    extras_require={
        'dev':[
            'plumbum',
            'pandas-datareader',
            'sphinx',
            'sphinxcontrib-napoleon'
        ]
    },
    cmdclass={
        'test':PyTest,
        'fast': QuickTest
    }
)
