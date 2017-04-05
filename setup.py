"""Wheel for ProsperWarehouse project"""

from os import path, listdir
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import importlib

HERE = path.abspath(path.dirname(__file__))

def get_version(package_name):
    """find __version__ for making package

    Args:
        package_path (str): path to _version.py folder (abspath > relpath)

    Returns:
        (str) __version__ value

    """
    module = package_name + '._version'
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
        (:obj:`list` :obj:`str`) list of all non-directories in a file

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
    user_options = [('pytest-args=', 'a', "Arguments to pass to pytest")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = [
            'tests',
            '--cov=prosper/',
            '--cov-report=term-missing'
        ]    #load defaults here

    def run_tests(self):
        import shlex
        #import here, cause outside the eggs aren't loaded
        import pytest
        pytest_commands = []
        try:    #read commandline
            pytest_commands = shlex.split(self.pytest_args)
        except AttributeError:  #use defaults
            pytest_commands = self.pytest_args
        errno = pytest.main(pytest_commands)
        exit(errno)

__package_name__ = 'ProsperWarehouse'
__version__ = get_version('prosper.warehouse')

setup(
    name=__package_name__,
    author='John Purcell',
    author_email='prospermarketshow@gmail.com',
    url='https://github.com/EVEprosper/' + __package_name__,
    download_url='https://github.com/EVEprosper/' + __package_name__ + '/tarball/v' + __version__,
    version=__version__,
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 3.5'
    ],
    keywords='prosper eveonline api database',
    packages=hack_find_packages('prosper'),
    include_package_data=True,
    data_files=[
        ('docs', include_all_subfiles('docs')),
        ('tests', include_all_subfiles('tests'))
    ],
    package_data={
        'prosper':[
            'warehouse/schemas/*',
            'warehouse/warehouse.cfg'
        ]
    },
    install_requires=[
        'ProsperCommon~=0.5.0',  #--extra-index-url=https://repo.fury.io/lockefox/
        'jsonschema~=2.6.0',
        'pymongo~=3.4.0',
        'pandas~=0.19.2',
        'semantic_version~=2.6.0'
    ],
    tests_require=[
        'pytest~=3.0.0',
        'pytest_cov~=2.4.0',
        'tinymongo~=0.1.7.dev0'
    ],
    cmdclass={
        'test':PyTest
    }
)
