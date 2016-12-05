"""Wheel for ProsperWarehouse project"""

from os import path, listdir
from setuptools import setup, find_packages

HERE = path.abspath(path.dirname(__file__))

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
        self.pytest_args = ['test']    #load defaults here

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

setup(
    name='ProsperWarehouse',
    author='John Purcell',
    author_email='prospermarketshow@gmail.com',
    url='https://github.com/EVEprosper/ProsperWarehouse',
    download_url='https://github.com/EVEprosper/ProsperWarehouse/tarball/v0.0.3',
    version='0.0.4',
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 3.5'
    ],
    keywords='prosper eveonline api database',
    packages=hack_find_packages('prosper'),
    #data_files=[
    #    #TODO: license + README
    #    #Can't use data_files with gemfury upload (need `bdist_wheel`)
    #    ('SQL', include_all_subfiles('SQL')),
    #    ('docs', include_all_subfiles('docs'))
    #],
    package_data={
        'prosper':[
            'table_configs/table_config.cfg'
        ]
    },
    install_requires=[
        'configparser==3.5.0',
        'mysql-connector==2.1.4',
        'numpy==1.11.1',
        'pandas==0.18.1',
        'plumbum==1.6.2',
        'python-dateutil==2.5.3',
        'pytz==2016.6.1',
        'six==1.10.0',
        'requests==2.11.1',
        'ProsperCommon==0.3.3'
    ]
)
