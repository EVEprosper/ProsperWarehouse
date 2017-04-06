"""test_version.py: setup pytest defaults/extensions"""
from os import path
import importlib

import pytest

import prosper.warehouse._version as version

HERE = path.abspath(path.dirname(__file__))
ROOT = path.dirname(HERE)

def test_importlib_func():
    """validate importlib (setup.py version lookup)"""
    module_str = 'prosper.warehouse._version'
    package = importlib.import_module(module_str)

    version_str = package.__version__

    assert version_str == version.__version__

def test_numeric_version():
    """validate numeric versioning - matches expected value"""
    ver_elements = version.__version__.split('.')

    assert len(ver_elements) == 3

    if '-' in ver_elements[2]:
        tmp_split = ver_elements[2].split('-')
        ver_elements[2] = tmp_split[0]
        ver_elements.append(tmp_split[1])
    else:
        ver_elements.append('0')

    ver_num_str = '%04d%04d%04d.%04d' % (
        int(ver_elements[0]),
        int(ver_elements[1]),
        int(ver_elements[2]),
        int(ver_elements[3])
    )

    assert float(ver_num_str) == version.semantic_to_numeric(version.__version__)
    assert float(ver_num_str) == version.__version_int__

def test_numeric_version_force_prerelease():
    """validate numeric versioning - with prerelease"""
    assert version.semantic_to_numeric('1.2.3-4') == 100020003.0005

def test_numeric_version_force_nopre():
    """validate numeric versioning - without prerelease"""
    assert version.semantic_to_numeric('1.2.3') == 100020003.0

def test_not_installed():
    """make sure not-installed behavior is taken care of"""
    version.INSTALLED = False
    numeric_val = version.semantic_to_numeric('1.2.3-4')

    assert numeric_val == -1.0
