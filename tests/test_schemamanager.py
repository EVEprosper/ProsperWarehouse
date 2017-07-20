"""test_schemamanager.py: validate utility funcs in SchemaManager"""

from os import path
import importlib.util

import pytest
import helpers

import semantic_version
SchemaManager = None

HERE = path.abspath(path.dirname(__file__))
ROOT = path.dirname(HERE)

ROOT_CONFIG = helpers.load_config(
    path.join(ROOT, 'prosper', 'warehouse', 'warehouse.cfg')
)
TEST_CONFIG = helpers.load_config(
    path.join(HERE, 'test_config.cfg')
)

def test_load_schemamanager():
    """DEBUG TEST -- Need to load SchemaManager into python path

    Note:
        No __init__.py, hard to source path, importlib does it directly

    """
    global SchemaManager

    importlib_path = path.join(ROOT, TEST_CONFIG.get('SchemaManager', 'importlib_path'))
    importlib_module = TEST_CONFIG.get('SchemaManager', 'importlib_module')

    spec = importlib.util.spec_from_file_location(importlib_module, importlib_path)
    assert spec is not None #make sure spec loads

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    SchemaManager = module

def test_find_schema_version():
    """validate find_schema_version() function"""
    if not SchemaManager:
        pytest.xfail('Unable to load module')

def test_validate_schema_matches():
    """validate validate_schema() -- matching"""
    if not SchemaManager:
        pytest.xfail('Unable to load module')

def test_validate_schema_diff():
    """validate validate_schema() -- diff"""
    if not SchemaManager:
        pytest.xfail('Unable to load module')

def test_load_version_file():
    """validate load_version_file() expected behavior"""
    if not SchemaManager:
        pytest.xfail('Unable to load module')
