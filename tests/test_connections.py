"""test_connections.py: validate tinymongo/mongo connectors"""

from os import path

import pytest
import helpers

import prosper.warehouse.connections as connections
import prosper.warehouse.exceptions as exceptions

HERE = path.abspath(path.dirname(__file__))
ROOT = path.dirname(HERE)

ROOT_CONFIG = helpers.load_config(
    path.join(ROOT, 'prosper', 'warehouse', 'warehouse.cfg')
)
TEST_CONFIG = helpers.load_config(
    path.join(HERE, 'test_config.cfg')
)

@pytest.mark.mongo
def test_prod_mongo_happypath():
    """test production normal path for connections"""
    test_collection = TEST_CONFIG.get('MONGO', 'test_collection')
    prod_connection = connections.ProsperWarehouse(test_collection)

    with prod_connection as mongo_handle:
        test_data = mongo_handle.find_one({})

    assert isinstance(test_data, dict)
    #TODO: assert test connection has data
