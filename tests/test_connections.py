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
    prod_connection = connections.ProsperWarehouse(config=ROOT_CONFIG)

    assert bool(prod_connection)
    conn_str = str(prod_connection)
    assert '{password}' in conn_str

    with prod_connection as mongo_handle:
        test_data = mongo_handle[test_collection].find_one({}, projection={'_id': False})

    assert isinstance(test_data, dict)
    assert test_data == helpers.TEST_RECORD

def test_prod_mongo_badconfig():
    """test prod connection with bad config"""
    with pytest.raises(exceptions.MongoConnectionStringException):
        with pytest.warns(exceptions.MongoMissingKeysWarning):
            bad_connection = connections.ProsperWarehouse(config=TEST_CONFIG)

        #missing_keys = bad_connection.__bad_connection_info()

def test_test_mongo_happypath():
    """test tinymongo normal path for connections"""
    test_collection = TEST_CONFIG.get('MONGO', 'test_collection')
    test_connector = connections.ProsperWarehouse(config=ROOT_CONFIG, testmode=True)

    assert bool(test_connector)
    assert str(test_connector) == 'TESTMODE'

    expected_data = helpers.init_tinymongo(
        helpers.TEST_RECORD,
        test_collection
    )
    with test_connector as mongo_handle:
        test_data = mongo_handle[test_collection].find_one({})

    test_data.pop('_id')    #projection doesn't work with tinymongo
    assert test_data == expected_data
