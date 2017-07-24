"""connections.py framework for using Prosper data warehouses"""
from os import path
import warnings

import pymongo
import semantic_version
import pandas as pd

import prosper.common.prosper_logging as p_logging
import prosper.common.prosper_config as p_config

import prosper.warehouse.exceptions as exceptions
from prosper.warehouse.monkeypatch import *

HERE = path.abspath(path.dirname(__file__))

DEFAULT_CONFIG = p_config.ProsperConfig(path.join(HERE, 'warehouse.cfg'))

EXPECTED_OPTIONS = [
    'mongo_host',
    'mongo_port',
    'mongo_user',
    'mongo_paswd',
    'mongo_db'
]
CONNECTION_STR = 'mongodb://{username}:{{password}}@{hostname}:{port}/{database}'

def build_connection_str(config_obj, database):
    """parse out the connection string

    Args:
        config_obj (:obj:`p_config.ProsperConfig`): config object with options
        database (str): which db to connect to
    Returns:
        (str) connection str (without password)

    """
    conn_str = CONNECTION_STR
    if config_obj.get('WAREHOUSE', 'mongo_str'):
        conn_str = config_obj.get('WAREHOUSE', 'mongo_str')
        mongo_str = conn_str.format(
            username=config_obj.get('WAREHOUSE', 'mongo_user'),
            port=config_obj.get('WAREHOUSE', 'mongo_port'),
            database=database
        )
        return mongo_str

    mongo_str = conn_str.format(
        username=config_obj.get('WAREHOUSE', 'mongo_user'),
        hostname=config_obj.get('WAREHOUSE', 'mongo_host'),
        port=config_obj.get('WAREHOUSE', 'mongo_port'),
        database=database
    )
    return mongo_str

class ProsperWarehouse(object):
    """container for connecting to Prosper's warehouse

    Args:
        collection (str): specific table to connect to
        config_override (:obj:`prosper_config.ProsperConfig`): defaults override
        testmode_override (bool, optional): force no-connection local-only mode
        logger (:obj:`logging.logger`, optional): logging handle for debugging
    """
    def __init__(
            self,
            collection,
            config=DEFAULT_CONFIG,
            testmode=False,
            logger=p_logging.DEFAULT_LOGGER
    ):
        self.testmode = testmode
        self.logger = logger
        self.config = config
        self.collection_exists = None
        self.collection = collection

        self.mongo_address = self.validate()

        self.mongo_conn = None

    def validate(self):
        """make sure connection string is ready

        Returns:
            (str): mongo connection str

        """
        self.database = self.config.get('WAREHOUSE', 'mongo_db')
        if self.testmode:
            return 'TESTMODE'
        if not all([
                self.config.get('WAREHOUSE', 'mongo_host'),
                self.config.get('WAREHOUSE', 'mongo_port'),
                self.config.get('WAREHOUSE', 'mongo_user'),
                self.config.get('WAREHOUSE', 'mongo_paswd'),
                self.config.get('WAREHOUSE', 'mongo_db')
                #self.collection
        ]):
            self.__bad_connection_info()
            raise exceptions.MongoConnectionStringException()

        else:
            connect_str = build_connection_str(self.config, self.database)
            if self.config.get('WAREHOUSE', 'mongo_options'):
                connect_str = connect_str + self.config.get('WAREHOUSE', 'mongo_options')

            return connect_str

    def get_data(
            self,
            query,
            **kwargs
    ):
        """fetch data from mongodb and return pandas

        Notes:
            scrubs metadata from collection

        Args:
            query (:obj:`dict`): mongo query to execute
            limit (int, optional): reduce rows returned
            skip (int, optional): pagination
            projection (:obj:`dict`, optional): filter specific keys
            distinct (str, optional):
            sort (:obj:`tuple`, optional): (sortkey, pymongo.direction)

        Returns:
            (:obj:`pandas.DataFrame`)

        """
        pass

    def __str__(self):
        """return mongo connection str"""
        return self.mongo_address

    def __bad_connection_info(self):
        """print more info for bad connection string

        Returns:
            (:obj:`list) names of blank keys
        """
        ## TODO remove?
        missing_keys = []
        for expected_key in EXPECTED_OPTIONS:
            if not self.config.get('WAREHOUSE', expected_key):
                missing_keys.append(expected_key)

        warnings.warn(
            'Unable to connect to mongo, missing keys: {}'.format(missing_keys),
            exceptions.MongoMissingKeysWarning
        )
        return missing_keys

    def __bool__(self):
        """is class ready to query?"""
        if self.testmode:
            return True

        if not self.mongo_address:
            #TODO: better test for queryability?
            self.__bad_connection_info()
            return False

        return True

    def __which_connector(self):
        """selects mongo/tinymongo connector for connecting

        Returns:
            (:obj:`pymongo.MongoClient` OR :obj:`tinymongo.TinyMongoClient`)

        """
        if self.testmode:
            warnings.warn(
                'USING LOCAL DB TINYMONGO',
                exceptions.TestModeWarning
            )
            self.logger.info('connecting to tinymongo %s', HERE)
            mongo_conn = ProsperTinyMongo(HERE)
        else:
            if not bool(self):
                #TODO: better test for queryability?
                self.logger.warning('Unable to connect to mongo, missing info')
                raise exceptions.MongoConnectionStringException()
            self.logger.info('connecting to mongo %s', self.mongo_address)
            mongo_conn = pymongo.MongoClient(self.mongo_address.format(
                password=self.config.get('WAREHOUSE', 'mongo_paswd')
            ))

        return mongo_conn

    def __enter__(self):
        """for `with obj()` logic -- open connection

        Returns:
            (:obj:`pymongo.MongoClient.collections`) handle for query

        """
        self.mongo_conn = self.__which_connector()

        return self.mongo_conn[self.database][self.collection]

    def __exit__(self, exception_type, exception_value, traceback):
        """for `with obj()` logic -- close connection"""
        self.mongo_conn.close()
