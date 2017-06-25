"""connections.py framework for using Prosper data warehouses"""
from os import path
import warnings

import pymongo

import prosper.common.prosper_logging as p_logging
import prosper.common.prosper_config as p_config

import prosper.warehouse.exceptions as exceptions
HERE = path.abspath(path.dirname(__file__))
LOGGER = p_logging.DEFAULT_LOGGER

DEFAULT_CONFIG = p_config.ProsperConfig(path.join(HERE, 'warehouse.cfg'))

EXPECTED_OPTIONS = [
    'mongo_host',
    'mongo_port',
    'mongo_user',
    'mongo_passwd',
    'mongo_db'
]
CONNECTION_STR = 'mongodb://{username}:{{password}}@{hostname}:{port}/{database}'
def get_connection(
        config,
        testmode=False,
        logger=LOGGER
):
    """get a mongo connection object

    Args:
        config (:obj:`prosper_config.ProsperConfig`): connection args in config
        testmode (bool, optional): switch to testmode/local/tinymongo connectors
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        (:obj:`pymongo.MongoClient` OR :obj:`tinymongo.TinyMongoClient`)

    """
    logger.info('generating mongo connection')

class ProsperWarehouse(object):
    """container for connecting to Prosper's warehouse

    Args:
        config_override (:obj:`prosper_config.ProsperConfig`): defaults override
        testmode_override (bool, optional): force no-connection local-only mode
        logger (:obj:`logging.logger`, optional): logging handle for debugging
    """
    def __init__(
            self,
            collection_name,
            config_override=DEFAULT_CONFIG,
            testmode_override=False,
            logger=LOGGER
    ):
        self.testmode = testmode_override
        self.logger = logger
        self.config = config_override
        self.collection = collection_name
        self.collection_exists = None

        self.mongo_address = CONNECTION_STR.format(
            username=self.config.get('mongo_user'),
            hostname=self.config.get('mongo_host'),
            port=self.config.get('mongo_port'),
            database=self.config.get('mongo_db')
        )

        self.mongo_conn = None

    def __check_collection_exists(self):
        """validate collection in database"""
        raise NotImplementedError('Collection $exists not set up')

    def __bad_connection_info(self):
        """print more info for bad connection string

        Returns:
            (:obj:`list) names of blank keys
        """
        missing_keys = []
        for expected_key in EXPECTED_OPTIONS:
            if not self.config.get('WAREHOUSE', expected_key):
                missing_keys.append(expected_key)

        warnings.warn(
            'Unable to connect to mongo, missing keys: {}'.format(missing_keys),
            exceptions.MongoMissingKeysWarning()
        )
        #self.logger.info('missing keys: {}'.format(missing_keys))
        return missing_keys

    def __bool__(self):
        """is class ready to query?"""
        status = all([
            self.config.get('WAREHOUSE', 'mongo_host'),
            self.config.get('WAREHOUSE', 'mongo_port'),
            self.config.get('WAREHOUSE', 'mongo_user'),
            self.config.get('WAREHOUSE', 'mongo_passwd'),
            self.config.get('WAREHOUSE', 'mongo_db'),
            self.collection
        ])

        if not status:
            self.__bad_connection_info()

        return status

    def __enter__(self):
        """for `with obj()` logic -- open connection

        Returns:
            (:obj:`pymongo.MongoClient.collections`) handle for query

        """
        if not bool(self):
            self.logger.warning('Unable to connect to mongo')
            raise exceptions.MongoConnectionStringException()

        self.logger.info('connecting to: %s', self.mongo_address)
        self.mongo_conn = pymongo.MongoClient(self.mongo_address.format(
            password=self.config.get('WAREHOUSE', 'mongo_passwd')))

        return self.mongo_conn[self.collection]

    def __exit__(self, exception_type, exception_value, traceback):
        """for `with obj()` logic -- close connection"""
        self.mongo_conn.close()
