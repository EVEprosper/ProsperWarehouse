"""schema_update.py: helper script for updating MongoDB with new schemas"""

from os import path
import json

import semantic_version
from pymongo import MongoClient
from plumbum import cli

import prosper.common.prosper_logging as p_logging
import prosper.common.prosper_config as p_config

HERE = path.abspath(path.dirname(__file__))
ROOT = path.dirname(HERE)

CONFIG_PATH = path.join(ROOT, 'prosper', 'warehouse', 'warehouse.cfg')
CONFIG = p_config.ProsperConfig(CONFIG_PATH)

LOGGER = p_logging.DEFAULT_LOGGER
class SafeDict(dict):
    """wrapper to keep passwords out of log
    http://stackoverflow.com/a/17215533/7835349
    """
    def __missing__(self, key):
        return '{' + key + '}'

def connect_db(
        host=CONFIG.get('WAREHOUSE', 'mongo_host'),
        port=int(CONFIG.get('WAREHOUSE', 'mongo_port')),
        user=CONFIG.get('WAREHOUSE', 'mongo_user'),
        passwd=CONFIG.get('WAREHOUSE', 'mongo_passwd'),
        logger=LOGGER
):
    """fetch connection object for mongodb

    Args:
        host (str): mongo hostname
        port (int): mongo port
        user (str): username for mongo connection
        passwd (str): password for mongo connection
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        (:obj:`pymongo.MongoClient)

    """

    mongo_address = 'mongodb://{user}:{passwd}@{host}:{port}/{database}'.format(
        SafeDict({
            'user':user,
            'host':host,
            'port':port,
            'database':CONFIG.get('WAREHOUSE', 'mongo_db')
        })
    )
    logger.info('connecting to: {}'.format(mongo_address))

    try:
        mongo_conn = MongoClient(mongo_address.format(passwd=passwd))
    except Exception:   #pragma: no cover
        logger.critical('UNABLE TO CONNECT TO MONGO', exc_info=True)

    return mongo_conn

class UpdateSchemas(cli.Application):
    """application for updating schemas in production mongo client"""
    __log_builder = p_logging.ProsperLogging(
        'UpdateSchemas',
        HERE,
        CONFIG
    )

    debug = cli.Flag(
        ['d', '--debug'],
        help='debug mode: do not write to live database'
    )
    @cli.switch(
        ['v', '--verbose'],
        help='Enable verbose messaging'
    )
    def enable_verbose(self):
        """toggle verbose/stdout logger"""
        self.__log_builder.configure_debug_logger()

    schema_object = {}
    @cli.switch(
        ['s', '--schema'],
        str,
        help='Path to schema to upload'
    )
    def set_path_to_schema(self, path_to_schema):
        """test and set path to schema file"""
        schema_path = path_to_schema
        if not path.isfile(path_to_schema):
            raise FileNotFoundError

        with open(schema_path, 'r') as schema_fh:
            data = json.load(schema_fh)

        self.schema_object = data

    def main(self):
        """core logic goes here"""
        if not self.debug:
            self.__log_builder.configure_discord_logger()

        global LOGGER
        LOGGER = self.__log_builder.logger

        LOGGER.info('HELLO WORLD')
        master_schema_path = path.join(
            ROOT, 'prosper', 'warehouse', CONFIG.get('WAREHOUSE', 'master_schema'))



if __name__ == '__main__':
    UpdateSchemas.run()
