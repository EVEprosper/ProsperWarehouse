"""SchemaManager.py: helper script for syncronizing library schemas with SoT"""

from datetime import datetime
from os import path
import json

import semantic_version
from plumbum import cli
from jsonschema import validate

import prosper.common.prosper_logging as p_logging
import prosper.common.prosper_config as p_config
import prosper.warehouse.connection as p_connection

HERE = path.abspath(path.dirname(__file__))
ROOT = path.dirname(HERE)

CONFIG_PATH = path.join(ROOT, 'prosper', 'warehouse', 'warehouse.cfg')
CONFIG = p_config.ProsperConfig(CONFIG_PATH)

PROD_SCHEMA_PATH = path.join(ROOT, 'prosper', 'warehouse', 'schemas')

def get_schema_from_mongo(
        connector,
        schema_name,
        logger=p_logging.DEFAULT_LOGGER
):
    """"pull schema from mongodb

    Args:
        connector (:obj:`p_connection.ProsperWarehouse`): database connector
        schema_name (str): name to look up
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        (:obj:`dict`): latest schema
    """
    pass

class ManagerScript(cli.Application):
    """application for managing schemas in mongoDB"""
    __log_builder = p_logging.ProsperLogger(
        'ReleaseScript',
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

    def main(self):
        """core logic goes here"""
        if not self.debug:
            self.__log_builder.configure_discord_logger()

        logger = self.__log_builder.logger
        logger.info('HELLO WORLD -- DEFAULT')

@ManagerScript.subcommand('pull')
class PullSchemas(cli.Application):
    """pulls current schemas down for packaging"""

    def main(self):
        """core logic goes here"""
        if not self.debug:
            self.__log_builder.configure_discord_logger()

        logger = self.__log_builder.logger
        logger.info('HELLO WORLD -- PULL')

@ManagerScript.subcommand('push')
class PushSchemas(cli.Application):
    """pushes one schema up to mongoDB"""

    source = {}
    @cli.switch(
        ['-s', '--source'],
        str,
        help='schema file to upload'
    )
    def override_source(self, source_path):
        """schema to upload

        Note:
            will try ../prosper/warehouse/schemas first
        Args:
            source_path (str): path to schema

        Returns:
            (:obj:`dict`) parsed JSON file

        """
        prod_fullpath = path.join(PROD_SCHEMA_PATH, source_path)
        if not path.isfile(prod_fullpath):
            if not path.isfile(source_path):
                raise FileNotFoundError
            else:
                prod_fullpath = source_path
                ## load from direct path

        with open(prod_fullpath, 'r') as data_fh:
            data = json.load(data_fh)

        self.source = data

    major = cli.Flag(
        ['-M', '--major'],
        help='Increment version -- Major'
    )

    minor = cli.Flag(
        ['-m', '--minor'],
        help='Increment version -- Minor'
    )

    no_release = cli.Flag(
        ['--no-release'],
        help='Debug release -- No prod version'
    )
    def main(self):
        """core logic goes here"""
        if not self.debug:
            self.__log_builder.configure_discord_logger()

        logger = self.__log_builder.logger
        logger.info('HELLO WORLD -- PUSH')

        logger.info('building mongo connector')
        connector = p_connection.ProsperWarehouse(
            CONFIG.get('WAREHOUSE', 'master_schema'),
            config=CONFIG,
            testmode=self.debug,
            logger=logger
        )



if __name__ == '__main__':
    ManagerScript.run()
