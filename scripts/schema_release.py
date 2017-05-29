"""schema_release.py: helper script for syncronizing library schemas with SoT"""

from os import path

import semantic_version
from pymongo import MongoClient
from plumbum import cli

import prosper.common.prosper_logging as p_logging
import prosper.common.prosper_config as p_config

HERE = path.abspath(path.dirname(__file__))
ROOT = path.dirname(HERE)

CONFIG_PATH = path.join(ROOT, 'prosper', 'warehouse', 'warehouse.cfg')
CONFIG = p_config.ProsperConfig(CONFIG_PATH)

class ReleaseScript(cli.Application):
    """application for updating schemas in production mongo client"""
    __log_builder = p_logging.ProsperLogging(
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

        logger.info('HELLO WORLD')

if __name__ == '__main__':
    ReleaseScript.run()
