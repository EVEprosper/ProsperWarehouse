"""connections.py framework for using Prosper data warehouses"""
from os import path

import prosper.common.prosper_logging as p_logging
import prosper.common.prosper_config as p_config

HERE = path.abspath(path.dirname(__file__))
LOGGER = p_logging.DEFAULT_LOGGER

DEFAULT_CONFIG = p_config.ProsperConfig(path.join(HERE, 'warehouse.cfg'))

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
            config_override=DEFAULT_CONFIG,
            testmode_override=False,
            logger=LOGGER
    ):
        self.testmode = testmode_override
        self.logger = logger
        self.config = config_override
