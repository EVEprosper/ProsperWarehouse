"""helpers.py: test utilities for common functionality"""
from os import path

import prosper.common.prosper_config as p_config

HERE = path.abspath(path.dirname(__file__))
ROOT = path.dirname(HERE)

def load_config(path_to_config):
    """load config object for testing

    Args:
        path_to_config (str): path to config file

    Returns:
        (:obj:`prosper_config.ProsperConfig`): config handle

    """
    return p_config.ProsperConfig(path_to_config)
