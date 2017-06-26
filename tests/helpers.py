"""helpers.py: test utilities for common functionality"""
from os import path
from datetime import datetime

import prosper.common.prosper_config as p_config

HERE = path.abspath(path.dirname(__file__))
ROOT = path.dirname(HERE)

TEST_RECORD = {
    "test_string": "butts",
    "test_int": 6,
    "test_float": 8.008,
    "test_list":["butts", "morebutts", "evenmorebutts"],
    "test_dict":{
        "test_string":"dict_butt",
        "test_int": 99,
        "test_float": 123.456
    },
    "datetime": datetime(2017, 1, 30, 16, 0, 0, 0),
    "test_null": None,
    "test_version": "1.0.0"
}

def load_config(path_to_config):
    """load config object for testing

    Args:
        path_to_config (str): path to config file

    Returns:
        (:obj:`prosper_config.ProsperConfig`): config handle

    """
    return p_config.ProsperConfig(path_to_config)
