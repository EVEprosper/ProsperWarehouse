"""helpers.py: test utilities for common functionality"""
from os import path, remove
from datetime import datetime

import tinymongo

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
    ## vv FIXME vv : Datetime vs tinymongo Serialization is FUBAR
    # https://github.com/msiemens/tinydb-serialization
    "datetime": datetime(2017, 1, 31, 0, 0, 0, 0),
    ## ^^ FIXME ^^ ##
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

def init_tinymongo(
        data_to_write,
        collection_name,
        database_name='prosper',
        path_to_tinymongo=path.join(ROOT, 'prosper', 'warehouse')
):
    """pushes debug data into tinymongo

    Args:
        data_to_write (:obj:`dict`): tinymonog data
        collection_name (str): collection to write data to
        database_name (str, optional): base database name
        path_to_tinymongo (str, optional): filepath to mongo instance

    Returns:
        (:obj:`dict`): expected data if reformatted
    """
    if path.isfile(path_to_tinymongo):
        remove(path_to_tinymongo)
    client = tinymongo.TinyMongoClient(path_to_tinymongo)

    doc_id = client[database_name][collection_name].insert_one(data_to_write)

    data_to_write.pop('_id')

    return data_to_write
