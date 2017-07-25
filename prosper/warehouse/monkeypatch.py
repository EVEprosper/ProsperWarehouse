"""monkeypatch.py: helper to patch over unsupported mongo/tinyDB issues"""
from os import path
import warnings
from datetime import datetime

import semantic_version
import tinymongo
import tinydb_serialization

import prosper.common.prosper_logging as p_logging
import prosper.warehouse.exceptions as exceptions

#__all__ = ('ProsperTinyMongo')
DEFAULT_PROJECTION = {
    '_id': False,
    'metadata': False
}

def monkey_patch_find(
        self,
        q_filter=None,
        sort=None,
        skip=None,
        limit=None,
        projection=None,
        *args,
        **kwargs
):
    """monkeypatching in support/warning for projection() support

    Args:
        q_filter (:obj:`dict`): mongo-style search query
        sort (bool, optional): sort direction
        skip (int, optional): pagination support, number of entries to skip
        limit (int, optional): limit total returned entries from query
        projection (:obj:`dict`): mongo-style `SELECT` support

    Returns:
        (:obj:`tinymongo.TinyMongoCursor`)
    """

    return_val = self.prod_find(q_filter, sort, skip, limit, *args, **kwargs)

    return return_val

########################
## Monkey Patch Magic ##
########################
prod_find = tinymongo.tinymongo.TinyMongoCollection.find
tinymongo.tinymongo.TinyMongoCollection.find = monkey_patch_find
tinymongo.tinymongo.TinyMongoCollection.prod_find = prod_find
########################
## Monkey Patch Magic ##
########################

SPECIAL_KEYS = ['_id']  #ALWAYS DROP
def test_tinydb_projection(projection):
    """mongoDB only allows all-True/all-False projections

    Args:
        projection (:obj:`dict`): projection to test

    Returns:
        (bool): go/no-go

    """
    keep_or_toss = None
    for key, value in projection.keys():
        if not isinstance(key, bool):
            raise TypeError
        if key in SPECIAL_KEYS:
            continue

        if keep_or_toss is None:
            keep_or_toss = value
            continue

        if value != keep_or_toss:
            return None

    return keep_or_toss

def tinydb_projection(
        data,
        projection,
        logger=p_logging.DEFAULT_LOGGER
):
    """tinydb does not support $projection.  Poorman's implementation

    Args:
        data (:obj:`list`) data from [tiny]mongodb
        projection (:obj:`dict`): projection to filter

    Returns:
        (:obj:`list`): scrubbed results

    """
    filter_direction = test_tinydb_projection(projection)
    if filter_direction is None:
        raise exceptions.BadProjectionException()

    filter_list = list(set(projection.keys()) - set(SPECIAL_KEYS))

    logger.info('--filtering data')
    if filter_direction:
        logger.info('----POSITIVE FILTER - Keep {}'.format(filter_list))
        data = keep_filter(filter_list, list(data))
    else:
        logger.info('----NEGATIVE FILTER - Drop {}'.format(filter_list))
        data = drop_filter(filter_list, list(data))

        data = drop_filter(SPECIAL_KEYS, list(data))

    return data


def keep_filter(filter_list, data):
    """keep what's in the filter

    TODO: Pandas > for-loop?

    Args:
        filter (:obj:`list`): keys to keep from data list
        data (:obj:`list`): data to scrub
    Returns:
        (:obj:`list`): cleaned data

    """
    clean_data = []
    for row in data:
        pop_list = list(set(filter_list) - set(row.keys))
        clean_data.append(  #use list-comprehension for filter
            [row.pop(key) for key in pop_list]
        )

    return clean_data

def drop_filter(filter_list, data):
    """drop what's in the filter

    TODO: Pandas > for-loop?

    Args:
        filter (:obj:`list`): keys to drop from the list
        data (:obj:`list`): data to scrub

    Returns:
        (:obj:`list`): cleaned data

    """
    clean_data = []
    for row in data:
        filtered_data = [row.pop(key) for key in filter_list]
        clean_data.append(filtered_data.pop(SPECIAL_KEYS))

    return clean_data

class DateTimeSerializer(tinydb_serialization.Serializer):
    """TinyDB serializer:
        https://github.com/msiemens/tinydb-serialization#creating-a-serializer
    """
    OBJ_CLASS = datetime  # The class this serializer handles

    def encode(self, obj):
        """obj -> str writing to .json file"""
        return obj.strftime('%Y-%m-%dT%H:%M:%S')

    def decode(self, s):
        """str -> obj reading from .json file"""
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')

class SemanticVersionSerializer(tinydb_serialization.Serializer):
    """TinyDB serializer for semantic versions"""
    OBJ_CLASS = semantic_version.base.Version

    def encode(self, obj):
        """obj -> str writing to .json file"""
        return str(obj)

    def decode(self, s):
        """str -> obj reading from .json file"""
        return semantic_version(s)

class ProsperTinyMongo(tinymongo.TinyMongoClient):
    """Extend serialization to better match MongoDB
        https://github.com/schapman1974/tinymongo#handling-datetime-objects
    """
    @property
    def _storage(self):
        serialization = tinydb_serialization.SerializationMiddleware()
        serialization.register_serializer(
            DateTimeSerializer(),
            'TinyDate'
        )
        serialization.register_serializer(
            SemanticVersionSerializer(),
            'TinyVersion'
        )
        return serialization
