"""schemas.py managment for schema validation"""

from os import path
from enum import Enum

import ujson as json
from jsonschema import validate

HERE = path.abspath(path.dirname(__file__))

class SchemaMode(Enum):
    """set which mode we're working in"""
    PROD = 'prod'
    LOCAL = 'local'
    DEBUG = 'debug'

