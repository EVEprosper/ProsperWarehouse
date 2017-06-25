"""test_connections.py: validate tinymongo/mongo connectors"""

from os import path

import pytest

import prosper.warehouse.connections as connections
import prosper.warehouse.exceptions as exceptions

HERE = path.abspath(path.dirname(__file__))
ROOT = path.dirname(HERE)
