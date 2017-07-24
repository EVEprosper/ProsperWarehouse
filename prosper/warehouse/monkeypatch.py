"""monkeypatch.py: helper to patch over unsupported mongo/tinyDB issues"""
from os import path
import warnings

import tinymongo

import prosper.common.prosper_logging as p_logging

HERE = path.abspath(path.dirname(__file__))
LOGGER = p_logging.DEFAULT_LOGGER
