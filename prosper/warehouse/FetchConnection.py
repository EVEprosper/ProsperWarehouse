'''FetchConnection.py: importlib magic for importing connections dynamically by-string'''

from os import path
import importlib.util

HERE = path.abspath(path.dirname(__file__))
DEFAULT_TABLECONFIG_PATH = path.join(path.dirname(HERE),'table_configs')
if __name__ == '__main__':
    pass
