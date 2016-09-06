'''snapshot_evecentral.py: contains connection logic for snapshot_evecentral database'''

from os import path

import pandas

from prosper.common.utilities import get_config, create_logger
import prosper.warehouse.Connection as Connection

HERE = path.abspath(path.dirname(__file__))
CONFIG_ABSPATH = path.join(HERE, 'table_config.cfg')

config = get_config(CONFIG_ABSPATH)

class snapshot_evecentral(Connection.Timeseries):
    '''worker class for handling eve_central data'''
    pass
    #TODO: maybe too complicated

if __name__ == '__main__':
    print(__name__)
