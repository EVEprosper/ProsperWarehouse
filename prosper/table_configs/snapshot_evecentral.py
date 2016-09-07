'''snapshot_evecentral.py: contains connection logic for snapshot_evecentral database'''

from os import path

import pandas
import mysql.connector

from prosper.common.utilities import get_config, create_logger
import prosper.warehouse.Connection as Connection

HERE = path.abspath(path.dirname(__file__))
ME = __file__.replace('.py', '')
CONFIG_ABSPATH = path.join(HERE, 'table_config.cfg')

config = get_config(CONFIG_ABSPATH)
CONNECTION_VALUES = Connection.get_config_values(config, ME)

DEBUG = False
class snapshot_evecentral(Connection.SQLTable):
    '''worker class for handling eve_central data'''
    pass
    #TODO: maybe too complicated

if __name__ == '__main__':
    print(ME)
    DEBUG = True
    TEST_OBJECT = snapshot_evecentral(
        CONNECTION_VALUES['table_name']
    )
    TEST_OBJECT.test_table()
