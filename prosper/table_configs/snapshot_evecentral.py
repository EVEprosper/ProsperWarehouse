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
    def get_keys(self):
        '''get primary/data keys from config file'''
        tmp_primary_keys = []
        tmp_data_keys = []

        try:
            tmp_primary_keys = ','.split(config.get(ME, 'primary_keys'))
            tmp_data_keys = ','.split(config.get(ME, 'data_keys'))
            self.index_key = config.get(ME, 'index_key') #TODO: this is bad
        except KeyError as error_msg:
            raise Connection.TableKeysMissing(
                error_msg,
                ME
            )

        return tmp_primary_keys, tmp_data_keys

    def get_connection(self):
        '''get con/cur for db connections'''
        tmp_connection = mysql.connector.connect(
            user    =CONNECTION_VALUES['user'],
            password=CONNECTION_VALUES['passwd'],
            database=CONNECTION_VALUES['schema'],
            host    =CONNECTION_VALUES['host'],
            port    =CONNECTION_VALUES['port']
        )
        tmp_cursor = tmp_connection.cursor()
        self.table_name = CONNECTION_VALUES['table'] #TODO: this is bad

        return tmp_connection, tmp_cursor

    def test_table(self):
        '''test table connection/contents'''
        #check headers
        all_keys = []
        all_keys.append(self.index_key)
        all_keys.append(self.primary_keys)
        all_keys.append(self.data_keys)

        header_query = \
        '''SELECT `COLUMN_NAME`
            FROM `INFORMATION_SCHEMA`.`COLUMNS`
            WHERE `TABLE_SCHEMA`='{schema_name}'
            AND `TABLE_NAME`='{table_name}'''.\
            format(
                schema_name=CONNECTION_VALUES['schema'],
                table_name =CONNECTION_VALUES['table']
            )
        self.cursor.execute(header_query)
        headers = self.cursor.fetchall()
        if DEBUG:
            print(headers)
    #TODO: maybe too complicated

if __name__ == '__main__':
    print(ME)
    DEBUG = True
    TEST_OBJECT = snapshot_evecentral(
        CONNECTION_VALUES['table_name']
    )
    TEST_OBJECT.test_table()
