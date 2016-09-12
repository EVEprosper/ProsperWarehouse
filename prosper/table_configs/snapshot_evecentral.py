'''snapshot_evecentral.py: contains connection logic for snapshot_evecentral database'''

from os import path

import pandas
import mysql.connector

from prosper.common.utilities import get_config, create_logger
import prosper.warehouse.Connection as Connection
import prosper.warehouse.Utilities as table_utils

HERE = path.abspath(path.dirname(__file__))
ME = __file__.replace('.py', '')
CONFIG_ABSPATH = path.join(HERE, 'table_config.cfg')

print('SNAPSHOT: GET_CONFIG')
config = get_config(CONFIG_ABSPATH)
CONNECTION_VALUES = table_utils.get_config_values(config, ME)

DEBUG = False
class snapshot_evecentral(Connection.SQLTable):
    '''worker class for handling eve_central data'''
    def _define_table_type(self):
        '''set TableType enum'''
        return Connection.TableType.MySQL

    def get_keys(self):
        '''get primary/data keys from config file'''
        tmp_primary_keys = []
        tmp_data_keys = []
        print('--SNAPSHOT: get_keys()')
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
    def _set_info(self):
        '''save info about table/datasource'''
        #TODO move up?
        return CONNECTION_VALUES['table'], CONNECTION_VALUES['schema']

    def get_connection(self):
        '''get con/cur for db connections'''
        print('--SNAPSHOT: get_connection()')
        tmp_connection = mysql.connector.connect(
            user    =CONNECTION_VALUES['user'],
            password=CONNECTION_VALUES['passwd'],
            database=CONNECTION_VALUES['schema'],
            host    =CONNECTION_VALUES['host'],
            port    =CONNECTION_VALUES['port']
        )
        tmp_cursor = tmp_connection.cursor()

        return tmp_connection, tmp_cursor

    def test_table(self):
        '''test table connection/contents'''
        if DEBUG: print('--SNAPSHOT: test_table()')
        ## Check if table exists ##
        if DEBUG: print('----table_exists: start')
        exists_query = \
        '''SHOW TABLES LIKE \'{table_name}\''''.\
            format(
                table_name=CONNECTION_VALUES['table']
            )
        try:
            exists_result = self._direct_query(exists_query)
        except Exception as error_msg:
            #TODO logger
            raise error_msg

        if len(exists_result) != 1:
            #TODO: move to mysql_create_table(schema_name, table_name, table_path, con, cur)
            if DEBUG:
                print(
                    'TABLE {schema_name}.{table_name} NOT FOUND, creating table'.\
                    format(
                        schema_name=CONNECTION_VALUES['schema'],
                        table_name =CONNECTION_VALUES['table']
                    ))
            self._create_table(
                config.get(ME, 'table_create_file'),
                HERE
            )
            if DEBUG:
                print(
                    'TABLE {schema_name')
        else:
            if DEBUG:
                print(
                    'TABLE {schema_name}.{table_name} CREATED'.\
                    format(
                        schema_name=CONNECTION_VALUES['schema'],
                        table_name =CONNECTION_VALUES['table']
                    ))
        ## Check if headers config is correct ##
        if DEBUG: print('----table_headers: start')
        all_keys = []
        all_keys.append(self.index_key)
        all_keys.append(self.primary_keys)
        all_keys.append(self.data_keys)

        #TODO: move to mysql_get_headers(schema_name, table_name, con, cur)
        header_query = \
        '''SELECT `COLUMN_NAME`
            FROM `INFORMATION_SCHEMA`.`COLUMNS`
            WHERE `TABLE_SCHEMA`=\'{schema_name}\'
            AND `TABLE_NAME`=\'{table_name}\''''.\
            format(
                schema_name=CONNECTION_VALUES['schema'],
                table_name =CONNECTION_VALUES['table']
            )
        self.cursor.execute(header_query)
        headers = self.cursor.fetchall()
        if DEBUG:
            print(headers)
        if not table_utils.bool_test_headers(
                headers,
                all_keys,
                debug=DEBUG,
                #logger=Logger #TODO
        ):
            error_msg = 'Table headers not equivalent'
            print(error_msg)
            return False
    #TODO: maybe too complicated

    def get_data(self, *args, **kwargs):
        pass

    def put_data(self, payload):
        pass

## MAIN = TEST ##
if __name__ == '__main__':
    print(ME)
    DEBUG = True
    CONNECTION_VALUES = table_utils.get_config_values(config, ME, DEBUG)
    print(CONNECTION_VALUES)
    TEST_OBJECT = snapshot_evecentral(
        CONNECTION_VALUES['table']
    )
    TEST_QUERY = TEST_OBJECT._direct_query('SELECT * FROM snapshot_evecentral')

