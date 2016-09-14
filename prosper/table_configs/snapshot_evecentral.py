'''snapshot_evecentral.py: contains connection logic for snapshot_evecentral database'''

from os import path

import pandas
from plumbum import local
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
    def set_local_path(self):
        return HERE

    def _define_table_type(self):
        '''set TableType enum'''
        return Connection.TableType.MySQL

    def get_table_create_string(self):
        '''get/parse table-create file'''
        full_table_filepath = None
        table_create_path = config.get(ME, 'table_create_file')
        if '..' in table_create_path:
            local.cwd.chdir(HERE)
            full_table_filepath = local.path(table_create_path)
        else:
            full_table_filepath = local.path(table_create_path)

        #TODO: test `exists`
        full_create_string = ''
        with open(full_table_filepath, 'r') as file_handle:
            full_create_string = file_handle.read()

        return full_create_string

    def get_keys(self):
        '''get primary/data keys from config file'''
        tmp_primary_keys = []
        tmp_data_keys = []
        print('--SNAPSHOT: get_keys()')
        try:
            tmp_primary_keys = config.get(ME, 'primary_keys').split(',')
            tmp_data_keys = config.get(ME, 'data_keys').split(',')
            self.index_key = config.get(ME, 'index_key') #FIXME: this is bad
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
        try:
            self.test_table_exists(
                CONNECTION_VALUES['table'],
                CONNECTION_VALUES['schema'],
                DEBUG
            )
        except Exception as error_msg:
            #TODO logger
            raise error_msg
        if DEBUG: print('----test_table_exists: PASS')

        ## Check if headers config is correct ##
        if DEBUG: print('----table_headers: start')
        all_keys = []
        all_keys.append(self.index_key)
        all_keys.extend(self.primary_keys)
        all_keys.extend(self.data_keys)

        try:
            self.test_table_headers(
                CONNECTION_VALUES['table'],
                CONNECTION_VALUES['schema'],
                all_keys,
                DEBUG
            )
        except Exception as error_msg:
            #TODO: logger
            raise error_msg
        if DEBUG: print('----test_table_headers: PASS')

    #TODO: maybe too complicated

    def get_data(
            self,
            datetime_start,
            *args,
            datetime_end=None,
            limit=None,
            **kwargs
    ):
        '''process queries to fetch data'''
        #**kwargs: filter query keys
        #*args: data keys to return
        if isinstance(datetime_start, int):
            #assume "last x days"
            datetime_start = table_utils.convert_days_to_datetime(datetime_start)

        ## Test argument contents before executing ##
        try:
            table_utils.test_kwargs_headers(self.primary_keys, kwargs)
        except Exception as error_msg:
            raise Connection.InvalidQueryKeys(
                error_msg,
                self.table_name
                )
        try:
            table_utils.test_args_headers(self.data_keys, args)
        except Exception as error_msg:
            raise Connection.InvalidDataKeys(
                error_msg,
                self.table_name
                )
        if isinstance(limit, int):
            limit = abs(limit)
        elif limit is not None: #<--FIXME: logic is kinda shitty
            raise Connection.BadQueryModifier(
                'limit badType: ' + str(type(limit)),
                self.table_name
                )
        #TODO: test datetimes

        ## Let's Build A Query! ##
        query_header_string = ','.join(args) if args else ','.join(self.data_keys)
        max_date_filter = ''
        if datetime_end:
            max_date_filter = 'AND {index_key} < \'{datetime_string}\''.\
                format(
                    index_key=self.index_key,
                    datetime_string=str(datetime_end)
                )
        limit_filter = ''
        if limit:
            limit_filter = 'AND LIMIT {limit}'.format(limit=limit)

        query_general_filter = \
            '''{index_key} > \'{datetime_string}\'
            {max_date_filter}'''.\
            format(
                index_key=self.index_key,
                datetime_string=str(datetime_start),
                max_date_filter=max_date_filter
                )
        query_specific_filter = table_utils.format_kwargs(kwargs)
        query_string = '''
            SELECT {index_key},{query_header_string}
            FROM {table_name}
            WHERE {query_general_filter}
            {query_specific_filter}
            ORDER BY {index_key} DESC
            {limit_filter}'''.\
            format(
                query_header_string=query_header_string,
                table_name=self.table_name,
                query_general_filter=query_general_filter,
                query_specific_filter=query_specific_filter,
                index_key=self.index_key,
                limit_filter=limit_filter
            )
        if DEBUG: print(query_string)
        #exit()
        pandas_dataframe = pandas.read_sql(
            query_string,
            self._connection
            )
        return pandas_dataframe

    def put_data(self, payload):
        pass

## MAIN = TEST ##
if __name__ == '__main__':
    print(ME)
    DEBUG = True
    CONNECTION_VALUES = table_utils.get_config_values(config, ME, DEBUG)
    print(CONNECTION_VALUES)
    TEST_OBJECT = snapshot_evecentral(
        CONNECTION_VALUES['table'],
    )
    TEST_DATA = TEST_OBJECT.get_data(
        10,
        "sell_min",
        "sell_volume",
        locationid=99999999,#30000142,
        typeid=34,
    )
    print(TEST_DATA)
    #TEST_QUERY = TEST_OBJECT._direct_query('SELECT * FROM snapshot_evecentral')
