'''snapshot_evecentral.py: contains connection logic for snapshot_evecentral database'''

from os import path
from datetime import datetime, timedelta

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
class crest_markethistory(Connection.SQLTable):
    '''worker class for handling eve_central data'''
    _latest_entry=None

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

        try:
            self.test_table_headers(
                CONNECTION_VALUES['table'],
                CONNECTION_VALUES['schema'],
                self.all_keys,
                DEBUG
            )
        except Exception as error_msg:
            #TODO: logger
            raise error_msg
        if DEBUG: print('----test_table_headers: PASS')

        self._latest_entry = self.latest_entry()

    #TODO: maybe too complicated


    def latest_entry(self, **kwargs):
        '''check source for latest entry (given kwargs)'''
        pd_dataframe = self.get_data(
            '1970-01-01',
            kwargs_passthrough=kwargs,
            limit=1,
            )
        if DEBUG: print('----latest_entry=')
        if pd_dataframe.empty:
            return None
        else:
            if DEBUG: print(pd_dataframe[self.index_key][0])
            return pd_dataframe[self.index_key][0]

    def put_data(self, payload):
        '''tests and pushes data to datastore'''
        if not isinstance(payload, pandas.DataFrame):
            raise NotImplementedError('put_data() requires Pandas.DataFrame.  No conversion implemented')


        if not payload.index.name:
            # change pandas.index to db's index_key
            payload.set_index(
                keys=self.index_key,
                drop=True,
                inplace=True
            )
        if DEBUG: print('----Testing dataframe against existing table')
        if DEBUG: print(self._latest_entry)
        if self._latest_entry:
            # avoid overwrites
            datemin = self._latest_entry + timedelta(days=1)
            datemax = payload.index.values.max()
            print('datemin: ' + str(datemin))
            print('datemax: ' + str(datemax))
            if payload.index.values.max() == self._latest_entry:
                print('WARNING: database already up-to-date, SKIPPING WRITE')
                return
            else:
                if DEBUG: print('----Adjusting dataframe')
                payload = payload.ix[
                    datemin:\
                    datemax
                    ]

        if DEBUG: print(payload)
        test_result = table_utils.bool_test_headers(
            list(payload.columns.values),
            self.all_keys,
            None,
            DEBUG
        )

        #FIXME vvv return types are weird without ConnectionExceptions being passed down
        if isinstance(test_result, str):
            raise Connection.MismatchedHeaders(test_result, self.table_name)

        try:
            payload.to_sql(
                name=self.table_name,
                con=self._connection,
                schema=self.schema_name,
                flavor='mysql',
                if_exists='append'
            )
        except Exception as error_msg:
            raise Connection.UnableToWriteToDatastore(
                error_msg,
                self.table_name
            )


def build_sample_dataframe(days):
    '''load a sample dataframe for testing'''
    #TODO make generic?
    #from datetime import datetime, timedelta
    from numpy import random


    datetime_today = datetime.today()
    datetime_target= datetime_today - timedelta(days=(days+1))
    datetime_range = pandas.date_range(
        start=datetime_target,
        end=datetime_today,
        freq='1D',
        format='%Y-%m-%d'
    )
    #datetime_range = datetime_range.date
    #print(datetime_range)
    sizeof_list = len(datetime_range)
    typeids = [34] * sizeof_list
    regionids = [99999999] * sizeof_list

    orders = random.randint(
        low=1000,
        high=100000,
        size=sizeof_list
        )
    volumes = random.randint(
        low=1e6,
        high=1e9,
        size=sizeof_list

        )
    pricelows = random.randint(
        low=600,
        high=800,
        size=sizeof_list
        ) / 100
    pricehighs = random.randint(
        low=800,
        high=1000,
        size=sizeof_list
        ) / 100
    priceavgs = random.randint(
        low=700,
        high=900,
        size=sizeof_list
        ) / 100

    dataframe = pandas.DataFrame({
        'price_date': datetime_range.date,
        'typeid': typeids,
        'regionid': regionids,
        'orderCount': orders,
        'volume': volumes,
        'lowPrice': pricelows,
        'highPrice': pricehighs,
        'avgPrice': priceavgs
        })

    dataframe.set_index(
        keys='price_date',
        drop=True,
        inplace=True
    )
    print(dataframe)
    return dataframe

## MAIN = TEST ##
if __name__ == '__main__':
    print(ME)
    DEBUG = True
    CONNECTION_VALUES = table_utils.get_config_values(config, ME, DEBUG)
    SAMPLE_DATA_FRAME = build_sample_dataframe(10)
    TEST_OBJECT = crest_markethistory(
        CONNECTION_VALUES['table'],
        debug=DEBUG
    )
    TEST_OBJECT.put_data(SAMPLE_DATA_FRAME)
    TEST_OBJECT.latest_entry(
        regionid=99999999,
        typeid=34
        )
    exit()
    TEST_DATA = TEST_OBJECT.get_data(
        10,
        "avgPrice",
        "volume",
        regionid=99999999,#30000142,
        typeid=34,
    )
    print(TEST_DATA)

    #TODO compare TEST_DATA and SAMPLE_DATA_FRAME
