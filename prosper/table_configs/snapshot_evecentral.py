'''snapshot_evecentral.py: contains connection logic for snapshot_evecentral database'''

from os import path

import pandas
from plumbum import local
import mysql.connector

from prosper.common.utilities import get_config, create_logger
import prosper.warehouse.Connection as Connection
import prosper.warehouse.Utilities as table_utils

HERE = path.abspath(path.dirname(__file__))
ME = __file__.replace('.py', '') #FIXME: only valid for single-config profiles
CONFIG_ABSPATH = path.join(HERE, 'table_config.cfg')

print('SNAPSHOT: GET_CONFIG')
config = get_config(CONFIG_ABSPATH)
CONNECTION_VALUES = table_utils.get_config_values(config, ME)

DEBUG = False
class snapshot_evecentral(Connection.SQLTable):
    '''worker class for handling eve_central data'''
    #_debug_service = super()._debug_service
    def set_local_path(self):
        '''push local path up to parent'''
        return HERE

    def _define_table_type(self):
        '''set TableType enum'''
        return Connection.TableType.MySQL

    def get_table_create_string(self):
        '''get/parse table-create file'''
        self._logger.info('snapshot_evecentral.get_table_create_string()')
        full_table_filepath = None
        table_create_path = config.get(ME, 'table_create_file')
        if '..' in table_create_path:
            local.cwd.chdir(HERE)
            full_table_filepath = local.path(table_create_path)
        else:
            full_table_filepath = local.path(table_create_path)
        self._logger.debug('-- full_table_filepath: {0}'.format(full_table_filepath))

        full_create_string = ''
        with open(full_table_filepath, 'r') as file_handle:
            full_create_string = file_handle.read()

        self._logger.debug('-- full_create_string: {0}'.format(full_create_string))
        return full_create_string

    def get_keys(self):
        '''get primary/data keys from config file'''
        self._logger.info('snapshot_evecentral.get_keys()')

        tmp_primary_keys = []
        tmp_data_keys = []
        try:
            tmp_primary_keys = config.get(ME, 'primary_keys').split(',')
            tmp_data_keys = config.get(ME, 'data_keys').split(',')
            self.index_key = config.get(ME, 'index_key') #FIXME: this is bad
        except KeyError as error_msg:
            self._logger.error(
                'EXCEPTION: keys missing' + \
                '\r\texception={0}'.format(str(error_msg)) + \
                '\r\tprimary_keys={0}'.format(','.join(tmp_primary_keys)) + \
                '\r\tdata_keys={0}'.format(','.join(tmp_data_keys)) + \
                '\r\tindex_key={0}'.format(self.index_key)
            )
            raise Connection.TableKeysMissing(error_msg, ME)

        self._logger.debug(
            '\r\tprimary_keys={0}'.format(','.join(tmp_primary_keys)) + \
            '\r\tdata_keys={0}'.format(','.join(tmp_data_keys)) + \
            '\r\tindex_key={0}'.format(self.index_key)
        )

        return tmp_primary_keys, tmp_data_keys

    def _set_info(self):
        '''save info about table/datasource'''
        #TODO move up?
        self._logger.info('snapshot_evecentral._set_info()')
        return CONNECTION_VALUES['table'], CONNECTION_VALUES['schema']

    def get_connection(self):
        '''get con/cur for db connections'''
        self._logger.info('snapshot_evecentral.get_connection()')
        #self._logger.debug(str(CONNECTION_VALUES))
        #FIXME vvv try/exception
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
        self._logger.info('snapshot_evecentral.test_table()')
        ## Check if table exists ##
        self._logger.info('-- table exists test: START')
        try:
            self.test_table_exists(
                CONNECTION_VALUES['table'],
                CONNECTION_VALUES['schema']
            )
        except Exception as error_msg:
            self._logger.error(
                'EXCEPTION: Table does not exist, unable to fix' + \
                '\r\texception={0}'.format(str(error_msg)) + \
                '\r\ttable={0}.{1}'.format(CONNECTION_VALUES['schema'], CONNECTION_VALUES['table'])
            )
            raise error_msg

        self._logger.info('-- table exists test: PASS')

        ## Check if headers config is correct ##
        self._logger.info('-- table headers test: START')
        try:
            self.test_table_headers(
                CONNECTION_VALUES['table'],
                CONNECTION_VALUES['schema'],
                self.all_keys,
            )
        except Exception as error_msg:
            self._logger.error(
                'EXCEPTION: table headers missmatch' + \
                '\r\texception={0}'.format(str(error_msg)) + \
                '\r\ttable={0}.{1}'.format(CONNECTION_VALUES['schema'], CONNECTION_VALUES['table'])
            )
            raise error_msg

        self._logger.info('-- table headers test: PASS')

    def latest_entry(self, **kwargs):
        '''not implemented'''
        raise NotImplementedError(
            'latest_entry() not implemented because collisions should not be issue')

def build_sample_dataframe(days, frequency):
    '''load a sample dataframe for testing'''
    #TODO make generic?
    from datetime import datetime, timedelta
    from numpy import random

    datetime_today = datetime.today()
    datetime_target= datetime_today - timedelta(days=(days+1))
    datetime_range = pandas.date_range(
        start=datetime_target,
        end=datetime_today,
        freq='{0}H'.format(int(24/frequency))
    )

    sizeof_list = len(datetime_range)
    typeids = [34] * sizeof_list
    locationids = [99999999] * sizeof_list
    locationtypes=['test'] * sizeof_list
    buymaxs = random.randint(
        low=400,
        high=600,
        size=sizeof_list
        ) / 100
    buyavgs = random.randint(
        low=400,
        high=600,
        size=sizeof_list
        ) / 100
    buyvols = random.randint(
        low=1000000,
        high=1000000000,
        size=sizeof_list)
    sellmins = random.randint(
        low=600,
        high=800,
        size=sizeof_list
        ) / 100
    sellavgs = random.randint(
        low=600,
        high=800,
        size=sizeof_list
        ) / 100
    sellvols = random.randint(
        low=1000000,
        high=1000000000,
        size=sizeof_list)
    dataframe = pandas.DataFrame({
        'price_datetime':datetime_range,
        'typeid':typeids,
        'locationid':locationids,
        'location_type':locationtypes,
        'buy_max':buymaxs,
        'buy_avg':buyavgs,
        'buy_volume':buyvols,
        'sell_min':sellmins,
        'sell_avg':sellavgs,
        'sell_volume':sellvols
        })
    typeids = [40] * sizeof_list
    buymaxs = random.randint(
        low=700,
        high=900,
        size=sizeof_list
        ) / 100
    buyavgs = random.randint(
        low=700,
        high=900,
        size=sizeof_list
        ) / 100
    buyvols = random.randint(
        low=500000,
        high=500000000,
        size=sizeof_list)
    sellmins = random.randint(
        low=800,
        high=1000,
        size=sizeof_list
        ) / 100
    sellavgs = random.randint(
        low=800,
        high=1000,
        size=sizeof_list
        ) / 100
    sellvols = random.randint(
        low=500000,
        high=500000000,
        size=sizeof_list)
    dataframe2 = pandas.DataFrame({
        'price_datetime':datetime_range,
        'typeid':typeids,
        'locationid':locationids,
        'location_type':locationtypes,
        'buy_max':buymaxs,
        'buy_avg':buyavgs,
        'buy_volume':buyvols,
        'sell_min':sellmins,
        'sell_avg':sellavgs,
        'sell_volume':sellvols
        })
    dataframe = dataframe.append(dataframe2)
    dataframe.set_index(
        keys='price_datetime',
        #keys=('price_datetime','typeid','locationid'),
        drop=True,
        inplace=True
    )

    if DEBUG: dataframe.to_csv('pandas_snapshot_evecentral.csv')
    return dataframe

## MAIN = TEST ##
if __name__ == '__main__':
    print(ME)
    DEBUG = True
    DEBUG_LOGGER = create_logger(
        'debug_snapshot_evecentral',
        HERE,
        None,
        'DEBUG'
        )
    DEBUG_LOGGER.log(10, '**STARTING TEST RUN**')
    #CONNECTION_VALUES = table_utils.get_config_values(config, ME)
    #print(CONNECTION_VALUES)
    SAMPLE_DATA_FRAME = build_sample_dataframe(2, 12)
    TEST_OBJECT = snapshot_evecentral(
        CONNECTION_VALUES['table'],
        debug=DEBUG,
        loging_handle=DEBUG_LOGGER
    )
    TEST_OBJECT.put_data(SAMPLE_DATA_FRAME)
    TEST_DATA = TEST_OBJECT.get_data(
        10,
        "sell_min",
        "sell_volume",
        locationid=99999999,#30000142,
        typeid=[34,40,99],
    )
    #print(TEST_DATA)
    #TODO compare TEST_DATA and SAMPLE_DATA_FRAME
