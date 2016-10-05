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
        self._debug_service.info('crest_markethistory.get_table_create_string()')

        full_table_filepath = None
        table_create_path = config.get(ME, 'table_create_file')
        if '..' in table_create_path:
            local.cwd.chdir(HERE)
            full_table_filepath = local.path(table_create_path)
        else:
            full_table_filepath = local.path(table_create_path)
        self._debug_service.debug('-- full_table_filepath: {0}'.format(full_table_filepath))

        #TODO: test `exists`
        full_create_string = ''
        with open(full_table_filepath, 'r') as file_handle:
            full_create_string = file_handle.read()

        self._debug_service.debug('-- full_create_string: {0}'.format(full_create_string))

        return full_create_string

    def get_keys(self):
        '''get primary/data keys from config file'''
        self._debug_service.info('crest_markethistory.get_keys()')

        tmp_primary_keys = []
        tmp_data_keys = []
        try:
            tmp_primary_keys = config.get(ME, 'primary_keys').split(',')
            tmp_data_keys = config.get(ME, 'data_keys').split(',')
            self.index_key = config.get(ME, 'index_key') #FIXME: this is bad
        except KeyError as error_msg:
            self._debug_service.error(
                'EXCEPTION: Keys missing' + \
                '\r\texception={0}'.format(str(error_msg)) + \
                '\r\tprimary_keys={0}'.format(','.join(tmp_primary_keys)) + \
                '\r\tdata_keys={0}'.format(','.join(tmp_data_keys)) + \
                '\r\tindex_key={0}'.format(self.index_key)
            )
            raise Connection.TableKeysMissing(error_msg, ME)

        self._debug_service.debug(
            'keys validated:' + \
            '\r\tprimary_keys={0}'.format(','.join(tmp_primary_keys)) + \
            '\r\tdata_keys={0}'.format(','.join(tmp_data_keys)) + \
            '\r\tindex_key={0}'.format(self.index_key)
        )
        return tmp_primary_keys, tmp_data_keys

    def _set_info(self):
        '''save info about table/datasource'''
        #TODO move up?
        self._debug_service.info('crest_markethistory._set_info()')
        return CONNECTION_VALUES['table'], CONNECTION_VALUES['schema']

    def get_connection(self):
        '''get con/cur for db connections'''
        self._debug_service.info('crest_markethistory.get_connection()')
        self._debug_service.debug(str(CONNECTION_VALUES))
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
        self._debug_service.info('crest_markethistory.test_table()')
        ## Check if table exists ##
        self._debug_service.info('-- table exists test: START')
        try:
            self.test_table_exists(
                CONNECTION_VALUES['table'],
                CONNECTION_VALUES['schema']
            )
        except Exception as error_msg:
            self._debug_service.error(
                'EXCEPTION: table does not exist, unable to fix:' + \
                '\r\texception={0}'.format(str(error_msg)) + \
                '\r\ttable={0}.{1}'.format(CONNECTION_VALUES['schema'], CONNECTION_VALUES['table'])
            )
            raise error_msg

        self._debug_service.info('-- table exists test: PASS')

        ## Check if headers config is correct ##
        self._debug_service.info('-- table headers test: START')

        try:
            self.test_table_headers(
                CONNECTION_VALUES['table'],
                CONNECTION_VALUES['schema'],
                self.all_keys
            )
        except Exception as error_msg:
            self._debug_service.error(
                'EXCEPTION: table headers missmatch:' + \
                '\r\texception={0}'.format(str(error_msg)) + \
                '\r\ttable={0}.{1}'.format(CONNECTION_VALUES['schema'], CONNECTION_VALUES['table'])
            )
            raise error_msg

        self._debug_service.info('-- table headers test: PASS')

        self._latest_entry = self.latest_entry()

    #TODO: maybe too complicated


    def latest_entry(self, **kwargs):
        '''check source for latest entry (given kwargs)'''
        self._debug_service.info('crest_markethistory.latest_entry()')
        pd_dataframe = self.get_data(
            '1970-01-01',   #FIXME: epoc0 is a hacky way to handle this
            kwargs_passthrough=kwargs,
            limit=1,
            )

        if pd_dataframe.empty:
            self._debug_service.info('-- latest_entry=None, no entries found')
            return None
        else:
            latest_entry = pd_dataframe[self.index_key][0]
            self._debug_service.debug('-- latest_entry={0}'.format(str(latest_entry)))
            return latest_entry

    def put_data(self, payload):
        '''tests and pushes data to datastore'''
        self._debug_service.info('crest_markethistory.put_data()')
        if not isinstance(payload, pandas.DataFrame):
            raise NotImplementedError('put_data() requires Pandas.DataFrame.  No conversion implemented')

        if not payload.index.name:
            # change pandas.index to db's index_key
            payload.set_index(
                keys=self.index_key,
                drop=True,
                inplace=True
            )

        self._debug_service.debug(
            '-- Testing dataframe against existing table.'
            '\r\tlatest_entry={0}'.format(self._latest_entry)
        )
        if self._latest_entry:
            # avoid overwrites
            datemin = self._latest_entry + timedelta(days=1)
            datemax = payload.index.values.max()
            print('datemin: ' + str(datemin))
            print('datemax: ' + str(datemax))
            if payload.index.values.max() == self._latest_entry:
                self._debug_service.warning('WARNING: db already up-to-date, SKIPPING WRITE')
                return
            else:
                self._debug_service.info(
                    '-- Adjusting dataframe to {0}'.format(self._latest_entry)
                )
                payload = payload.ix[
                    datemin:\
                    datemax
                ]

        self._debug_service.info('-- returning to super().put_data()')
        self._debug_service.debug(str(payload))
        super().put_data(payload)

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
    if DEBUG: print(dataframe)
    return dataframe

## MAIN = TEST ##
if __name__ == '__main__':
    print(ME)
    DEBUG = True
    DEBUG_LOGGER = create_logger(
        'debug_crest_markethistory',
        HERE,
        None,
        'DEBUG'
        )
    DEBUG_LOGGER.log(10, '**STARTING TEST RUN**')

    #CONNECTION_VALUES = table_utils.get_config_values(config, ME)
    SAMPLE_DATA_FRAME = build_sample_dataframe(10)
    TEST_OBJECT = crest_markethistory(
        CONNECTION_VALUES['table'],
        debug=DEBUG,
        loging_handle=DEBUG_LOGGER
    )
    TEST_OBJECT.put_data(SAMPLE_DATA_FRAME)
    TEST_OBJECT.latest_entry(
        regionid=99999999,
        typeid=34
        )
    TEST_DATA = TEST_OBJECT.get_data(
        10,
        "avgPrice",
        "volume",
        regionid=99999999,#30000142,
        typeid=[34, 40],
    )
    print(TEST_DATA)

    #TODO compare TEST_DATA and SAMPLE_DATA_FRAME
