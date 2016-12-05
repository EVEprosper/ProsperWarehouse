'''snapshot_evecentral.py: contains connection logic for snapshot_evecentral database'''

from os import path
from datetime import datetime, timedelta

import pandas
from plumbum import local
import mysql.connector

#from prosper.common.utilities import get_config, create_logger
import prosper.common.prosper_logging as p_logging
import prosper.common.prosper_config as p_config
from prosper.common.prosper_logging import create_logger
from prosper.common.prosper_config import get_config
import prosper.warehouse.Connection as Connection
import prosper.warehouse.Utilities as table_utils

HERE = path.abspath(path.dirname(__file__))
ME = __file__.replace('.py', '')
CONFIG_ABSPATH = path.join(HERE, 'table_config.cfg')

print('EVE_SERVERINFO: GET_CONFIG')
config = p_config.get_config(CONFIG_ABSPATH)
CONNECTION_VALUES = table_utils.get_config_values(config, ME)

DEBUG = False
class eve_serverinfo(Connection.SQLTable):
    """Worker class for handling eve server info data"""
    _latest_entry=None

    def set_local_path(self):
        """return the local_path for eve_serverinfo"""
        return HERE

    def _define_table_type(self):
        """Set TableType enum"""
        return Connection.TableType.MySQL

    def get_table_create_string(self):
        """fetch/parse table-create file"""
        #TODO: move up logic one level, only need table_name from child
        self._logger.info(ME + '.get_table_create_string()')

        full_table_filepath = None
        table_create_path = config.get(ME, 'table_create_file')
        if '..' in table_create_path:
            local.cwd.chdir(HERE)
            full_table_filepath = local.path(table_create_path)
        else:
            full_table_filepath = local.path(table_create_path)
        self._logger.debug('-- full_table_filepath: {0}'.format(full_table_filepath))

        #TODO: test `exists`
        full_create_string = ''
        with open(full_table_filepath, 'r') as file_handle:
            full_create_string = file_handle.read()

        self._logger.debug('-- full_create_string: {0}'.format(full_create_string))

        return full_create_string

    def get_keys(self):
        """Get Primary/Data/Index keys from config file

        Returns:
            (str): self.index_keys (pushed up to parent object)
            (:obj:`list` :ob:`str`): primary_keys cols for indexing/filtering
            (:obj:`list` :obj:`str`): data_keys cols that have actual data in them

        """
        self._logger.info(ME + '.get_keys()')

        tmp_primary_keys = []
        tmp_data_keys = []
        try:
            tmp_primary_keys = config.get(ME, 'primary_keys').split(',')
            tmp_data_keys = config.get(ME, 'data_keys').split(',')
            self.index_key = config.get(ME, 'index_key') #FIXME: this is bad
        except KeyError as error_msg:
            self._logger.error(
                'EXCEPTION: Keys missing' +
                '\r\tprimary_keys={0}'.format(','.join(tmp_primary_keys)) +
                '\r\tdata_keys={0}'.format(','.join(tmp_data_keys)) +
                '\r\tindex_key={0}'.format(self.index_key),
                exc_info=True
            )
            raise Connection.TableKeysMissing(error_msg, ME)

        self._logger.debug(
            'keys validated:' + \
            '\r\tprimary_keys={0}'.format(','.join(tmp_primary_keys)) +
            '\r\tdata_keys={0}'.format(','.join(tmp_data_keys)) +
            '\r\tindex_key={0}'.format(self.index_key)
        )
        return tmp_primary_keys, tmp_data_keys

    def _set_info(self):
        """Save info about table/datasource

        Returns:
            (str): table name
            (str): schema name

        """
        #TODO move up?
        self._logger.info(ME + '._set_info()')
        return CONNECTION_VALUES['table'], CONNECTION_VALUES['schema']

    def get_connection(self):
        """Get connection objects for table

        Returns:
            (:obj:`MySQLdb.connection`)
            (:obj:`MySQLdb.cursor`)

        """
        self._logger.info(ME + '.get_connection()')
        #self._logger.debug(str(CONNECTION_VALUES))
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
        """Test table connections/contents"""
        self._logger.info(ME + '.test_table()')
        ## Check if table exists ##
        self._logger.info('-- table exists test: START')
        try:
            self.test_table_exists(
                CONNECTION_VALUES['table'],
                CONNECTION_VALUES['schema']
            )
        except Exception as error_msg:
            self._logger.error(
                'EXCEPTION: table does not exist, unable to fix:' +
                '\r\ttable={0}.{1}'.format(CONNECTION_VALUES['schema'],CONNECTION_VALUES['table']),
                exc_info=True
            )
            raise error_msg

        self._logger.info('-- table exists test: PASS')

        ## Check if headers config is correct ##
        self._logger.info('-- table headers test: START')

        try:
            self.test_table_headers(
                CONNECTION_VALUES['table'],
                CONNECTION_VALUES['schema'],
                self.all_keys
            )
        except Exception as error_msg:
            self._logger.error(
                'EXCEPTION: table headers missmatch:' +
                '\r\ttable={0}.{1}'.format(CONNECTION_VALUES['schema'],CONNECTION_VALUES['table']),
                exc_info=True
            )
            raise error_msg

        self._logger.info('-- table headers test: PASS')

    #TODO: maybe too complicated

    def latest_entry(self, **kwargs):
        """Not Implemented"""
        raise NotImplementedError(
            'latest_entry() not implemented because collisions should not be issue'
        )


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
    online_players = random.randint(
        low=10000,
        high=50000,
        size=sizeof_list
    )
    server_open = [True] * sizeof_list

    dataframe = pandas.DataFrame({
        'server_datetime':datetime_range,
        'onlinePlayers':online_players,
        'serverOpen':server_open,
    })

    dataframe.set_index(
        keys='server_datetime',
        drop=True,
        inplace=True
    )

    if DEBUG: dataframe.to_csv('pandas_' + ME + '.csv')
    return dataframe

## MAIN = TEST ##
if __name__ == '__main__':
    print(ME)
    DEBUG = True
    LOG_BUILDER = p_logging.ProsperLogger(
        'debug_eve_serverinfo',
        HERE
    )
    LOG_BUILDER.configure_debug_logger()
    DEBUG_LOGGER = LOG_BUILDER.get_logger()
    DEBUG_LOGGER.log(10, '**STARTING TEST RUN**')

    #CONNECTION_VALUES = table_utils.get_config_values(config, ME)
    SAMPLE_DATA_FRAME = build_sample_dataframe(2, 12)
    TEST_OBJECT = eve_serverinfo(
        CONNECTION_VALUES['table'],
        debug=DEBUG,
        loging_handle=DEBUG_LOGGER
    )
    TEST_OBJECT.put_data(SAMPLE_DATA_FRAME)
    TEST_DATA = TEST_OBJECT.get_data(
        10
    )

    #TODO compare TEST_DATA and SAMPLE_DATA_FRAME
