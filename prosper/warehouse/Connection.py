'''connection.py: a framework for defining/managing database connections'''

import abc
import importlib.util
#import configparser

#from prosper.common.utilities import get_config
class Database(metaclass=abc.ABCMeta):
    '''parent class for holding database connection info'''
    def __init__(self, datasource_name):
        '''basic info about all databases'''
        self.datasource_name = datasource_name
        self.table_name = ''
        self.__connection,self.__cursor = self.get_connection()
        #TODO: con/cur method only works for direct db, not RESTy

        self.primary_keys, self.data_keys = self.get_keys()
        self.index_key = None
        try:
            self.test_table()
        except Exception as error_msg:
            raise error_msg

    def __str__(self):
        return self.datasource_name

    @abc.abstractmethod
    def get_keys(self):
        '''get primary/data keys for query manipulation'''
        pass

    @abc.abstractmethod
    def get_connection(self):
        '''get con[nection] for database handles'''
        pass

    @abc.abstractmethod
    def get_data(self, *args, **kwargs):
        '''process queries to fetch data'''
        #**kwargs: filter query keys
        #*args: data keys to return
        pass

    @abc.abstractmethod
    def put_data(self, payload):
        '''save data to table (if allowed)'''
        pass

    @abc.abstractmethod
    def test_table(self):
        '''validate that datasource exists, and is ready for data'''
        pass

    @abc.abstractmethod
    def create_table(self):
        '''create the table if needed'''
        #TODO: required?
        pass

class SQLTable(Database):
    '''child class for handling TimeSeries databases'''
    def __del__(self):
        '''release connection/cursor'''
        #__del__ needs to be in lowest-child to execute:
        #http://www.electricmonk.nl/log/2008/07/07/python-destructor-and-garbage-collection-notes/
        self.__cursor.close()
        self.__connection.close()
    #TODO: write helper methods for handling timeseries data

class ConnectionException(Exception):
    '''base class for table-connection exceptions'''
    def __init__(self, message, tablename):
        self.message = message
        self.tablename = tablename

    def __str__(self):
        error_msg = 'CONNECTION EXCEPTION: {tablename}-{message}'.\
        format(
            tablename=self.tablename,
            message=self.message
        )
        return error_msg

class CreateTableError(ConnectionException):
    '''unable to create table (if none existed)'''
    pass

class TableKeysMissing(ConnectionException):
    '''missing keys in config.  Can recreate from db connection...
        but manual listing for easier code for now'''
    pass

## TODO: UTILTIES ##
def bool_can_write(DatabaseClass):
    '''return permissions if writing to db is allowed'''
    pass

## TODO: UTILTIES ##
def get_config_values(config_object, key_name, debug=False):
    '''parses standardized config object and returns vals, or defaults'''
    if debug:
        print('Parsing config for: {key_name}'.format(key_name=key_name))
    connection_values = {}
    connection_values['schema'] = config_object.get(key_name, 'db_schema')
    connection_values['host']   = config_object.get(key_name, 'db_host')
    connection_values['user']   = config_object.get(key_name, 'db_user')
    connection_values['passwd'] = config_object.get(key_name, 'db_pw')
    connection_values['port']   = int(config_object.get(key_name, 'db_port'))
    connection_values['table']  = config_object.get(key_name, 'table_name')

    if bool(connection_values['schema']) and \
       bool(connection_values['host'])   and \
       bool(connection_values['user'])   and \
       bool(connection_values['passwd']) and \
       bool(connection_values['table'])  and \
       bool(connection_values['port']):
        #if (ANY) blank, use defaults
        if debug:
            print('--USING DEFAULT TABLE CONNECTION RULES--')
        connection_values['schema'] = config_object.get('default', 'db_schema')
        connection_values['host']   = config_object.get('default', 'db_host')
        connection_values['user']   = config_object.get('default', 'db_user')
        connection_values['passwd'] = config_object.get('default', 'db_pw')
        connection_values['port']   = int(config_object.get('default', 'db_port'))
        connection_values['table']  = key_name

    return connection_values

## TODO: UTILTIES ##
def bool_test_headers(
        existing_headers,
        defined_headers,
        logger=None,
        debug=False
):
    '''tests if existing_headers == defined_headers'''
    return_bool = False
    #http://stackoverflow.com/a/3462160
    mismatch_list = list(set(existing_headers) - set(defined_headers))

    if len(mismatch_list) > 0:
        a_list = []
        b_list = []
        for element in mismatch_list:
            if element in existing_headers:
                a_list.append(element)
            elif element in defined_headers:
                b_list.append(element)
            else:
                print('ORPHAN ELEMENT: ' + str(element))

        error_msg = 'Table Headers not equivalent: {a_group}; {b_group}'.\
            format(
                #TODO: IF a_list: str() else: None
                a_group='existing_headers: ' + ','.join(a_list),
                b_group='defined_headers:' + ','.join(b_list)
            )

        if logger:
            logger.ERROR(error_msg)

        if debug:
            print(error_msg)
    else:
        return_bool = True

    return return_bool
