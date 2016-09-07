'''connection.py: a framework for defining/managing database connections'''

import abc
import importlib.util
import configparser

#from prosper.common.utilities import get_config
class Database(metaclass=abc.ABCMeta):
    '''parent class for holding database connection info'''
    def __init__(self, datasource_name):
        '''basic info about all databases'''
        self.datasource_name = datasource_name
        self.connection = self.get_connection()
        self.cursor = self.get_cursor()
        #TODO: con/cur method only works for direct db, not RESTy

        self.primary_keys = []
        self.data_keys = []

        try:
            self.test_table()
        except Exception as error_msg:
            raise error_msg

    def __str__(self):
        return self.datasource_name

    @abc.abstractmethod
    def get_connection(self):
        '''get con[nection] for database handles'''
        pass

    @abc.abstractmethod
    def get_cursor(self):
        '''get cur[sor] for database handles'''
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

class SQLTable(Database):
    '''child class for handling TimeSeries databases'''
    pass
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

class CreateTableError(Exception):
    '''unable to create table (if none existed)'''
    pass


def bool_can_write(DatabaseClass):
    '''return permissions if writing to db is allowed'''
    pass

def get_config_values(config_object, key_name):
    '''parses standardized config object and returns vals, or defaults'''
    connection_values = {}
    connection_values['db_schema'] = config_object.get(key_name, 'db_schema')
    connection_values['db_host']   = config_object.get(key_name, 'db_host')
    connection_values['db_user']   = config_object.get(key_name, 'db_user')
    connection_values['db_pw']     = config_object.get(key_name, 'db_pw')
    connection_values['db_port']   = int(config_object.get(key_name, 'db_port'))
    connection_values['table_name']= config_object.get(key_name, 'table_name')

    if bool(connection_values['db_schema']) and \
       bool(connection_values['db_host'])   and \
       bool(connection_values['db_user'])   and \
       bool(connection_values['db_pw'])     and \
       bool(connection_values['table_name'])and \
       bool(connection_values['db_port']):
        #if (ANY) blank, use defaults
        connection_values['db_schema'] = config_object.get('default', 'db_schema')
        connection_values['db_host']   = config_object.get('default', 'db_host')
        connection_values['db_user']   = config_object.get('default', 'db_user')
        connection_values['db_pw']     = config_object.get('default', 'db_pw')
        connection_values['db_port']   = int(config_object.get('default', 'db_port'))
        connection_values['table_name']= key_name

    return connection_values
