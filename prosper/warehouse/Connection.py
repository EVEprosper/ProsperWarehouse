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
        self.connection,self.cursor = self.get_connection() #TODO: __private?
        print('--DATABASE: made con/cur')
        #TODO: con/cur method only works for direct db, not RESTy

        self.primary_keys, self.data_keys = self.get_keys()
        print('--DATABASE: got keys from config')

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
        self.cursor.close()
        self.connection.close()
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

