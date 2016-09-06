'''connection.py: a framework for defining/managing database connections'''

import abc
import importlib.util

#from prosper.common.utilities import get_config
def bool_can_write(DatabaseClass):
    '''return permissions if writing to db is allowed'''
    pass

class Database(metaclass=abc.ABCMeta):
    '''parent class for holding database connection info'''
    def __init__(self, datasource_name):
        '''basic info about all databases'''
        self.datasource_name = datasource_name
        self.connection = None
        self.cursor = None

        self.primary_keys = []
        self.data_keys = []

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

class TimeSeries(Database):
    '''child class for handling TimeSeries databases'''
    pass
    #TODO: write helper methods for handling timeseries data
