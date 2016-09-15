'''connection.py: a framework for defining/managing database connections'''

import abc
import importlib.util
#import configparser

from plumbum import local
import pandas

import prosper.warehouse.Utilities as table_utils #TODO, required?
#from prosper.common.utilities import get_config
class TableType:
    '''enumeration for tabletypes'''
    MySQL = 'MySQL'
    Postgress = 'Postgress'
    NOTDEFINED = 'NOTDEFINED'

    def set_table_type(self, string_enum):
        '''roll enum from string'''
        if string_enum.lower() == 'mysql':
            return self.MySQL
        elif string_enum.lower() == 'postgress':
            return self.Postgress

        else:
            return self.NOTDEFINED

class Database(metaclass=abc.ABCMeta):
    '''parent class for holding database connection info'''
    _debug = False
    _logger = None
    def __init__(self, datasource_name, debug=False, logger=None):
        '''basic info about all databases'''
        self.datasource_name = datasource_name
        self._debug = debug
        self._logger= logger
        self.local_path = self.set_local_path()
        print('--DATABASE: made con/cur')

        self.index_key = None
        self.primary_keys, self.data_keys = self.get_keys()
        self.all_keys=[]
        self.all_keys.append(self.index_key)
        self.all_keys.extend(self.primary_keys)
        self.all_keys.extend(self.data_keys)
        print('--DATABASE: got keys from config')


        self.table_type = self._define_table_type()
        try:
            self.test_table()
        except Exception as error_msg:
            raise error_msg

    def __str__(self):
        return self.datasource_name

    @abc.abstractmethod
    def set_local_path(self):
        '''set CWD path for sourcing files'''
        pass

    @abc.abstractmethod
    def _define_table_type(self):
        '''helps define technology for class definition'''
        pass

    @abc.abstractmethod
    def get_keys(self):
        '''get primary/data keys for query manipulation'''
        pass

    @abc.abstractmethod
    def _set_info(self):
        '''save useful info about table/datasource'''
        pass

    @abc.abstractmethod
    def get_connection(self):
        '''get con[nection] for database handles'''
        pass

    @abc.abstractmethod
    def get_data(
            self,
            datetime_start,
            datetime_end=None, limit=None, kwargs_passthrough=None,
            *args, **kwargs
    ):
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
    def _direct_query(self, query_str):
        '''some tests require direct SQL execution.  Support those calls internally ONLY'''
        pass

    @abc.abstractmethod
    def latest_entry(self, **kwargs):
        '''get the latest instance of a filter to avoid overwrites'''
        pass


class SQLTable(Database):
    '''child class for handling TimeSeries databases'''

    def __init__(self, datasource_name, debug=False, logger=None):
        '''Traditional SQL-style hook setup'''
        self._connection,self._cursor = self.get_connection()
        self.table_name, self.schema_name = self._set_info()
        super().__init__(datasource_name, debug, logger)

    def _direct_query(self, query_str):
        '''direct query for SQL tables'''
        #TODO: if/else check for every query seems wasteful, rework?
        if self.table_type == TableType.MySQL:
            #MYSQL EXECUTE
            try:
                self._cursor.execute(query_str)
                query_result = self._cursor.fetchall()
            except Exception as error_msg:
                raise error_msg

            return query_result

        elif self.table_type == TableType.Postgress:
            #POSTGRESS EXECUTE
            pass

        else:
            raise UnsupportedTableType(
                'unsupported table type: ' + str(self.table_type),
                self.table_name
                )

    def _create_table(self, full_create_string):
        '''handles executing table-create query'''


        command_list = full_create_string.split(';')
        for command in command_list:
            if command.startswith('--') or \
               command == '\n':
                #don't execute comments or blank lines
                #FIXME hacky as fuck
                continue

            self._cursor.execute(command)
            self._connection.commit()

    def test_table_exists(
            self,
            table_name,
            schema_name,
            debug=False,
            logger=None
    ):
        '''basic test for table existing'''
        if not debug:
            debug = self._debug
        if not logger:
            logger= self._logger

        exists_query = ''
        exists_result = False #TODO: remove?
        if self.table_type == TableType.MySQL:
            exists_query = \
            '''SHOW TABLES LIKE \'{table_name}\''''.\
                format(
                    table_name=table_name
                )
        else:
            raise UnsupportedTableType(
                'unsupported table type: ' + str(self.table_type),
                table_name
            )

        try:
            exists_result = self._direct_query(exists_query)
        except Exception as error_msg:
            #TODO logger
            raise error_msg

        if len(exists_result) != 1:
            #TODO logger
            if debug:
                print(
                    '---- TABLE {schema_name}.{table_name} NOT FOUND, creating table'.\
                    format(
                        schema_name=schema_name,
                        table_name =table_name
                    ))
            try:
                self._create_table(self.get_table_create_string())
            except Exception as error_msg:
                raise error_msg

            if debug:
                print(
                    '---- {schema_name}.{table_name} CREATED'.\
                    format(
                        schema_name=schema_name,
                        table_name =table_name
                    ))
        else:
            if debug:
                print(
                    '---- TABLE {schema_name}.{table_name} EXISTS'.\
                    format(
                        schema_name=schema_name,
                        table_name =table_name
                    ))

    def test_table_headers(
            self,
            table_name,
            schema_name,
            defined_headers,
            debug=False,
            logger=None

    ):
        '''test if headers are correctly covered by cfg'''
        if not debug:
            debug = self._debug
        if not logger:
            logger= self._logger

        header_query = ''
        header_result = False #TODO: remove?
        if self.table_type == TableType.MySQL:
            header_query = \
            '''SELECT `COLUMN_NAME`
                FROM `INFORMATION_SCHEMA`.`COLUMNS`
                WHERE `TABLE_SCHEMA`=\'{schema_name}\'
                AND `TABLE_NAME`=\'{table_name}\''''.\
                format(
                    schema_name=schema_name,
                    table_name =table_name
                )
        else:
            raise UnsupportedTableType(
                'unsupported table type: ' + str(self.table_type),
                table_name
            )

        try:
            headers = self._direct_query(header_query)
        except Exception as error_msg:
            #TODO logger
            raise error_msg
        #TODO mysql specific? vvv
        headers = table_utils.mysql_cleanup_results(headers)

        if debug:
            print(headers)
        #FIXME vvv bool_test_headers return values are weird
        if not table_utils.bool_test_headers(
                headers,
                defined_headers,
                debug=debug,
                #logger=Logger #TODO
        ):
            error_msg = 'Table headers not equivalent'
            print(error_msg)
            raise MismatchedHeaders(
                error_msg,
                table_name)

    def get_data(
            self,
            datetime_start,
            *args,
            datetime_end=None,
            limit=None,
            kwargs_passthrough=None,
            **kwargs
    ):
        '''process queries to fetch data'''
        #**kwargs: filter query keys
        #*args: data keys to return
        if kwargs_passthrough:
            kwargs = kwargs_passthrough

        if isinstance(datetime_start, int):
            #assume "last x days"
            datetime_start = table_utils.convert_days_to_datetime(datetime_start)

        ## Test argument contents before executing ##
        try:
            table_utils.test_kwargs_headers(self.primary_keys, kwargs)
        except Exception as error_msg:
            raise InvalidQueryKeys(
                error_msg,
                self.table_name
                )
        try:
            table_utils.test_args_headers(self.data_keys, args)
        except Exception as error_msg:
            raise InvalidDataKeys(
                error_msg,
                self.table_name
                )
        if isinstance(limit, int):
            limit = abs(limit)
        elif limit is not None: #<--FIXME: logic is kinda shitty
            raise BadQueryModifier(
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
            limit_filter = 'LIMIT {limit}'.format(limit=limit)

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
        if self._debug: print(query_string)
        #exit()
        pandas_dataframe = pandas.read_sql(
            query_string,
            self._connection
            )
        return pandas_dataframe

    @abc.abstractmethod
    def get_table_create_string(self):
        '''get/parse table-create file'''
        pass

    def __del__(self):
        '''release connection/cursor'''
        #__del__ needs to be in lowest-child to execute:
        #http://www.electricmonk.nl/log/2008/07/07/python-destructor-and-garbage-collection-notes/
        #FIXME vvv need to close cursor?
        #self._cursor.close()
        self._connection.close()

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

class UnsupportedTableType(ConnectionException):
    '''unable to execute command, not supported'''
    pass

class MismatchedHeaders(ConnectionException):
    '''defined headers and table headers do not line up'''
    pass

class InvalidQueryKeys(ConnectionException):
    '''tried to pivot table on unsupported keys'''
    pass

class InvalidDataKeys(ConnectionException):
    '''tried to filter table on unsupported keys'''
    pass

class BadQueryModifier(ConnectionException):
    '''not a supported modifier type or isinstance() exception'''
    pass

class UnableToWriteToDatastore(ConnectionException):
    '''issues writing to store'''
    pass
