'''connection.py: a framework for defining/managing database connections'''

import abc
import importlib.util
import logging
#use NullHandler to avoid "NoneType is not Scriptable" exceptions
DEFAULT_LOGGER = logging.getLogger('NULL').addHandler(logging.NullHandler())

from plumbum import local
import pandas

import prosper.warehouse.Utilities as table_utils #TODO, required?
from prosper.common.utilities import LoggerDebugger
#from prosper.common.utilities import get_config


class TableType:
    '''enumeration for tabletypes'''
    MySQL = 'MySQL'
    Postgres = 'Postgres'
    NOTDEFINED = 'NOTDEFINED'

    def set_table_type(self, string_enum):
        '''roll enum from string'''
        if string_enum.lower() == 'mysql':
            return self.MySQL
        elif string_enum.lower() == 'postgres':
            return self.Postgres

        else:
            return self.NOTDEFINED

class Database(metaclass=abc.ABCMeta):
    '''parent class for holding database connection info'''
    _debug = False
    _logger = None
    def __init__(self, datasource_name, debug=False, loging_handle=DEFAULT_LOGGER):
        '''basic info about all databases'''
        self._debug = debug
        self._logger = loging_handle
        self._logger.info(
            'Database __init__(' + \
            '\r\tdatasouce_name={0},'.format(datasource_name) + \
            '\r\tdebug={0},'.format(str(debug)) + \
            '\r\tlogger={0})'.format(str(loging_handle))
        )

        self._logger.debug('-- Global Setup')

        self.datasource_name = datasource_name
        self.local_path = self.set_local_path()

        self.index_key = None
        self.primary_keys, self.data_keys = self.get_keys()
        self.all_keys=[]
        self.all_keys.append(self.index_key)
        self.all_keys.extend(self.primary_keys)
        self.all_keys.extend(self.data_keys)
        self._logger.info('--DATABASE: got keys from config')

        self.table_type = self._define_table_type()
        try:
            self.test_table()
        except Exception as error_msg:
            self._logger.error(
                'EXCEPTION: test_table failed' + \
                '\r\terror_msg={0}'.format(str(error_msg))
            )
            raise error_msg

    def __str__(self):
        return self.datasource_name

    @abc.abstractmethod
    def set_local_path(self):
        '''set CWD path for sourcing files'''
        #TODO: is level correct/required?
        pass

    @abc.abstractmethod
    def _define_table_type(self):
        '''helps define technology for class definition'''
        #TODO: is level correct/required?
        pass

    @abc.abstractmethod
    def get_keys(self):
        '''get primary/data keys for query manipulation'''
        #TODO: move config parser up to higher layer?
        pass

    @abc.abstractmethod
    def _set_info(self):
        '''save useful info about table/datasource'''
        pass

    #@abc.abstractmethod
    #def get_connection(self):
    #    '''get con[nection] for database handles'''
    #    pass

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

    def __init__(self, datasource_name, debug=False, loging_handle=DEFAULT_LOGGER):
        '''Traditional SQL-style hook setup'''
        self._logger = loging_handle
        self._logger.info('SQLTable __init__()')
        self._connection,self._cursor = self.get_connection()
        self.table_name, self.schema_name = self._set_info()
        super().__init__(datasource_name, debug, loging_handle)

    @abc.abstractmethod
    def get_connection(self):
        '''get con[nection] for database handles'''
        pass

    @abc.abstractmethod
    def get_table_create_string(self):
        '''get/parse table-create file'''
        pass

    def _direct_query(self, query_str):
        '''direct query for SQL tables'''
        #TODO: if/else check for every query seems wasteful, rework?
        self._logger.info('--_direct_query')

        #FIXME vvv do different coonections need different execute/fetch cmds?
        if self.table_type == TableType.MySQL:
            #MYSQL EXECUTE
            try:
                self._cursor.execute(query_str)
                query_result = self._cursor.fetchall()
            except Exception as error_msg:
                #log error one step up
                raise error_msg

            return query_result

        elif self.table_type == TableType.Postgres:
            #POSTGRESS EXECUTE
            raise NotImplementedError('Postgres not supported yet')

        else:
            raise UnsupportedTableType(
                'unsupported table type: ' + str(self.table_type),
                self.table_name
                )

    def _create_table(self, full_create_string):
        '''handles executing table-create query'''
        self._logger.info('--_create_table')
        command_list = full_create_string.split(';')
        for command in command_list:
            self._logger.debug('-- `{0}`'.format(command))
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
            schema_name
    ):
        '''basic test for table existing'''
        self._logger.info(
            '-- test_table_exists(' + \
            '\r\ttable_name={0},'.format(table_name) + \
            '\r\tschema_name={0})'.format(schema_name)
        )

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
        self._logger.debug(exists_query)

        try:
            exists_result = self._direct_query(exists_query)
        except Exception as error_msg:
            self._logger.error(
                'EXCEPTION query failed:' + \
                '\r\terror_msg={0},'.format(str(error_msg)) + \
                '\r\ttable_type={0},'.format(str(self.table_type)) + \
                '\r\tquery={0}'.format(exists_query)
            )
            raise error_msg

        if len(exists_result) != 1:
            warning_str = '-- TABLE {schema_name}.{table_name} NOT FOUND, creating table'.\
                format(
                    schema_name=schema_name,
                    table_name =table_name
                )
            self._logger.warning(
                '-- WARNING: Table not found.  Attempting to create' + \
                '\r\ttable_name={0}.{1}'.format(schema_name, table_name)
            )
            try:
                self._create_table(self.get_table_create_string())
            except Exception as error_msg:
                self._logger.error(
                    'EXCEPTION: Unable to create table' + \
                    '\r\texception={0}'.format(str(error_msg)) + \
                    '\r\ttable_name={0}.{1}'.format(schema_name, table_name) + \
                    '\r\ttable_type={0}'.format(self.table_type) + \
                    '\r\tcreate_table_string={0}'.format(self.get_table_create_string())
                )
                raise error_msg

            self._logger.info('-- Created Table: {0}.{1}'.format(schema_name, table_name))
        else:
            self._logger.info('-- Table Already Exists: {0}.{1}'.format(schema_name, table_name))

    def test_table_headers(
            self,
            table_name,
            schema_name,
            defined_headers

    ):
        '''test if headers are correctly covered by cfg'''
        self._logger.info(
            'test_table_headers(' + \
            '\r\ttable_name={0}'.format(table_name) + \
            '\r\tschema_name={0}'.format(schema_name) + \
            '\r\tdefined_headers={0}'.format(defined_headers)
        )

        header_query = ''
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
        self._logger.debug('-- header_query={0}'.format(header_query))

        try:
            headers = self._direct_query(header_query)
        except Exception as error_msg:
            self._logger.error(
                'EXCEPTION: query failed:' + \
                '\r\texception={0}'.format(str(error_msg)) + \
                '\r\ttable_type={0}'.format(self.table_type) + \
                '\r\tquery={0}'.format(header_query)
            )
            raise error_msg

        #TODO mysql specific? vvv
        headers = table_utils.mysql_cleanup_results(headers)
        self._logger.debug('-- headers={0}'.format(','.join(headers)))

        #FIXME vvv bool_test_headers return values are weird
        if not table_utils.bool_test_headers(
                headers,
                defined_headers,
                debug=self._debug,
                logger=self._logger #TODO
        ):
            self._logger.warning('WARNING: Table headers not equivalent')
            raise MismatchedHeaders(error_msg, table_name)

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
        self._logger.info('get_data()')
        #TODO: self._logger.debug(args)

        #**kwargs: filter query keys
        #*args: data keys to return
        if kwargs_passthrough:
            self._logger.debug(
                '-- received override kwargs: {0}'.format(','.join(kwargs_passthrough.keys()))
            )
            kwargs = kwargs_passthrough

        if isinstance(datetime_start, int):
            #assume "last x days"
            self._logger.debug('-- type(datetime_start)=INT.  Converting to datetime')
            datetime_start = table_utils.convert_days_to_datetime(datetime_start)

        ## Test argument contents before executing ##
        try:
            table_utils.test_kwargs_headers(self.primary_keys, kwargs)
        except Exception as error_msg:
            self._logger.error(
                'EXCEPTION: query/kwarg keys invalid' + \
                'exception={0}'.format(str(error_msg)) + \
                'kwargs.keys={0} '.format(','.join(kwargs.keys())) + \
                'primary_keys={0}'.format(','.join(self.primary_keys))
            )
            raise InvalidQueryKeys(error_msg, self.table_name)

        try:
            table_utils.test_args_headers(self.data_keys, args)
        except Exception as error_msg:
            self._logger.error(
                'EXCEPTION data/args keys invalid ' + \
                'exception={0}'.format(str(error_msg)) + \
                'args={0} '.format(','.join(args)) + \
                'data_keys={0}'.format(','.join(self.data_keys))
            )
            raise InvalidDataKeys(error_msg, self.table_name)

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
            SELECT {index_key},{query_keys},{query_header_string}
            FROM {table_name}
            WHERE {query_general_filter}
            {query_specific_filter}
            ORDER BY {index_key} DESC
            {limit_filter}'''.\
            format(
                query_header_string=query_header_string,
                query_keys=str(','.join(self.primary_keys)),
                table_name=self.table_name,
                query_general_filter=query_general_filter,
                query_specific_filter=query_specific_filter,
                index_key=self.index_key,
                limit_filter=limit_filter
            )
        self._logger.debug(query_string)
        pandas_dataframe = pandas.read_sql(
            query_string,
            self._connection
            )
        self._logger.debug(str(pandas_dataframe))
        return pandas_dataframe

    def put_data(self, payload):
        '''tests and pushes data to datastore'''
        self._logger.info('put_data()')
        if not isinstance(payload, pandas.DataFrame):
            raise NotImplementedError(
                'put_data() requires Pandas.DataFrame.  No conversion implemented'
            )

        test_result = table_utils.bool_test_headers(
            list(payload.columns.values),
            self.all_keys,
            self._debug,
            self._logger
        )

        if not payload.index.name:
            self._logger.info('-- setting payload.index to {0}'.format(self.index_key))
            payload.set_index(
                keys=self.index_key,
                drop=True,
                inplace=True
            )

        #FIXME vvv return types are weird without ConnectionExceptions being passed down
        if isinstance(test_result, str):
            raise MismatchedHeaders(test_result, self.table_name)

        try:
            #FIXME vvv to_sql is a problem
            payload.to_sql(
                name=self.table_name,
                con=self._connection,
                schema=self.schema_name,
                flavor='mysql',
                if_exists='append'
            )
        except Exception as error_msg:
            self._logger.error(
                'EXCEPTION: Unable to write to table' + \
                '\r\texception={0}'.format(str(error_msg)) + \
                '\r\ttable_name={0}.{1}'.format(self.schema_name, self.table_name)
            )
            raise UnableToWriteToDatastore(error_msg, self.table_name)

    def __del__(self):
        '''release connection/cursor'''
        #__del__ needs to be in lowest-child to execute:
        #http://www.electricmonk.nl/log/2008/07/07/python-destructor-and-garbage-collection-notes/
        #FIXME vvv need to close cursor?
        #self._cursor.close()
        self._connection.close()

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
