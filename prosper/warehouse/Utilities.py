'''Utilities.py: working functions for parsing database stuff'''

import datetime

## TODO: UTILTIES ##
def bool_can_write(DatabaseClass):
    '''return permissions if writing to db is allowed'''
    pass

## TODO: UTILTIES ##
def get_config_values(config_object, key_name, debug=False):
    '''parses standardized config object and returns vals, or defaults'''
    if debug:
        print('----get_config_values: Parsing config for: {key_name}'.format(key_name=key_name))
    connection_values = {}
    connection_values['schema'] = config_object.get(key_name, 'db_schema')
    connection_values['host']   = config_object.get(key_name, 'db_host')
    connection_values['user']   = config_object.get(key_name, 'db_user')
    connection_values['passwd'] = config_object.get(key_name, 'db_pw')
    connection_values['port']   = int(config_object.get(key_name, 'db_port'))
    connection_values['table']  = config_object.get(key_name, 'table_name')

    if not(bool(connection_values['schema']) and \
           bool(connection_values['host'])   and \
           bool(connection_values['user'])   and \
           bool(connection_values['passwd']) and \
           bool(connection_values['table'])  and \
           bool(connection_values['port'])):
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
        orphan_list = []
        for element in mismatch_list:
            if element in existing_headers:
                a_list.append(element)
            elif element in defined_headers:
                b_list.append(element)
            else:
                #TODO: logger/debug?
                orphan_list.append(element)
                #print('ORPHAN ELEMENT: ' + str(element))

        error_msg = 'Table Headers not equivalent:{a_group}{b_group}{orphan_group}'.\
            format(
                a_group=' unique existing_headers: (' + ','.join(a_list) + ')'\
                    if a_list else '',
                b_group=' unique defined_headers: (' + ','.join(b_list) + ')'\
                    if b_list else '',
                orphan_group=' orphan elements: (' + ','.join(orphan_list) + ')'\
                    if orphan_list else ''
            )

        if logger:
            logger.ERROR(error_msg)

        if debug:
            print(error_msg)

        return error_msg
    else:
        return_bool = True

    return return_bool

def mysql_cleanup_results(result_to_listify):
    '''cleans up .fetchall() behavior and makes the result listified'''
    ##NOTE: REWRITE##

    result_list = []
    for row in result_to_listify:
        #FIXME: for-each feels hacky
        result_list.append(row[0])

    return result_list

def convert_days_to_datetime(days):
    '''may want to work with "x-days" instead of datetime'''
    datetime_now = datetime.datetime.today()
    datetime_target = datetime_now - datetime.timedelta(days=(days+1))
    datetime_str = datetime_target.strftime('%Y-%m-%d %H:%M:%S')
    return datetime_str

def test_kwargs_headers(primary_keys, kwargs):
    '''parse kwargs and validate against given keys'''
    kwargs_list = list(kwargs.keys())
    #print(set(primary_keys) - set(kwargs))
    if len(set(kwargs_list) - set(primary_keys)):
        mismatch_keys = set(kwargs_list) - set(primary_keys)
        raise TypeError('Invalid args: ' + ','.join(mismatch_keys))
    else:
        return True

def test_args_headers(data_keys, args):
    '''test if args all exist as data_keys (and no extras)'''
    if len(set(args) - set(data_keys)):
        mismatch_keys = set(args) - set(data_keys)
        raise TypeError('Invalid args: ' + ','.join(mismatch_keys))
    else:
        return True

def format_kwargs(kwargs, table_type=None):
    '''parse key:values to build up filter keys'''
    #TODO: change query magic on table_type
    #FIXME: this seems stupid vvv
    query_list = []
    for key, value in kwargs.items():
        value_str = '\'{0}\''.format(value) if isinstance(value, str) \
            else '{0}'.format(value)
        partial_str = '{key}={value} '.\
            format(
                key=key,
                value=value_str
            )
        query_list.append(partial_str)
    query_str = 'AND ({0})'.format('AND '.join(query_list))
    return query_str
