'''Utilities.py: working functions for parsing database stuff'''

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
                    if a_list else None,
                b_group=' unique defined_headers: (' + ','.join(b_list) + ')'\
                    if b_list else None,
                orphan_group=' orphan elements: (' + ','.join(orphan_list) + ')'\
                    if orphan_list else None
            )

        if logger:
            logger.ERROR(error_msg)

        if debug:
            print(error_msg)
    else:
        return_bool = True

    return return_bool
