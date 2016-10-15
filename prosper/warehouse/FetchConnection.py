'''FetchConnection.py: importlib magic for importing connections dynamically by-string'''

from os import path #FIXME: plumbum
import importlib.util
import logging
#use NullHandler to avoid "NoneType is not Scriptable" exceptions
DEFAULT_LOGGER = logging.getLogger('NULL')
DEFAULT_LOGGER.addHandler(logging.NullHandler())

## NOTE: importlib magic: http://www.blog.pythonlibrary.org/2016/05/27/python-201-an-intro-to-importlib/

HERE = path.abspath(path.dirname(__file__))
DEFAULT_TABLECONFIG_PATH = path.join(path.dirname(HERE), 'table_configs')
DEBUG = False

def fetch_data_source(
        family_name,
        datasource_name=None,
        table_config_path=DEFAULT_TABLECONFIG_PATH,
        debug=DEBUG,
        logger=DEFAULT_LOGGER
):
    '''importlib magic to fetch table connections'''
    if not datasource_name:
        #make 1 call: snapshot_evecentral.snapshot_evecentral
        datasource_name = family_name
    #else: zkillboard.map_stats, zkillboard.item_stats...
    logger.debug(
        'fetch_data_source' + \
        '\r\family_name={0}'.format(family_name) + \
        '\r\tdatasource_name={0}'.format(datasource_name) + \
        '\r\ttable_config_path={0}'.format(table_config_path) + \
        '\r\tdebug={0}'.format(str(debug)) + \
        '\r\tlogger={0}'.format(str(logger))
    )

    ## Fetch module spec ##
    logger.debug('-- fetching module spec')

    module_path = path.join(table_config_path, family_name + '.py')
    import_spec = importlib.util.spec_from_file_location(datasource_name, module_path)
    if import_spec is None:
        logger.error(
            'EXCEPTION: Unable to find module in path' + \
            '\r\ttable_config_path={0}'.format(table_config_path) + \
            '\r\tfamily_name={0}'.format(family_name) + \
            '\r\tdatasource_name={0}'.format(datasource_name)
        )
        raise FindConnectionModuleError(
            'Unable to find module in path: {0} {1}.{2}'.\
                format(table_config_path, family_name, datasource_name)
        )

    logger.debug('-- fetching module from spec')

    import_module = importlib.util.module_from_spec(import_spec)
    import_spec.loader.exec_module(import_module)
    try:
        connection_class = getattr(import_module, datasource_name)(
            datasource_name,
            debug,
            logger
        )
    except Exception as e_msg:
        logger.exception(
            'EXCEPTION: Unable to load datasource class:' + \
            '\r\texception={0}'.format(str(e_msg)) + \
            '\r\tsource_dir={0}'.format(table_config_path) + \
            '\r\tmodule={0}.{1}'.format(family_name, datasource_name)
        )

        raise LoadConnectionModuleError(
            'Unable to load datasource: {family}.{source} EXCEPTION={exc}'.\
                format(
                    family=family_name,
                    source=datasource_name,
                    exc=str(e_msg)
                )
        )

    logger.info(
        '-- SUCCESS: got connection class {0}.{1}'.\
            format(family_name, datasource_name)
    )

    return connection_class


class FetchConnectionException(Exception):
    '''base class for module-fetch exceptions'''
    def __init__(self, error_msg):
        self.error_msg = error_msg

    def __str__(self):
        return self.error_msg

class FindConnectionModuleError(FetchConnectionException):
    '''failed to find connection at path'''
    pass

class LoadConnectionModuleError(FetchConnectionException):
    '''failed loading connection module'''
    pass

if __name__ == '__main__':
    DEBUG=True
