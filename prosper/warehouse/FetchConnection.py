'''FetchConnection.py: importlib magic for importing connections dynamically by-string'''

from os import path #FIXME: plumbum
import importlib.util

HERE = path.abspath(path.dirname(__file__))
DEFAULT_TABLECONFIG_PATH = path.join(path.dirname(HERE),'table_configs')
DEBUG = False

def fetch_data_source(
        family_name,
        datasource_name=None,
        table_config_path=DEFAULT_TABLECONFIG_PATH,
        debug=DEBUG,
        logger=None
):
    '''importlib magic to fetch table connections'''
    if not datasource_name:
        #make 1 call: snapshot_evecentral.snapshot_evecentral
        datasource_name=family_name
    #else: zkillboard.map_stats, zkillboard.item_stats...
    module_path = path.join(table_config_path, family_name + '.py')
    import_spec = importlib.util.spec_from_file_location(datasource_name, module_path)
    if import_spec is None:
        error_msg = 'Unable to find module in path: ' + \
        '{table_config_path} {family_name}.{datasource_name}'.\
        format(
            table_config_path=table_config_path,
            family_name=family_name,
            datasource_name=datasource_name
        )
        if debug: print(error_msg)
        if logger: logger.error(error_msg)

        raise FindConnectionModuleError(error_msg)

    import_module = importlib.util.module_from_spec(import_spec)
    import_spec.loader.exec_module(import_module)
    try:
        connection_class = getattr(import_module, datasource_name)(
            datasource_name,
            debug,
            logger
        )
    except Exception as e_msg:
        error_msg = \
        '''Unable to load datasource class:
    source_dir: {table_config_path}
    module: {family_name}.{datasource_name}
    args: {datsource_name},{debug},{logger}
    exception: {e_msg}'''.\
        format(
            table_config_path=table_config_path,
            family_name=family_name,
            datasource_name=datasource_name,
            debug=str(debug),
            logger=str(logger),
            e_msg=str(e_msg)
        )
        if debug: print(error_msg)
        if logger: logger.error(error_msg)

        raise LoadConnectionModuleError(error_msg)

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
