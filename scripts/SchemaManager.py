"""SchemaManager.py: helper script for syncronizing library schemas with SoT"""

from datetime import datetime
from os import path, listdir
import json
import yaml
from enum import Enum
import warnings

import semantic_version
from plumbum import cli
from jsonschema import validate

import prosper.common.prosper_logging as p_logging
import prosper.common.prosper_config as p_config
import prosper.warehouse.connections as p_connection
import prosper.warehouse._version as p_version
import prosper.warehouse.exceptions as exceptions

HERE = path.abspath(path.dirname(__file__))
ROOT = path.dirname(HERE)

CONFIG_PATH = path.join(ROOT, 'prosper', 'warehouse', 'warehouse.cfg')
CONFIG = p_config.ProsperConfig(CONFIG_PATH)

PROD_SCHEMA_PATH = path.join(ROOT, 'prosper', 'warehouse', 'schemas')
MASTER_SCHEMA_PATH = path.join(ROOT, 'prosper', 'warehouse', 'master.schema')

SCHEMA_FILETYPE = '.schema'

class VersionFileMode(Enum):
    """enum for switching yaml/json"""
    yaml = 'yaml'
    json = 'json'

def find_schema_version(
        connector,
        logger=p_logging.DEFAULT_LOGGER,
        **kwargs
):
    """finds and returns latest schema version in database

    Args:
        connector (:obj:`connections.ProsperWarehouse): connection to db
        **kwargs: key/value pairs for query
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        (:obj:`semantic_version.base.Version`)

    """
    logger.info('Checking for existing schemas')
    with connector as mongo_handle:
        existing_schemas = list(mongo_handle.find(kwargs))
        ## TODO projection={'version':True} ##
        ## TODO Not supported in tinymongo ##

    if not existing_schemas:
        logger.info('--no existing schema found')
        return None #no data to parse

    ver_list = []
    for schema in existing_schemas:
        ver_list.append(semantic_version.Version(schema['version']))

    current_ver = max(ver_list)
    logger.info('--Latest version: %s', str(current_ver))
    return current_ver

def validate_schema(
        connector,
        local_schema,
        logger=p_logging.DEFAULT_LOGGER,
        **kwargs
):
    """check if remote schema needs to be updated

    Args:
        connector (:obj:`connections.ProsperWarehouse): connection to db
        local_schema (:obj:`dict`): local file to test
        logger (:obj:`logging.logger`, optional): logging handle
        **kwargs: query keys

    Returns:
        (bool): do update?

    """
    logger.info('Checking if schema needs update')
    with connector as mongo_handle:
        current_schema = mongo_handle.find_one(kwargs)

    if current_schema['schema'] == local_schema:
        logger.info('--Remote schema = local schema, no update')
        return False
    else:
        logger.info('--Schemas do not match, update required')
        return True

def increment_version(
        current_ver,
        do_major=False,
        do_minor=False,
        logger=p_logging.DEFAULT_LOGGER
):
    """increment version

    Args:
        current_ver (:obj:`semantic_version.base.Version`): current version
        do_major (bool): increment major
        do_minor (bool): increment minor
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        (:obj:`semantic_version.base.Version`) version to write in update

    """
    logger.info('Updating version')
    if not current_ver:
        logger.info('--No version found, defaulting to 1.0.0')
        return semantic_version.Version('1.0.0')

    if do_major:
        update_ver = current_ver.next_major()
        logger.info('--major release: %s', str(update_ver))
        return update_ver

    if do_minor:
        update_ver = current_ver.next_minor()
        logger.info('--minor release: %s', str(update_ver))
        return update_ver

    update_ver = current_ver.next_patch()
    logger.info('--patch release: %s', str(update_ver))
    return update_ver

def add_schema_metadata(
        schema,
        filename,
        schema_version,
        master_schema_path=MASTER_SCHEMA_PATH,
        logger=p_logging.DEFAULT_LOGGER
):
    """build up extra data for tracking schema metadata

    Args:
        schema (:obj:`dict`): schema to write to db
        filename (str): name for schema
        schema_version (:obj:`semantic_version.base.Version`)
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        (:obj:`dict`) JSON-serializable mongo-ready entry to send to db

    """
    logger.info('Building schema for db')
    if '.schema' not in filename:
        raise TypeError('Expected .schema file')
    logger.debug(schema_version)

    full_schema = {}
    full_schema['name'] = filename.replace('.schema', '')
    full_schema['file_name'] = filename
    full_schema['version'] = str(schema_version)
    full_schema['version_numeric'] = p_version.semantic_to_numeric(str(schema_version))
    full_schema['date'] = datetime.utcnow()
    full_schema['schema'] = schema

    logger.debug(full_schema)

    with open(master_schema_path, 'r') as version_fh:
        master_schema = json.load(version_fh)

    ## Validator will throw if there is issue
    test_schema = full_schema
    test_schema['date'] = full_schema['date'].isoformat()
    validate(test_schema, master_schema)

    return full_schema

def get_latest_schema(
        schema_name,
        connector,
        logger=p_logging.DEFAULT_LOGGER
):
    """find latest schema for schema_name

    Args:
        schema_name (str): name of schema
        connector (:obj:`connections.ProsperWarehouse): connection to db
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        (:obj:`dict`): schema entry

    """
    logger.info('--finding latest schema for %s', schema_name)
    by_version = {}
    with connector as mongo_handle:
        data = list(mongo_handle.find({'name': schema_name}))
        ## NOTE: projection() not supported by tinymongo

    for schema in data:
        ver_obj = semantic_version.Version(schema['version'])
        by_version[ver_obj] = schema

    max_version = max(by_version.keys())
    logger.info('--using version: %s', str(max_version))

    current_schema = by_version[max_version]
    logger.debug(current_schema)
    return current_schema

def load_version_file(
        version_file_path,
        version_file_type,
        logger=p_logging.DEFAULT_LOGGER
):
    """load current version file from disk

    Args:
        version_file_path (str): path to version file
        version_file_type (:enum:): info on which parser to use
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        (:obj:`dict`): version info

    """
    if not path.isfile(version_file_path):
        logger.info('--No existing version file found at %s, building new file', version_file_path)
        return {}

    with open(version_file_path, 'r') as v_fh:
        if version_file_type == VersionFileMode.yaml:
            logger.info('--Parsing file: YAML')
            data = yaml.load_all(v_fh)

        elif version_file_type == VersionFileMode.json:
            logger.info('--Parsing file: JSON')
            data = json.load(v_fh)

        else:
            raise NotImplementedError('Unsupported file format {}'.format(version_file_type.value))

    logger.debug(data)
    return data

def update_version_info_file(
        version_info_obj,
        version_file_path,
        version_file_type,
        logger=p_logging.DEFAULT_LOGGER
):
    """write version_info to disk

    Args:
        version_info_obj (:obj:`dict`): version_info data
        version_file_path (str): path to version file
        version_file_type (:enum:): info on which parser to use
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        None

    """
    logger.info('Updating version_info file %s', version_file_path)
    logger.debug(version_info_obj)

    with open(version_file_path, 'w') as v_fh:
        if version_file_type == VersionFileMode.yaml:
            logger.info('--writing file: YAML')
            yaml.dump(version_info_obj, v_fh, default_flow_style=False)

        elif version_file_type == VersionFileMode.json:
            logger.info('writing file: JSON')
            json.dump(version_info_obj, v_fh, indent=4)

        else:
            raise NotImplementedError('Unsupported file format {}'.format(version_file_type.value))

def get_local_schema(
        schema_name,
        schema_filetype=SCHEMA_FILETYPE,
        schema_path=PROD_SCHEMA_PATH,
        logger=p_logging.DEFAULT_LOGGER
):
    """try to load current schema from local path

    Args:
        schema_name (str): name of schema
        schema_filetype (str, optional): file extension for schema files
        schema_path (str, optional): where to look for schema files
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        (:obj:`dict`): current schema (or empty)

    """
    logger.info('Fetching schema from local: %s', schema_name)

    full_schema_path = path.join(schema_path, schema_name + schema_filetype)
    logger.debug(full_schema_path)

    try:
        with open(full_schema_path, 'r') as schema_fh:
            schema = json.load(schema_fh)
    except Exception:
        logger.warning('Unable to load: %s', full_schema_path, exc_info=True)
        schema = {}

    logger.debug(schema)
    return schema

def update_local_schema(
        schema_name,
        schema_object,
        schema_filetype=SCHEMA_FILETYPE,
        schema_path=PROD_SCHEMA_PATH,
        logger=p_logging.DEFAULT_LOGGER,
        **kwargs
):
    """save remote schema to local file

    Args:
        schema_name (str): name of schema
        schema_object (:obj:`dict`): new schema object
        update_time (str): datetime of update
        schema_filetype (str, optional): file extension for schema files
        schema_path (str, optional): where to look for schema files
        logger (:obj:`logging.logger`, optional): logging handle
        **kwargs schema_info keys/values

    Returns:
        (:obj:`dict`): schema info for latest version

    """
    logger.info('Updating local schema: %s', schema_name)

    full_schema_path = path.join(schema_path, schema_name + schema_filetype)

    logger.info('--Writing schema to: %s', full_schema_path)
    with open(full_schema_path, 'w') as schema_fh:
        json.dump(schema_object, schema_fh)

    logger.info('--Updating schema_info')
    schema_info = {}
    schema_info['name'] = schema_name
    schema_info['path'] = path.join(path.dirname(schema_path), schema_name + schema_filetype)
    for key, value in kwargs.items():
        schema_info[key] = str(value)   #avoid type errors when writing data file

    logger.debug(schema_info)

    return schema_info

LOG_BUILDER = None
DEBUG = None
class ManagerScript(cli.Application):
    """application for managing schemas in mongoDB"""
    __log_builder = p_logging.ProsperLogger(
        'ReleaseScript',
        HERE,
        CONFIG
    )

    debug = cli.Flag(
        ['d', '--debug'],
        help='debug mode: do not write to live database'
    )
    @cli.switch(
        ['v', '--verbose'],
        help='Enable verbose messaging'
    )
    def enable_verbose(self):
        """toggle verbose/stdout logger"""
        self.__log_builder.configure_debug_logger()

    @cli.switch(
        ['--type'],
        str,
        help='Override schema filetype -- DEFAULT `{}`'.format(SCHEMA_FILETYPE)
    )
    def override_filetype(self, schema_filetype):
        """override schema filetype"""
        global SCHEMA_FILETYPE
        SCHEMA_FILETYPE = schema_filetype

    def main(self):
        """core logic goes here"""
        if not self.debug:
            self.__log_builder.configure_discord_logger()

        logger = self.__log_builder.logger
        logger.info('HELLO WORLD -- DEFAULT')

        global LOG_BUILDER, DEBUG
        LOG_BUILDER = self.__log_builder
        DEBUG = self.debug

@ManagerScript.subcommand('pull')
class PullSchemas(cli.Application):
    """pulls current schemas down for packaging"""

    update_all = cli.Flag(
        ['a', '--all'],
        help='Update all schemas from remote store'
    )

    single_schema = ''
    @cli.switch(
        ['s', '--schema'],
        str,
        help='Download/update specific schema'
    )
    def override_single_schema(self, schema_name):
        """update single schema"""
        self.single_schema = schema_name

    version_file = path.join(path.dirname(MASTER_SCHEMA_PATH), 'version_info.yml')
    version_mode = VersionFileMode.yaml
    @cli.switch(
        ['--version_file'],
        str,
        help='Change version file'
    )
    def override_version_file(self, version_filename):
        """update version info file"""
        self.version_file = version_filename

        if '.yaml' in version_filename or '.yml' in version_filename:
            self.version_mode = VersionFileMode.yaml
        elif '.json' in version_filename:
            self.version_mode = VersionFileMode.json
        else:
            raise NotImplementedError('{} file format not supported'.format(version_filename))

    schema_path = PROD_SCHEMA_PATH
    @cli.switch(
        ['p', '--path'],
        str,
        help='Override local schema path {}'.format(PROD_SCHEMA_PATH)
    )
    def override_schema_path(self, schema_path):
        """update schema_path"""
        self.schema_path = schema_path

    def main(self):
        """core logic goes here"""
        logger = LOG_BUILDER.logger
        logger.info('HELLO WORLD -- PULL')

        if self.single_schema and self.update_all:
            print('Make up your mind - One or all?!')
            exit(-1)

        logger.info('Building mongo connector')
        connector = p_connection.ProsperWarehouse(
            'schemas',
            config=CONFIG,
            testmode=DEBUG,
            logger=logger
        )

        logger.info('Loading version_info file %s', self.version_file)
        version_info = load_version_file(
            self.version_file,
            self.version_mode,
            logger=logger
        )

        now = datetime.utcnow().isoformat()

        logger.info('Loading schema list')
        schema_list = []
        if self.single_schema:
            schema_list = self.single_schema
        else:
            with connector as mongo_handle:
                schema_list = mongo_handle.distinct('name')

        logger.info('Processing Schemas')
        for schema_name in cli.terminal.Progress(schema_list):
            logger.info('SCHEMA: %s', schema_name)

            if schema_name == 'master':
                logger.info('--skipping schema %s', schema_name)
                continue

            if schema_name not in version_info.keys():
                logger.info('--Schema does not exist in version_info')
                version_info[schema_name] = {}
                version_info[schema_name]['version'] = '0.0.0'

            full_schema = get_latest_schema(
                schema_name,
                connector,
                logger=logger
            )

            latest_schema = full_schema['schema']
            version = semantic_version.Version(full_schema['version'])

            current_local_schema = get_local_schema(
                schema_name,
                schema_path=self.schema_path,
                logger=logger
            )

            if not current_local_schema == latest_schema:
                logger.info('Update required')
                if version < semantic_version.Version(version_info[schema_name]['version']):
                    logger.warning(
                        'local version (%s) > remote version (%s). Skipping write on file %s',
                        str(version),
                        version_info[schema_name]['version'],
                        path.join(self.schema_path, schema_name + SCHEMA_FILETYPE))
                    warnings.warn(
                        'local version > remote version?',
                        exceptions.VersionMismatchWarning())
                    continue

                version_info[schema_name] = update_local_schema(
                    schema_name,
                    latest_schema,
                    schema_path=self.schema_path,
                    logger=logger,
                    update_time=now,
                    date=full_schema['date'],
                    version=str(version),
                    version_numeric=full_schema['version_numeric']
                )

            else:
                logger.info('%s up-to-date v%s', schema_name, str(version))

        update_version_info_file(
            version_info,
            self.version_file,
            self.version_mode,
            logger=logger
        )

        logger.info('OP Success :D')

@ManagerScript.subcommand('push')
class PushSchemas(cli.Application):
    """pushes one schema up to mongoDB"""
    source_list = []
    filename_list = []

    @cli.switch(
        ['-s', '--source'],
        str,
        help='schema file to upload'
    )
    def override_source(self, source_path):
        """single file update style"""
        prod_fullpath = path.join(PROD_SCHEMA_PATH, source_path)
        self.filename_list.append(path.basename(prod_fullpath))
        if not path.isfile(prod_fullpath):
            if not path.isfile(source_path):
                raise FileNotFoundError
            else:
                prod_fullpath = source_path
                ## load from direct path

        with open(prod_fullpath, 'r') as data_fh:
            data = json.load(data_fh)

        self.source_list.append(data)

    @cli.switch(
        ['f', '--folder'],
        str,
        help='Upload all schemas in path'
    )
    def override_folder_path(self, folder_path):
        """upload all files in folder (with .schema file type)"""
        if not folder_path:
            folder_path = PROD_SCHEMA_PATH

        if not path.isdir(folder_path):
            raise FileNotFoundError

        for schema_filename in listdir(folder_path):
            if SCHEMA_FILETYPE not in schema_filename:
                continue

            schema_fullpath = path.join(folder_path, schema_filename)
            self.override_source(schema_fullpath)

    major = cli.Flag(
        ['-M', '--major'],
        help='Increment version -- Major'
    )

    minor = cli.Flag(
        ['-m', '--minor'],
        help='Increment version -- Minor'
    )

    no_release = cli.Flag(
        ['--no-release'],
        help='Debug release -- No prod version'
    )

    def main(self):
        """core logic goes here"""
        logger = LOG_BUILDER.logger
        logger.info('HELLO WORLD -- PUSH')

        if not self.source_list:
            print('schema required -- Exiting')
            exit(-1)

        logger.info('Building mongo connector')
        connector = p_connection.ProsperWarehouse(
            'schemas',
            config=CONFIG,
            testmode=DEBUG,
            logger=logger
        )

        schema_count = len(self.source_list)
        for index in cli.terminal.Progress(range(schema_count)):
            file_name = self.filename_list[index]
            source = self.source_list[index]
            logger.info('Processing %s', file_name)

            current_ver = find_schema_version(
                connector,
                logger=logger,
                file_name=file_name
            )

            if current_ver:
                do_update = validate_schema(
                    connector,
                    source,
                    logger=logger,
                    file_name=file_name,
                    version=str(current_ver)
                )
            else:
                do_update = True

            if do_update:
                logger.info('Updating remote schema')
                current_ver = increment_version(
                    current_ver,
                    self.major,
                    self.minor
                )
                full_schema = add_schema_metadata(
                    source,
                    file_name,
                    current_ver,
                    logger=logger
                )
                logger.info('Writing schema to database')
                with connector as mongo_handle:
                    mongo_handle.insert_one(full_schema)
            else:
                logger.info('No update for %s', file_name)

if __name__ == '__main__':
    ManagerScript.run()
