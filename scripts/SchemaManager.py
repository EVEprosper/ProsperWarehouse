"""SchemaManager.py: helper script for syncronizing library schemas with SoT"""

from datetime import datetime
from os import path
import json

import semantic_version
from plumbum import cli
from jsonschema import validate

import prosper.common.prosper_logging as p_logging
import prosper.common.prosper_config as p_config
import prosper.warehouse.connections as p_connection
import prosper.warehouse._version as p_version

HERE = path.abspath(path.dirname(__file__))
ROOT = path.dirname(HERE)

CONFIG_PATH = path.join(ROOT, 'prosper', 'warehouse', 'warehouse.cfg')
CONFIG = p_config.ProsperConfig(CONFIG_PATH)

PROD_SCHEMA_PATH = path.join(ROOT, 'prosper', 'warehouse', 'schemas')
MASTER_SCHEMA_PATH = path.join(ROOT, 'prosper', 'warehouse', 'master.schema')

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

    with open(MASTER_SCHEMA_PATH, 'r') as vfh:
        master_schema = json.load(vfh)

    ## Validator will throw if there is issue
    test_schema = full_schema
    test_schema['date'] = full_schema['date'].isoformat()
    validate(test_schema, master_schema)

    return full_schema

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

    def main(self):
        """core logic goes here"""
        logger = LOG_BUILDER.logger
        logger.info('HELLO WORLD -- PULL')

@ManagerScript.subcommand('push')
class PushSchemas(cli.Application):
    """pushes one schema up to mongoDB"""

    source = {}
    filename = ''
    @cli.switch(
        ['-s', '--source'],
        str,
        help='schema file to upload'
    )
    def override_source(self, source_path):
        """schema to upload

        Note:
            will try ../prosper/warehouse/schemas first
        Args:
            source_path (str): path to schema

        Returns:
            (:obj:`dict`) parsed JSON file

        """
        prod_fullpath = path.join(PROD_SCHEMA_PATH, source_path)
        self.filename = path.basename(prod_fullpath)
        if not path.isfile(prod_fullpath):
            if not path.isfile(source_path):
                raise FileNotFoundError
            else:
                prod_fullpath = source_path
                ## load from direct path

        with open(prod_fullpath, 'r') as data_fh:
            data = json.load(data_fh)

        self.source = data

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

        logger.info('building mongo connector')
        connector = p_connection.ProsperWarehouse(
            'schemas',
            config=CONFIG,
            testmode=DEBUG,
            logger=logger
        )

        current_ver = find_schema_version(
            connector,
            logger=logger,
            file_name=self.filename)

        if current_ver:
            do_update = validate_schema(
                connector,
                self.source,
                logger=logger,
                file_name=self.filename,
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
                self.source,
                self.filename,
                current_ver,
                logger=logger
            )
            logger.info('Writing schema to database')
            with connector as mongo_handle:
                mongo_handle.insert_one(full_schema)
        else:
            logger.info('No update to do -- Have a nice day!')

if __name__ == '__main__':
    ManagerScript.run()
