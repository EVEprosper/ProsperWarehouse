"""exceptions.py custom exceptions for project"""

class WarehouseException(Exception):
    """base class for Warehouse exceptions"""
    pass
class WarehouseWarning(UserWarning):
    """warning parent class for ProsperWarehouse"""
    pass

class VirginInstallOverride(WarehouseException):
    """pip install . environment override"""
    pass

## PROSPER.WAREHOUSE.CONNECTIONS ##
class ConnectionException(WarehouseException):
    """base exception for prosper.warehouse.connections errors"""
    pass
class ConnectionWarning(WarehouseWarning):
    """base warning for prosper.warehouse.connections warnings"""
    pass
class MongoConnectionStringException(ConnectionException):
    """unable to make a valid connection string"""
    pass
class BadProjectionException(ConnectionException):
    """mongoDB only allows all-True/all-False projections"""
    pass

class TestModeWarning(ConnectionWarning):
    """warning raised when connecting to tinymongo instance"""
    pass
class MongoMissingKeysWarning(ConnectionWarning):
    """warning for missing keys"""
    pass

class LibrarySchemaWarning(WarehouseWarning):
    """warning for failing mongo connection and fallback to library"""
    pass
class LocalOverrideSchemaWarning(WarehouseWarning):
    """warning for being in debug mode on non-produciton schema"""
    pass
