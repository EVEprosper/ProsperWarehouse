"""exceptions.py custom exceptions for project"""

class WarehouseException(Exception):
    """base class for Warehouse exceptions"""
    pass

class VirginInstallOverride(WarehouseException):
    """pip install . environment override"""
    pass

class WarehouseWarning(UserWarning):
    """warning parent class for ProsperWarehouse"""
    pass
class LibrarySchemaWarning(WarehouseWarning):
    """warning for failing mongo connection and fallback to library"""
    pass
class LocalOverrideSchemaWarning(WarehouseWarning):
    """warning for being in debug mode on non-produciton schema"""
    pass
