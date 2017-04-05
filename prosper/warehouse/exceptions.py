"""exceptions.py custom exceptions for project"""

class WarehouseException(Exception):
    """base class for Warehouse exceptions"""
    pass

class VirginInstallOverride(WarehouseException):
    """pip install . environment override"""
    pass
