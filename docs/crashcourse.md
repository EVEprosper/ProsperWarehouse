# Info
ProsperWarehouse is a general utility for handling datasource connections.  By abstracting connections/queries, connections can be managed as their own thing without ruining working scripts.

# Project Shape
## prosper.Warehouse
General platform-specific code/utils go here.  Helper functions and highly-recycled worker functions go here.

### Connection
Connection manages the API design.  top-level functions should be abstracted here.

### Utilities
helper functions (with technology defined) for less platform-specific and more test/logic-specific funcs.

##prosper.table_configs
Individual connection profiles go here.  Technology-specific connection handling and source testing

These configurations will be crawled by importlib.util to be imported by name elsewhere in the project.  These will allow us to wrap away protected functions and unify results.

table_configs also holds all private connection info in one place.  Using table_config_*local*_.cfg will keep private keys private.

# Using ProsperWarehouse
TODO
