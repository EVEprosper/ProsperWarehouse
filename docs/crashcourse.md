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
## Building new table_configs
### SQL-sources
Required dependencies:
* config parser (prosper.common.utilities.get_config)
* HERE/ME/CONFIG abspathing
* DEBUG/main -- optional but useful

TODO - cleanup?: helper methods:
* set_local_path: return HERE
* _define_table_type (prosper.warehouse.connection.TableType enum)
* get_table_create_string (parses SQL table config, returns str())
* get_keys: reads `primary_keys`, `data_keys`, and `index_key` from config
* _set_info: returns `table_name` and `db_schema` from config

REQUIRED methods:
* get_connection: returns `connection` and `cursor` objects for the class to use
* test_table: tests table integrity and connections to table
    * `self.test_table_exists` has logic for checking for `schema.table_name`.  Can create table if DNE
    * `self.test_table_headers` has logic to compare str() keys from config against table in DB
* get_data():
    * has query logic from `*args`/`**kwargs` TODO
    * returns pandas dataframe (using `pandas.read_sql`)
* put_data():
    * takes pandas dataframe
    * validates headers TODO
    * pushes dataframe into database (using `pandas.write_sql`)

Debug `__main__`
* main has been reserved for running config directly.  Try to instantiate object and execute basic query for TEST

