# Info
ProsperWareouse is a library for managing database connectivity.  ProsperWarehouse allows easy switching between debug/test/prod for contributors to easily add and use Prosper sources

# Getting started
```python
import prosper.warehouse.connection as prosper_connection

connection = prosper_connection.ProsperWarehouse(
	collection='dummy_collection',
	config=ConfigParser,
	logger=logger,

)

if TESTMODE:
	connection.headless_mode()

```

