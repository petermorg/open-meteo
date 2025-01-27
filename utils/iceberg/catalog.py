import os

from pyiceberg.table import Table
from pyiceberg.schema import Schema
from pyiceberg.catalog import load_catalog, Catalog


class IcebergCatalog:
    def __init__(
            self, 
            catalog_name: str = 'local', 
            uri: str = 'sqlite:///data_store/catalog.db', 
            warehouse: str = 'file://data_store',
            default_namespace: str = 'default'
            ):
        
        self.initialise_iceberg()
        self.catalog = self.initialise_catalog(catalog_name, uri, warehouse)
        self.initialise_namespace(default_namespace)

    def initialise_namespace(self, name: str) -> None:
        self.catalog.create_namespace_if_not_exists(name)

    def initialise_table(self, identifier, schema: Schema) -> Table:
        return self.catalog.create_table_if_not_exists(
            identifier=identifier,
            schema=schema,
        )
    
    @staticmethod
    def initialise_iceberg():
        os.environ['PYICEBERG_HOME'] = os.getcwd()
        print(os.environ["PYICEBERG_HOME"])

    @staticmethod
    def initialise_catalog(name: str, uri: str, warehouse: str) -> Catalog:
        return load_catalog(name, uri=uri, warehouse=warehouse)
