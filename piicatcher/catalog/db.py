from contextlib import closing

from dbcat import Catalog, catalog_connection, init_db, pull

from piicatcher.catalog import Store


class DbStore(Store):
    @classmethod
    def save_schemas(cls, explorer):
        catalog: Catalog = catalog_connection(explorer.catalog_conf)
        with closing(catalog) as catalog:
            init_db(catalog)
            with catalog.managed_session:
                print("Connection: {}".format(explorer.connection_parameters))
                source = catalog.add_source(
                    explorer.database.get_name(),
                    explorer.type,
                    **explorer.connection_parameters
                )
                pull(catalog, source.name)

            schemas = explorer.get_schemas()
            with catalog.managed_session:
                for s in schemas:
                    for t in s.get_children():
                        for c in t.get_children():
                            column = catalog.get_column(
                                source_name=explorer.database.get_name(),
                                schema_name=s.get_name(),
                                table_name=t.get_name(),
                                column_name=c.get_name(),
                            )
                            if c.has_pii():
                                catalog.set_column_pii_type(
                                    column, c.get_pii_types().pop()
                                )
