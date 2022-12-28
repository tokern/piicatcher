from dbcat.api import open_catalog, add_postgresql_source
from piicatcher.api import scan_database

catalog = open_catalog(app_dir='/tmp/.config/piicatcher', path=':memory:', secret='my_secret')

with catalog.managed_session:
    # Add a postgresql source
    source = add_postgresql_source(catalog=catalog, name="pg_db", uri="covid19db.org", username="covid19",
                                    password="covid19", database="covid19")
    output = scan_database(catalog=catalog, source=source, output_format="json")

print(output)
