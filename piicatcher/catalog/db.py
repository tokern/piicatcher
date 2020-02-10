from peewee import *

from piicatcher.catalog import Store
from piicatcher.catalog.pii_type_field import PiiTypeField

database_proxy = DatabaseProxy()


class BaseModel(Model):
    class Meta:
        database = database_proxy


class DbSchemas(BaseModel):
    id = AutoField()
    name = CharField()


class DbTables(BaseModel):
    id = AutoField()
    name = CharField()
    schema_id = ForeignKeyField(DbSchemas, 'id')


class DbColumns(BaseModel):
    id = AutoField()
    name = CharField()
    pii_type = PiiTypeField()
    table_id = ForeignKeyField(DbTables, 'id')


class DbFile(BaseModel):
    id = AutoField()
    full_path = CharField()
    mime_type = CharField()
    pii_types = PiiTypeField()


def init_test(path):
    database = SqliteDatabase(path)
    database_proxy.initialize(database)
    database_proxy.connect()
    database_proxy.create_tables([DbSchemas, DbTables, DbColumns])


def model_db_close():
    database_proxy.close()


class DbStore(Store):
    @classmethod
    def setup_database(cls, catalog):
        if catalog is not None:
            database = MySQLDatabase('tokern',
                                     host=catalog['host'],
                                     port=int(catalog['port']),
                                     user=catalog['user'],
                                     password=catalog['password'])
            database_proxy.initialize(database)
            database_proxy.connect()
            database_proxy.create_tables([DbSchemas, DbTables, DbColumns, DbFile])

    @classmethod
    def save_schemas(cls, explorer):
        cls.setup_database(explorer.catalog)
        with database_proxy.atomic():
            schemas = explorer.get_schemas()
            for s in schemas:
                schema_model = DbSchemas.create(name=s.get_name())

                for t in s.get_children():
                    tbl_model = DbTables.create(schema_id=schema_model, name=t.get_name())

                    for c in t.get_children():
                        DbColumns.create(table_id=tbl_model, name=c.get_name(),
                                         pii_type=c.get_pii_types())
