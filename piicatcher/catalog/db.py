from peewee import (
    AutoField,
    CharField,
    DatabaseProxy,
    ForeignKeyField,
    Model,
    MySQLDatabase,
)

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
    schema_id = ForeignKeyField(DbSchemas, "id")


class DbColumns(BaseModel):
    id = AutoField()
    name = CharField()
    pii_type = PiiTypeField()
    table_id = ForeignKeyField(DbTables, "id")


class DbFile(BaseModel):
    id = AutoField()
    full_path = CharField()
    mime_type = CharField()
    pii_types = PiiTypeField()


def model_db_close():
    database_proxy.close()


class DbStore(Store):
    @classmethod
    def setup_database(cls, catalog):
        if catalog is not None:
            database = MySQLDatabase(
                "tokern",
                host=catalog["host"],
                port=int(catalog["port"]),
                user=catalog["user"],
                password=catalog["password"],
            )
            database_proxy.initialize(database)
            database_proxy.connect()
            database_proxy.create_tables([DbSchemas, DbTables, DbColumns, DbFile])

    @classmethod
    def save_schemas(cls, explorer):
        cls.setup_database(explorer.catalog)
        with database_proxy.atomic():
            schemas = explorer.get_schemas()
            for s in schemas:
                schema_model, schema_created = DbSchemas.get_or_create(
                    name=s.get_name()
                )
                print(schema_model)
                for t in s.get_children():
                    tbl_model, tbl_created = DbTables.get_or_create(
                        schema_id=schema_model.id, name=t.get_name()
                    )

                    for c in t.get_children():
                        col_model, col_created = DbColumns.get_or_create(
                            table_id=tbl_model.id, name=c.get_name()
                        )
                        if col_created:
                            col_model.pii_type = c.get_pii_types()
                            col_model.save()
