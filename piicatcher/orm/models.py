import json

from peewee import *

from piicatcher.piitypes import PiiTypeEncoder

database_proxy = DatabaseProxy()


class BaseModel(Model):
    class Meta:
        database = database_proxy


class Schemas(BaseModel):
    id = AutoField()
    name = CharField()


class Tables(BaseModel):
    id = AutoField()
    name = CharField()
    schema_id = ForeignKeyField(Schemas, 'id')


class Columns(BaseModel):
    id = AutoField()
    name = CharField()
    pii_type = CharField()
    table_id = ForeignKeyField(Tables, 'id')


def init(host, port, user, password):
    database = MySQLDatabase('tokern',
                             host=host,
                             port=port,
                             user=user,
                             password=password)
    database_proxy.initialize(database)
    database_proxy.connect()
    database_proxy.create_tables([Schemas, Tables, Columns])


def init_test(path):
    database = SqliteDatabase(path)
    database_proxy.initialize(database)
    database_proxy.connect()
    database_proxy.create_tables([Schemas, Tables, Columns])


def model_db_close():
    database_proxy.close()


class Store:
    @classmethod
    def save_schemas(cls, explorer):
        with database_proxy.atomic():
            schemas = explorer.get_schemas()
            for s in schemas:
                schema_model = Schemas.create(name=s.get_name())

                for t in s.get_tables():
                    tbl_model = Tables.create(schema_id=schema_model, name=t.get_name())

                    for c in t.get_columns():
                        col_model = Columns.create(table_id=tbl_model, name=c.get_name(),
                                                   pii_type=json.dumps(list(c.get_pii_types()),
                                                                       cls=PiiTypeEncoder))
