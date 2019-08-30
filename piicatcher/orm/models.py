import json

from peewee import *

from piicatcher.piitypes import PiiTypeEncoder
from piicatcher.config import config

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
    pii_type = CharField()
    table_id = ForeignKeyField(DbTables, 'id')


def init():
    if 'orm' in config:
        orm = config['orm']
        database = MySQLDatabase('tokern',
                                 host=orm['host'],
                                 port=int(orm['port']),
                                 user=orm['user'],
                                 password=orm['password'])
        database_proxy.initialize(database)
        database_proxy.connect()
        database_proxy.create_tables([DbSchemas, DbTables, DbColumns])


def init_test(path):
    database = SqliteDatabase(path)
    database_proxy.initialize(database)
    database_proxy.connect()
    database_proxy.create_tables([DbSchemas, DbTables, DbColumns])


def model_db_close():
    database_proxy.close()


class Store:
    @classmethod
    def save_schemas(cls, explorer):
        with database_proxy.atomic():
            schemas = explorer.get_schemas()
            for s in schemas:
                schema_model = DbSchemas.create(name=s.get_name())

                for t in s.get_tables():
                    tbl_model = DbTables.create(schema_id=schema_model, name=t.get_name())

                    for c in t.get_columns():
                        col_model = DbColumns.create(table_id=tbl_model, name=c.get_name(),
                                                     pii_type=json.dumps(list(c.get_pii_types()),
                                                                       cls=PiiTypeEncoder))
