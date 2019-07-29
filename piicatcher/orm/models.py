from peewee import *

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
