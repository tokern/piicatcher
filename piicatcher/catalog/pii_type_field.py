import json

from peewee import Field

from piicatcher.piitypes import PiiTypeEncoder, as_enum


def enum_value(member):
    return member.value


class PiiTypeField(Field):
    field_type = "VARCHAR(255)"

    def db_value(self, value):
        return json.dumps(sorted(value, key=enum_value), cls=PiiTypeEncoder)

    def python_value(self, value):
        return json.loads(value, object_hook=as_enum)
