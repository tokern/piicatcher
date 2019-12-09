import json
import logging
from peewee import Field
from piicatcher.piitypes import PiiTypeEncoder, as_enum


class PiiTypeField(Field):
    field_type = "varchar"

    def db_value(self, value):
        return json.dumps(list(value), cls=PiiTypeEncoder)

    def python_value(self, value):
        return json.loads(value, object_hook=as_enum)