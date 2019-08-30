"""PiiTypes enumerates the different types of PII data"""
from enum import Enum, auto
import json


class PiiTypes(Enum):
    """PiiTypes enumerates the different types of PII data"""
    NONE = auto()
    UNSUPPORTED = auto()
    PHONE = auto()
    EMAIL = auto()
    CREDIT_CARD = auto()
    ADDRESS = auto()
    PERSON = auto()
    LOCATION = auto()
    BIRTH_DATE = auto()
    GENDER = auto()
    NATIONALITY = auto()
    IP_ADDRESS = auto()
    SSN = auto()
    USER_NAME = auto()
    PASSWORD = auto()


# Ref: https://stackoverflow.com/questions/24481852/serialising-an-enum-member-to-json
class PiiTypeEncoder(json.JSONEncoder):
    def default(self, obj):
        if type(obj) == PiiTypes:
            return {"__enum__": str(obj)}
        return json.JSONEncoder.default(self, obj)


def as_enum(d):
    if "__enum__" in d:
        name, member = d["__enum__"].split(".")
        return getattr(PiiTypes, member)
    else:
        return d
