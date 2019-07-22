"""PiiTypes enumerates the different types of PII data"""
from enum import Enum, auto


class PiiTypes(Enum):
    """PiiTypes enumerates the different types of PII data"""
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