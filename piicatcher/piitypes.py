"""PiiTypes enumerates the different types of PII data"""
from enum import Enum, auto


class PiiTypes(Enum):
    """PiiTypes enumerates the different types of PII data"""
    PHONE = auto()
    EMAIL = auto()
    CREDIT_CARD = auto()
    ADDRESS = auto()
