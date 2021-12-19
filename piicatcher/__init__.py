# flake8: noqa
__version__ = "0.19.1"

from dbcat.catalog.pii_types import PiiType


class Phone(PiiType):
    name = "Phone"
    type = "phone"
    pass


class Email(PiiType):
    name = "Email"
    type = "email"
    pass


class CreditCard(PiiType, type="credit_card"):  # type: ignore
    name = "Credit Card"
    type = "credit_card"
    pass


class Address(PiiType):
    name = "Address"
    type = "address"
    pass


class Person(PiiType):
    name = "Person"
    type = "person"
    pass


class BirthDate(PiiType, type="birth_date"):  # type: ignore
    name = "Birth Date"
    type = "birth_date"
    pass


class Gender(PiiType):
    name = "Gender"
    type = "gender"
    pass


class Nationality(PiiType):
    name = "Nationality"
    type = "nationality"
    pass


class SSN(PiiType):
    name = "SSN"
    type = "ssn"
    pass


class UserName(PiiType, type="user_name"):  # type: ignore
    name = "User Name"
    type = "user_name"
    pass


class Password(PiiType):
    name = "Password"
    type = "password"
    pass
