# flake8: noqa
__version__ = "0.20.3"
__google_analytics_tid__ = "UA-148590293-1"

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


class ZipCode(PiiType, type="zip_code"):  # type: ignore
    name = "Zip Code"
    type = "zip_code"
    pass


class PoBox(PiiType, type="po_box"):  # type: ignore
    name = "PO Box"
    type = "po_box"
    pass


class IPAddress(PiiType, type="ip_address"):  # type: ignore
    name = "IP Address"
    type = "ip_address"
    pass


class UserName(PiiType, type="user_name"):  # type: ignore
    name = "User Name"
    type = "user_name"
    pass


class Password(PiiType):
    name = "Password"
    type = "password"
    pass
