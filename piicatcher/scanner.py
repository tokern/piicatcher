"""Different types of scanners for PII data"""
import logging
import re
from abc import ABC, abstractmethod

import spacy
from commonregex import CommonRegex

from piicatcher.piitypes import PiiTypes
# from piicatcher.explorer.databases import ns

# pylint: disable=too-few-public-methods
class Scanner(ABC):
    """Scanner abstract class that defines required methods"""

    @abstractmethod
    def scan(self, text):
        """Scan the text and return an array of PiiTypes that are found"""


class RegexScanner(Scanner):
    """A scanner that uses common regular expressions to find PII"""

    def scan(self, text):
        """Scan the text and return an array of PiiTypes that are found"""
        regex_result = CommonRegex(text)

        types = []
        if regex_result.phones:  # pylint: disable=no-member
            types.append(PiiTypes.PHONE)
        if regex_result.emails:  # pylint: disable=no-member
            types.append(PiiTypes.EMAIL)
        if regex_result.credit_cards:  # pylint: disable=no-member
            types.append(PiiTypes.CREDIT_CARD)
        if regex_result.street_addresses:  # pylint: disable=no-member
            types.append(PiiTypes.ADDRESS)

        return types


class NERScanner(Scanner):
    """A scanner that uses Spacy NER for entity recognition"""

    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")

    def scan(self, text):
        """Scan the text and return an array of PiiTypes that are found"""
        logging.debug("Processing '%s'", text)
        doc = self.nlp(text)
        types = set()
        for ent in doc.ents:
            logging.debug("Found %s", ent.label_)
            if ent.label_ == "PERSON":
                types.add(PiiTypes.PERSON)

            if ent.label_ == "GPE":
                types.add(PiiTypes.LOCATION)

            if ent.label_ == "DATE":
                types.add(PiiTypes.BIRTH_DATE)

        logging.debug("PiiTypes are %s", ",".join(str(x) for x in list(types)))
        return list(types)


class ColumnNameScanner(Scanner):
    def __init__(self, exclude=()):
        self.excluded_column = exclude
    regex = {
        PiiTypes.PERSON: re.compile(
            "^.*(firstname|fname|lastname|lname|"
            "fullname|maidenname|_name|"
            "nickname|name_suffix|name).*$",
            re.IGNORECASE,
        ),
        PiiTypes.EMAIL: re.compile("^.*(email|e-mail|mail).*$", re.IGNORECASE),
        PiiTypes.BIRTH_DATE: re.compile(
            "^.*(date_of_birth|dateofbirth|dob|"
            "birthday|date_of_death|dateofdeath).*$",
            re.IGNORECASE,
        ),
        PiiTypes.GENDER: re.compile("^.*(gender).*$", re.IGNORECASE),
        PiiTypes.NATIONALITY: re.compile("^.*(nationality).*$", re.IGNORECASE),
        PiiTypes.ADDRESS: re.compile(
            "^.*(address|city|state|county|country|" "zipcode|postal|zone|borough).*$",
            re.IGNORECASE,
        ),
        PiiTypes.USER_NAME: re.compile("^.*user(id|name|).*$", re.IGNORECASE),
        PiiTypes.PASSWORD: re.compile("^.*pass.*$", re.IGNORECASE),
        PiiTypes.SSN: re.compile("^.*(ssn|social).*$", re.IGNORECASE),
    }

    def scan(self, text):
        types = set()
        ignore_regex = re.compile(self.excluded_column[0], re.IGNORECASE)
        for pii_type in self.regex:
            if self.regex[pii_type].match(text) is not None and ignore_regex.match(text) is None:    # ignore_regex shouldn't match
                types.add(pii_type)

        logging.debug("PiiTypes are %s", ",".join(str(x) for x in list(types)))
        return list(types)
