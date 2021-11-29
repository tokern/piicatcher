"""Different types of scanners for PII data"""
import logging
import re
from abc import ABC, abstractmethod
from typing import Generator, Tuple

import spacy
from commonregex import CommonRegex
from dbcat.catalog import Catalog
from dbcat.catalog.models import CatColumn, CatSchema, CatTable, PiiTypes
from tqdm import tqdm

from piicatcher.generators import SMALL_TABLE_MAX, _filter_text_columns

LOGGER = logging.getLogger(__name__)


data_logger = logging.getLogger("piicatcher.data")
data_logger.propagate = False
data_logger.setLevel(logging.INFO)
data_logger.addHandler(logging.NullHandler())

scan_logger = logging.getLogger("piicatcher.scan")
scan_logger.propagate = False
scan_logger.setLevel(logging.INFO)
scan_logger.addHandler(logging.NullHandler())


class Scanner(ABC):
    """Scanner abstract class that defines required methods"""

    @abstractmethod
    def scan(self, text):
        """Scan the text and return an array of PiiTypes that are found"""

    @abstractmethod
    def name(self) -> str:
        """Return name of the scanner"""


class RegexScanner(Scanner):
    """A scanner that uses common regular expressions to find PII"""

    def name(self) -> str:
        return "Regex Scanner on data"

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

    def name(self) -> str:
        return "NLP Scanner on data"

    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")

    def scan(self, text):
        """Scan the text and return an array of PiiTypes that are found"""
        doc = self.nlp(text)
        types = set()
        for ent in doc.ents:
            LOGGER.debug("Found %s", ent.label_)
            if ent.label_ == "PERSON":
                types.add(PiiTypes.PERSON)

            if ent.label_ == "GPE":
                types.add(PiiTypes.LOCATION)

            if ent.label_ == "DATE":
                types.add(PiiTypes.BIRTH_DATE)

        LOGGER.debug("PiiTypes are %s", ",".join(str(x) for x in list(types)))
        return list(types)


class ColumnNameScanner(Scanner):
    def name(self) -> str:
        return "Regular Expression Scanner on column name"

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
            "^.*(address|city|state|county|country|zipcode|postal|zone|borough).*$",
            re.IGNORECASE,
        ),
        PiiTypes.USER_NAME: re.compile("^.*user(id|name|).*$", re.IGNORECASE),
        PiiTypes.PASSWORD: re.compile("^.*pass.*$", re.IGNORECASE),
        PiiTypes.SSN: re.compile("^.*(ssn|social).*$", re.IGNORECASE),
    }

    def scan(self, text):
        types = set()
        for pii_type in self.regex:
            if self.regex[pii_type].match(text) is not None:
                types.add(pii_type)

        return list(types)


def shallow_scan(
    catalog: Catalog,
    generator: Generator[Tuple[CatSchema, CatTable, CatColumn], None, None],
):
    scanner = ColumnNameScanner()

    counter = 0
    set_number = 0
    for schema, table, column in generator:
        counter += 1
        LOGGER.debug("Scanning column name %s", column.fqdn)
        types = scanner.scan(column.name)
        if len(types) > 0:
            set_number += 1
            catalog.set_column_pii_type(
                column=column, pii_type=types.pop(), pii_plugin=scanner.name()
            )

    LOGGER.info("Columns Scanned: %d, Columns Labeled: %d", counter, set_number)


def deep_scan(
    catalog: Catalog,
    work_generator: Generator[Tuple[CatSchema, CatTable, CatColumn], None, None],
    generator: Generator[Tuple[CatSchema, CatTable, CatColumn, str], None, None],
    sample_size: int = SMALL_TABLE_MAX,
):
    scanners = [RegexScanner(), NERScanner()]

    total_columns = _filter_text_columns([c for s, t, c in work_generator])
    total_work = len(total_columns) * sample_size

    counter = 0
    set_number = 0

    for schema, table, column, val in tqdm(
        generator, total=total_work, desc="datum", unit="datum"
    ):
        counter += 1
        LOGGER.debug("Scanning column name %s", column.fqdn)
        if val is not None:
            for scanner in scanners:
                for pii in scanner.scan(val):
                    set_number += 1

                    catalog.set_column_pii_type(
                        column=column, pii_type=pii, pii_plugin=scanner.name()
                    )
                    LOGGER.debug("{} has {}".format(column.fqdn, pii))

                    scan_logger.info(
                        "deep_scan", extra={"column": column.fqdn, "pii_types": pii}
                    )
                    data_logger.info(
                        "deep_scan",
                        extra={"column": column.fqdn, "data": val, "pii_types": pii},
                    )
                    break
                else:
                    continue
                break
    LOGGER.info("Columns Scanned: %d, Columns Labeled: %d", counter, set_number)
