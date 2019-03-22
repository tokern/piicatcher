"""Different types of scanners for PII data"""
from abc import ABC, abstractmethod
from commonregex import CommonRegex
import spacy

from piicatcher.piitypes import PiiTypes


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
        self.nlp = spacy.load('en_core_web_sm')

    def scan(self, text):
        """Scan the text and return an array of PiiTypes that are found"""
        doc = self.nlp(text)
        types = set()
        for ent in doc.ents:
            print(ent.label_)
            if ent.label_ == 'PERSON':
                types.add(PiiTypes.PERSON)

            if ent.label_ == 'GPE':
                types.add(PiiTypes.LOCATION)

        return list(types)
