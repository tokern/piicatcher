from unittest import TestCase

from piicatcher.scanner import RegexScanner, NERScanner, ColumnNameScanner
from piicatcher.piitypes import PiiTypes


class RegexTestCase(TestCase):
    def setUp(self):
        self.parser = RegexScanner()

    def test_phones(self):
        matching = ["12345678900", "1234567890", "+1 234 567 8900", "234-567-8900",
                    "1-234-567-8900", "1.234.567.8900", "5678900", "567-8900",
                    "(123) 456 7890", "+41 22 730 5989", "(+41) 22 730 5989",
                    "+442345678900"]
        for text in matching:
            self.assertEqual(self.parser.scan(text), [PiiTypes.PHONE])

    def test_emails(self):
        matching = ["john.smith@gmail.com", "john_smith@gmail.com", "john@example.net"]
        non_matching = ["john.smith@gmail..com"]
        for text in matching:
            self.assertEqual(self.parser.scan(text), [PiiTypes.EMAIL])
        for text in non_matching:
            self.assertEqual(self.parser.scan(text), [])

    def test_credit_cards(self):
        matching = ["0000-0000-0000-0000", "0123456789012345",
                    "0000 0000 0000 0000", "012345678901234"]
        for text in matching:
            self.assertTrue(PiiTypes.CREDIT_CARD in self.parser.scan(text))

    def test_street_addresses(self):
        matching = ["checkout the new place at 101 main st.",
                    "504 parkwood drive", "3 elm boulevard",
                    "500 elm street "]
        non_matching = ["101 main straight"]

        for text in matching:
            self.assertEqual(self.parser.scan(text), [PiiTypes.ADDRESS])
        for text in non_matching:
            self.assertEqual(self.parser.scan(text), [])


class NERTests(TestCase):
    def setUp(self):
        self.parser = NERScanner()

    def test_person(self):
        types = self.parser.scan("Jonathan is in the office")
        self.assertTrue(PiiTypes.PERSON in types)

    def test_location(self):
        types = self.parser.scan("Jonathan is in Bangalore")
        self.assertTrue(PiiTypes.LOCATION in types)

    def test_date(self):
        types = self.parser.scan("Jan 1 2016 is a new year")
        self.assertTrue(PiiTypes.BIRTH_DATE in types)


class ColumnNameScannerTests(TestCase):
    def setUp(self):
        self.parser = ColumnNameScanner()

    def test_person(self):
        self.assertTrue(PiiTypes.PERSON in self.parser.scan("fname"))
        self.assertTrue(PiiTypes.PERSON in self.parser.scan("full_name"))
        self.assertTrue(PiiTypes.PERSON in self.parser.scan("name"))

    def test_email(self):
        self.assertTrue(PiiTypes.EMAIL in self.parser.scan("email"))
        self.assertTrue(PiiTypes.EMAIL in self.parser.scan("EMAIL"))

    def test_birth_date(self):
        self.assertTrue(PiiTypes.BIRTH_DATE in self.parser.scan("dob"))
        self.assertTrue(PiiTypes.BIRTH_DATE in self.parser.scan("birthday"))

    def test_gender(self):
        self.assertTrue(PiiTypes.GENDER in self.parser.scan("gender"))

    def test_nationality(self):
        self.assertTrue(PiiTypes.NATIONALITY in self.parser.scan("nationality"))

    def test_address(self):
        self.assertTrue(PiiTypes.ADDRESS in self.parser.scan("address"))
        self.assertTrue(PiiTypes.ADDRESS in self.parser.scan("city"))
        self.assertTrue(PiiTypes.ADDRESS in self.parser.scan("state"))
        self.assertTrue(PiiTypes.ADDRESS in self.parser.scan("country"))
        self.assertTrue(PiiTypes.ADDRESS in self.parser.scan("zipcode"))
        self.assertTrue(PiiTypes.ADDRESS in self.parser.scan("postal"))

    def test_user_name(self):
        self.assertTrue(PiiTypes.USER_NAME in self.parser.scan("user"))
        self.assertTrue(PiiTypes.USER_NAME in self.parser.scan("userid"))
        self.assertTrue(PiiTypes.USER_NAME in self.parser.scan("username"))

    def test_password(self):
        self.assertTrue(PiiTypes.PASSWORD in self.parser.scan("pass"))
        self.assertTrue(PiiTypes.PASSWORD in self.parser.scan("password"))

    def test_ssn(self):
        self.assertTrue(PiiTypes.SSN in self.parser.scan("ssn"))
