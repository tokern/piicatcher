from unittest.mock import patch

import pytest

from piicatcher import (
    SSN,
    Address,
    BirthDate,
    CreditCard,
    Email,
    Gender,
    Nationality,
    Password,
    Person,
    Phone,
    UserName,
)
from piicatcher.generators import column_generator, data_generator
from piicatcher.scanner import (
    ColumnNameRegexDetector,
    DatumRegexDetector,
    data_scan,
    metadata_scan,
)


@pytest.mark.parametrize(
    "text",
    [
        "12345678900",
        "1234567890",
        "+1 234 567 8900",
        "234-567-8900",
        "1-234-567-8900",
        "1.234.567.8900",
        "5678900",
        "567-8900",
        "(123) 456 7890",
        "+41 22 730 5989",
        "(+41) 22 730 5989",
        "+442345678900",
    ],
)
def test_datum_regex_phone(text):
    detector: DatumRegexDetector = DatumRegexDetector()
    assert detector.detect(column=None, datum=text) == Phone()


@pytest.mark.parametrize(
    "text", ["john.smith@gmail.com", "john_smith@gmail.com", "john@example.net"]
)
def test_datum_regex_email(text):
    detector: DatumRegexDetector = DatumRegexDetector()
    assert detector.detect(column=None, datum=text) == Email()


@pytest.mark.skip
@pytest.mark.parametrize(
    "text",
    [
        "0000-0000-0000-0000",
        "0123456789012345",
        "0000 0000 0000 0000",
        "012345678901234",
    ],
)
def test_datum_regex_cc(text):
    detector: DatumRegexDetector = DatumRegexDetector()
    assert detector.detect(column=None, datum=text) == CreditCard()


@pytest.mark.parametrize(
    "text",
    [
        "checkout the new place at 101 main st.",
        "504 parkwood drive",
        "3 elm boulevard",
        "500 elm street ",
    ],
)
def test_datum_regex_address(text):
    detector: DatumRegexDetector = DatumRegexDetector()
    assert detector.detect(column=None, datum=text) == Address()


@pytest.mark.parametrize(
    "name", ["fname", "full_name", "name", "FNAME", "FULL_NAME", "NAME"]
)
def test_metadata_name(name):
    with patch("piicatcher.scanner.CatColumn") as mocked:
        instance = mocked.return_value
        instance.name = name
        detector = ColumnNameRegexDetector()
        assert detector.detect(instance) == Person()


@pytest.mark.parametrize(
    "name", ["address", "city", "state", "country", "zipcode", "postal"]
)
def test_column_address(name):
    with patch("piicatcher.scanner.CatColumn") as mocked:
        instance = mocked.return_value
        instance.name = name
        detector = ColumnNameRegexDetector()
        assert detector.detect(instance) == Address()


@pytest.mark.parametrize("name", ["dob", "birthday"])
def test_metadata_dob(name):
    with patch("piicatcher.scanner.CatColumn") as mocked:
        instance = mocked.return_value
        instance.name = name
        detector = ColumnNameRegexDetector()
        assert detector.detect(instance) == BirthDate()


@pytest.mark.parametrize(
    "name,expected",
    [
        ("gender", Gender()),
        ("nationality", Nationality()),
        ("ssn", SSN()),
        ("pass", Password()),
        ("password", Password()),
        ("email", Email()),
        ("user", UserName()),
        ("userid", UserName()),
    ],
)
def test_column_name(name, expected):
    with patch("piicatcher.scanner.CatColumn") as mocked:
        instance = mocked.return_value
        instance.name = name
        detector = ColumnNameRegexDetector()
        assert detector.detect(instance) == expected


def test_shallow_scan(load_data_and_pull):
    catalog, source_id = load_data_and_pull
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        metadata_scan(
            catalog=catalog,
            detectors=[ColumnNameRegexDetector()],
            work_generator=column_generator(catalog=catalog, source=source),
            generator=column_generator(catalog=catalog, source=source),
        )

        schemata = catalog.search_schema(source_like=source.name, schema_like="%")
        state = catalog.get_column(
            source_name=source.name,
            schema_name=schemata[0].name,
            table_name="full_pii",
            column_name="state",
        )
        assert state.pii_type == Address()

        name = catalog.get_column(
            source_name=source.name,
            schema_name=schemata[0].name,
            table_name="full_pii",
            column_name="name",
        )
        assert name.pii_type == Person()


def test_deep_scan(load_data_and_pull):
    catalog, source_id = load_data_and_pull
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        data_scan(
            catalog=catalog,
            detectors=[DatumRegexDetector()],
            work_generator=column_generator(catalog=catalog, source=source),
            generator=data_generator(catalog=catalog, source=source),
        )

        schemata = catalog.search_schema(source_like=source.name, schema_like="%")
        state = catalog.get_column(
            source_name=source.name,
            schema_name=schemata[0].name,
            table_name="partial_pii",
            column_name="a",
        )
        assert state.pii_type == Phone()
