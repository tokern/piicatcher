import pytest

import piicatcher
from piicatcher.api import ScanTypeEnum, list_detectors, scan_database


def test_detector_list():
    assert list(list_detectors()) == ["ColumnNameRegexDetector", "DatumRegexDetector"]


def test_scan_database_shallow(load_sample_data_and_pull):
    catalog, source_id = load_sample_data_and_pull
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        scan_database(catalog=catalog, source=source, include_table_regex=["sample"])

        schemata = catalog.search_schema(source_like=source.name, schema_like="%")

        for column_name, pii_type in [
            ("address", piicatcher.Address()),
            ("city", piicatcher.Address()),
            ("email", piicatcher.Email()),
            ("fname", piicatcher.Person()),
            ("gender", piicatcher.Gender()),
            ("lname", piicatcher.Person()),
            ("maiden_name", piicatcher.Person()),
            ("state", piicatcher.Address()),
        ]:
            column = catalog.get_column(
                source_name=source.name,
                schema_name=schemata[0].name,
                table_name="sample",
                column_name=column_name,
            )
            assert column.pii_type == pii_type

        latest_task = catalog.get_latest_task("piicatcher.{}".format(source.name))
        assert latest_task.status == 0
        assert latest_task.created_at is not None
        assert latest_task.updated_at is not None


@pytest.mark.skip
def test_scan_database_deep(load_sample_data_and_pull):
    catalog, source_id = load_sample_data_and_pull
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        scan_database(
            catalog=catalog,
            source=source,
            include_table_regex=["sample"],
            scan_type=ScanTypeEnum.data,
        )

        schemata = catalog.search_schema(source_like=source.name, schema_like="%")

        for column_name, pii_type in [("id", piicatcher.BirthDate)]:
            column = catalog.get_column(
                source_name=source.name,
                schema_name=schemata[0].name,
                table_name="sample",
                column_name=column_name,
            )
            assert column.pii_type == pii_type
