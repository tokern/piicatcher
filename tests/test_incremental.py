import datetime
import time
from typing import Generator, Tuple

import pytest
from dbcat.api import scan_sources
from dbcat.catalog import Catalog
from dbcat.catalog.models import PiiTypes
from pytest_cases import fixture

from piicatcher.api import scan_database
from piicatcher.generators import column_generator, data_generator
from piicatcher.output import output_dict, output_tabular


@fixture(scope="module")
def setup_incremental(
    load_sample_data, load_data
) -> Generator[Tuple[Catalog, int], None, None]:
    catalog, source_id, name = load_sample_data
    scan_sources(catalog, [name], include_table_regex=["sample"])
    time.sleep(1)
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        scan_database(catalog=catalog, source=source, include_table_regex=["sample"])
        time.sleep(1)
        scan_sources(catalog, [name])
        time.sleep(1)
        scan_database(catalog=catalog, source=source, include_table_regex=["partial.*"])
        yield catalog, source_id


def test_incremental_scan(setup_incremental):
    catalog, source_id = setup_incremental

    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)

        # there should be 2 tasks
        tasks = catalog.get_tasks_by_app_name("piicatcher.{}".format(source.name))
        assert len(tasks) == 2

        first_task = tasks[0]
        second_task = tasks[1]

        schemata = catalog.search_schema(source_like=source.name, schema_like="%")

        # sample table should have earlier timestamp
        sample_table = catalog.get_table(
            source_name=source.name, schema_name=schemata[0].name, table_name="sample"
        )
        assert sample_table.updated_at < first_task.updated_at
        assert sample_table.updated_at < second_task.updated_at

        # full_pii and no_pii should have timestamp between tasks as they are not scanned because of include_table_regex

        for table_name in ["no_pii", "full_pii", "partial_pii"]:
            table = catalog.get_table(
                source_name=source.name,
                schema_name=schemata[0].name,
                table_name=table_name,
            )
            assert table.updated_at > first_task.updated_at
            assert table.updated_at < second_task.updated_at

            for column in catalog.get_columns_for_table(table):
                assert column.updated_at > first_task.updated_at
                assert column.updated_at < second_task.updated_at

        # partial_data_type.ssn should have the latest timestamps

        partial_data_type = catalog.get_table(
            source_name=source.name,
            schema_name=schemata[0].name,
            table_name="partial_data_type",
        )
        assert partial_data_type.updated_at > first_task.updated_at
        assert partial_data_type.updated_at < second_task.updated_at

        partial_data_type_id = catalog.get_column(
            source_name=source.name,
            schema_name=schemata[0].name,
            table_name="partial_data_type",
            column_name="id",
        )
        assert partial_data_type_id.updated_at > first_task.updated_at
        assert partial_data_type_id.updated_at < second_task.updated_at

        partial_data_type_ssn = catalog.get_column(
            source_name=source.name,
            schema_name=schemata[0].name,
            table_name="partial_data_type",
            column_name="ssn",
        )
        assert partial_data_type_ssn.updated_at > first_task.updated_at
        assert (
            second_task.updated_at - partial_data_type_ssn.updated_at
        ) < datetime.timedelta(seconds=3)


def test_incremental_column_generator(setup_incremental):
    catalog, source_id = setup_incremental

    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        tasks = catalog.get_tasks_by_app_name("piicatcher.{}".format(source.name))

        count = 0
        for tpl in column_generator(catalog=catalog, source=source):
            count += 1

        assert count == 24

        count = 0
        for tpl in column_generator(
            catalog=catalog, source=source, last_run=tasks[0].updated_at
        ):
            count += 1

        assert count == 8


def test_incremental_data_generator(setup_incremental):
    catalog, source_id = setup_incremental

    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        tasks = catalog.get_tasks_by_app_name("piicatcher.{}".format(source.name))

        count = 0
        for tpl in data_generator(catalog=catalog, source=source):
            count += 1

        assert count == 434

        count = 0
        for tpl in data_generator(
            catalog=catalog, source=source, last_run=tasks[0].updated_at
        ):
            count += 1

        assert count == 14


def test_incremental_tabular_output(setup_incremental):
    catalog, source_id = setup_incremental

    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        tasks = catalog.get_tasks_by_app_name("piicatcher.{}".format(source.name))
        assert len(tasks) == 2

        first_task = tasks[0]
        second_task = tasks[1]

        # List all PII columns
        op = output_tabular(catalog=catalog, source=source)
        assert len(op) == 9

        # List all PII columns with include_filter
        op = output_tabular(
            catalog=catalog, source=source, include_table_regex=["partial_data_type"]
        )
        assert len(op) == 1

        # List after first task.
        op = output_tabular(
            catalog=catalog, source=source, last_run=first_task.updated_at
        )
        assert len(op) == 1

        # List for second task
        op = output_tabular(
            catalog=catalog, source=source, last_run=second_task.updated_at
        )
        assert len(op) == 0


sqlite_all = {
    "name": "sqlite_src",
    "schemata": [
        {
            "name": "",
            "tables": [
                {
                    "columns": [
                        {
                            "data_type": "VARCHAR(255)",
                            "name": "address",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.ADDRESS,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "VARCHAR(255)",
                            "name": "city",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.ADDRESS,
                            "sort_order": 6,
                        },
                        {
                            "data_type": "VARCHAR(255)",
                            "name": "email",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.EMAIL,
                            "sort_order": 7,
                        },
                        {
                            "data_type": "VARCHAR(255)",
                            "name": "fname",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 8,
                        },
                        {
                            "data_type": "VARCHAR(255)",
                            "name": "gender",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.GENDER,
                            "sort_order": 9,
                        },
                        {
                            "data_type": "VARCHAR(255)",
                            "name": "lname",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 11,
                        },
                        {
                            "data_type": "VARCHAR(255)",
                            "name": "maiden_name",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 12,
                        },
                        {
                            "data_type": "VARCHAR(255)",
                            "name": "state",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.ADDRESS,
                            "sort_order": 14,
                        },
                    ],
                    "name": "sample",
                },
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.SSN,
                            "sort_order": 1,
                        }
                    ],
                    "name": "partial_data_type",
                },
            ],
        }
    ],
}


pg_all = {
    "name": "pg_src",
    "schemata": [
        {
            "name": "public",
            "tables": [
                {
                    "columns": [
                        {
                            "data_type": "varchar",
                            "name": "gender",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.GENDER,
                            "sort_order": 1,
                        },
                        {
                            "data_type": "varchar",
                            "name": "maiden_name",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 3,
                        },
                        {
                            "data_type": "varchar",
                            "name": "lname",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 4,
                        },
                        {
                            "data_type": "varchar",
                            "name": "fname",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 5,
                        },
                        {
                            "data_type": "varchar",
                            "name": "address",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.ADDRESS,
                            "sort_order": 6,
                        },
                        {
                            "data_type": "varchar",
                            "name": "city",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.ADDRESS,
                            "sort_order": 7,
                        },
                        {
                            "data_type": "varchar",
                            "name": "state",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.ADDRESS,
                            "sort_order": 8,
                        },
                        {
                            "data_type": "varchar",
                            "name": "email",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.EMAIL,
                            "sort_order": 11,
                        },
                    ],
                    "name": "sample",
                },
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.SSN,
                            "sort_order": 1,
                        }
                    ],
                    "name": "partial_data_type",
                },
            ],
        }
    ],
}


mysql_all = {
    "name": "mysql_src",
    "schemata": [
        {
            "name": "piidb",
            "tables": [
                {
                    "columns": [
                        {
                            "data_type": "varchar",
                            "name": "email",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.EMAIL,
                            "sort_order": 3,
                        },
                        {
                            "data_type": "varchar",
                            "name": "gender",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.GENDER,
                            "sort_order": 8,
                        },
                        {
                            "data_type": "varchar",
                            "name": "maiden_name",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 10,
                        },
                        {
                            "data_type": "varchar",
                            "name": "lname",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 11,
                        },
                        {
                            "data_type": "varchar",
                            "name": "fname",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 12,
                        },
                        {
                            "data_type": "varchar",
                            "name": "address",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.ADDRESS,
                            "sort_order": 13,
                        },
                        {
                            "data_type": "varchar",
                            "name": "city",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.ADDRESS,
                            "sort_order": 14,
                        },
                        {
                            "data_type": "varchar",
                            "name": "state",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.ADDRESS,
                            "sort_order": 15,
                        },
                    ],
                    "name": "sample",
                },
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.SSN,
                            "sort_order": 1,
                        }
                    ],
                    "name": "partial_data_type",
                },
            ],
        }
    ],
}


sqlite_one = {
    "name": "sqlite_src",
    "schemata": [
        {
            "name": "",
            "tables": [
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.SSN,
                            "sort_order": 1,
                        }
                    ],
                    "name": "partial_data_type",
                }
            ],
        }
    ],
}


pg_one = {
    "name": "pg_src",
    "schemata": [
        {
            "name": "public",
            "tables": [
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.SSN,
                            "sort_order": 1,
                        }
                    ],
                    "name": "partial_data_type",
                }
            ],
        }
    ],
}

mysql_one = {
    "name": "mysql_src",
    "schemata": [
        {
            "name": "piidb",
            "tables": [
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_plugin": "Regular Expression Scanner on column name",
                            "pii_type": PiiTypes.SSN,
                            "sort_order": 1,
                        }
                    ],
                    "name": "partial_data_type",
                }
            ],
        }
    ],
}


def test_incremental_dict_output(setup_incremental):
    catalog, source_id = setup_incremental

    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        tasks = catalog.get_tasks_by_app_name("piicatcher.{}".format(source.name))
        assert len(tasks) == 2

        first_task = tasks[0]
        second_task = tasks[1]

        # List all PII columns
        op = output_dict(catalog=catalog, source=source)
        if source.source_type == "sqlite":
            assert op == sqlite_all
        elif source.source_type == "postgresql":
            assert op == pg_all
        elif source.source_type == "mysql":
            assert op == mysql_all

        # include filter
        op = output_dict(
            catalog=catalog, source=source, include_table_regex=["partial_data_type"]
        )
        if source.source_type == "sqlite":
            assert op == sqlite_one
        elif source.source_type == "postgresql":
            assert op == pg_one
        elif source.source_type == "mysql":
            assert op == mysql_one

        # List after first task.
        op = output_dict(catalog=catalog, source=source, last_run=first_task.updated_at)
        if source.source_type == "sqlite":
            assert op == sqlite_one
        elif source.source_type == "postgresql":
            assert op == pg_one
        elif source.source_type == "mysql":
            assert op == mysql_one

        # List for second task
        op = output_dict(
            catalog=catalog, source=source, last_run=second_task.updated_at
        )
        assert op == {}


@pytest.mark.order(-1)
def test_full_scan(setup_incremental):
    catalog, source_id = setup_incremental

    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        time.sleep(1)
        scan_database(catalog=catalog, source=source, incremental=False)
        # there should be 3 tasks
        tasks = catalog.get_tasks_by_app_name("piicatcher.{}".format(source.name))
        assert len(tasks) == 3

        schemata = catalog.search_schema(source_like=source.name, schema_like="%")

        updated_cols = 0
        for table_name in [
            "no_pii",
            "full_pii",
            "partial_pii",
            "partial_data_type",
            "sample",
        ]:
            table = catalog.get_table(
                source_name=source.name,
                schema_name=schemata[0].name,
                table_name=table_name,
            )
            updated_cols += len(
                list(
                    catalog.get_columns_for_table(table, newer_than=tasks[1].updated_at)
                )
            )

        assert updated_cols == 11
