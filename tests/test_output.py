from piicatcher.api import scan_database
from piicatcher.output import output_dict, output_tabular

mysql_output_all = {
    "name": "mysql_src",
    "schemata": [
        {
            "name": "piidb",
            "tables": [
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "name",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "Person",
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "state",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "Address",
                            "sort_order": 1,
                        },
                    ],
                    "name": "full_pii",
                },
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "a",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "b",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 1,
                        },
                    ],
                    "name": "no_pii",
                },
                {
                    "columns": [
                        {
                            "data_type": "int",
                            "name": "id",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "SSN",
                            "sort_order": 1,
                        },
                    ],
                    "name": "partial_data_type",
                },
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "a",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "b",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 1,
                        },
                    ],
                    "name": "partial_pii",
                },
            ],
        }
    ],
}


pg_output_all = {
    "name": "pg_src",
    "schemata": [
        {
            "name": "public",
            "tables": [
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "name",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "Person",
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "state",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "Address",
                            "sort_order": 1,
                        },
                    ],
                    "name": "full_pii",
                },
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "a",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "b",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 1,
                        },
                    ],
                    "name": "no_pii",
                },
                {
                    "columns": [
                        {
                            "data_type": "int4",
                            "name": "id",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "SSN",
                            "sort_order": 1,
                        },
                    ],
                    "name": "partial_data_type",
                },
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "a",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "b",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 1,
                        },
                    ],
                    "name": "partial_pii",
                },
            ],
        }
    ],
}

sqlite_output_all = {
    "name": "sqlite_src",
    "schemata": [
        {
            "name": "",
            "tables": [
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "name",
                            "pii_type": "Person",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "state",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "Address",
                            "sort_order": 1,
                        },
                    ],
                    "name": "full_pii",
                },
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "a",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "b",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 1,
                        },
                    ],
                    "name": "no_pii",
                },
                {
                    "columns": [
                        {
                            "data_type": "int",
                            "name": "id",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "SSN",
                            "sort_order": 1,
                        },
                    ],
                    "name": "partial_data_type",
                },
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "a",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "b",
                            "pii_plugin": None,
                            "pii_type": None,
                            "sort_order": 1,
                        },
                    ],
                    "name": "partial_pii",
                },
            ],
        }
    ],
}


def test_output_dict_all(load_data_and_pull):
    catalog, source_id = load_data_and_pull
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        scan_database(catalog=catalog, source=source)

        result = output_dict(catalog=catalog, source=source, list_all=True)
        if source.source_type == "mysql":
            assert result == mysql_output_all
        elif source.source_type == "postgresql":
            assert result == pg_output_all
        elif source.source_type == "sqlite":
            assert result == sqlite_output_all


mysql_output_only = {
    "name": "mysql_src",
    "schemata": [
        {
            "name": "piidb",
            "tables": [
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "name",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "Person",
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "state",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "Address",
                            "sort_order": 1,
                        },
                    ],
                    "name": "full_pii",
                },
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "SSN",
                            "sort_order": 1,
                        }
                    ],
                    "name": "partial_data_type",
                },
            ],
        }
    ],
}

pg_output_only = {
    "name": "pg_src",
    "schemata": [
        {
            "name": "public",
            "tables": [
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "name",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "Person",
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "state",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "Address",
                            "sort_order": 1,
                        },
                    ],
                    "name": "full_pii",
                },
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "SSN",
                            "sort_order": 1,
                        }
                    ],
                    "name": "partial_data_type",
                },
            ],
        }
    ],
}


sqlite_output_only = {
    "name": "sqlite_src",
    "schemata": [
        {
            "name": "",
            "tables": [
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "name",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "Person",
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "state",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "Address",
                            "sort_order": 1,
                        },
                    ],
                    "name": "full_pii",
                },
                {
                    "columns": [
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_plugin": "ColumnNameRegexDetector",
                            "pii_type": "SSN",
                            "sort_order": 1,
                        }
                    ],
                    "name": "partial_data_type",
                },
            ],
        }
    ],
}


def test_output_dict(load_data_and_pull):
    catalog, source_id = load_data_and_pull
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        scan_database(
            catalog=catalog, source=source, exclude_table_regex=["partial_data_type"]
        )

        result = output_dict(catalog=catalog, source=source, list_all=False)
        if source.source_type == "mysql":
            assert result == mysql_output_only
        elif source.source_type == "postgresql":
            assert result == pg_output_only
        elif source.source_type == "sqlite":
            assert result == sqlite_output_only


mysql_output_tabular = [
    ["piidb", "full_pii", "name", "Person", "ColumnNameRegexDetector",],
    ["piidb", "full_pii", "state", "Address", "ColumnNameRegexDetector",],
    ["piidb", "partial_data_type", "ssn", "SSN", "ColumnNameRegexDetector",],
]

pg_output_tabular = [
    ["public", "full_pii", "name", "Person", "ColumnNameRegexDetector",],
    ["public", "full_pii", "state", "Address", "ColumnNameRegexDetector",],
    ["public", "partial_data_type", "ssn", "SSN", "ColumnNameRegexDetector",],
]

sqlite_output_tabular = [
    ["", "full_pii", "name", "Person", "ColumnNameRegexDetector",],
    ["", "full_pii", "state", "Address", "ColumnNameRegexDetector",],
    ["", "partial_data_type", "ssn", "SSN", "ColumnNameRegexDetector",],
]


def test_output_tabular(load_data_and_pull):
    catalog, source_id = load_data_and_pull
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        scan_database(
            catalog=catalog, source=source, exclude_table_regex=["partial_data_type"]
        )

        result = output_tabular(catalog=catalog, source=source, list_all=False)
        if source.source_type == "mysql":
            assert result == mysql_output_tabular
        elif source.source_type == "postgresql":
            assert result == pg_output_tabular
        elif source.source_type == "sqlite":
            assert result == sqlite_output_tabular


mysql_output_tabular_all = [
    ["piidb", "full_pii", "name", "Person", "ColumnNameRegexDetector",],
    ["piidb", "full_pii", "state", "Address", "ColumnNameRegexDetector",],
    ["piidb", "no_pii", "a", None, None],
    ["piidb", "no_pii", "b", None, None],
    ["piidb", "partial_data_type", "id", None, None],
    ["piidb", "partial_data_type", "ssn", "SSN", "ColumnNameRegexDetector",],
    ["piidb", "partial_pii", "a", None, None],
    ["piidb", "partial_pii", "b", None, None],
]

pg_output_tabular_all = [
    ["public", "full_pii", "name", "Person", "ColumnNameRegexDetector",],
    ["public", "full_pii", "state", "Address", "ColumnNameRegexDetector",],
    ["public", "no_pii", "a", None, None],
    ["public", "no_pii", "b", None, None],
    ["public", "partial_data_type", "id", None, None],
    ["public", "partial_data_type", "ssn", "SSN", "ColumnNameRegexDetector",],
    ["public", "partial_pii", "a", None, None],
    ["public", "partial_pii", "b", None, None],
]

sqlite_output_tabular_all = [
    ["", "full_pii", "name", "Person", "ColumnNameRegexDetector",],
    ["", "full_pii", "state", "Address", "ColumnNameRegexDetector",],
    ["", "no_pii", "a", None, None],
    ["", "no_pii", "b", None, None],
    ["", "partial_data_type", "id", None, None],
    ["", "partial_data_type", "ssn", "SSN", "ColumnNameRegexDetector",],
    ["", "partial_pii", "a", None, None],
    ["", "partial_pii", "b", None, None],
]


def test_output_tabular_all(load_data_and_pull):
    catalog, source_id = load_data_and_pull
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        scan_database(catalog=catalog, source=source)

        result = output_tabular(catalog=catalog, source=source, list_all=True)
        if source.source_type == "mysql":
            assert result == mysql_output_tabular_all
        elif source.source_type == "postgresql":
            assert result == pg_output_tabular_all
        elif source.source_type == "sqlite":
            assert result == sqlite_output_tabular_all
