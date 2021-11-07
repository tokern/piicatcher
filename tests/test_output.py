from dbcat.catalog.models import PiiTypes

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
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "state",
                            "pii_type": PiiTypes.ADDRESS,
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
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "b",
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
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_type": PiiTypes.SSN,
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
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "b",
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
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "state",
                            "pii_type": PiiTypes.ADDRESS,
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
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "b",
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
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_type": PiiTypes.SSN,
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
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "b",
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
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "state",
                            "pii_type": PiiTypes.ADDRESS,
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
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "b",
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
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "ssn",
                            "pii_type": PiiTypes.SSN,
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
                            "pii_type": None,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "b",
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


def test_output_dict_all(load_data):
    catalog, source_id = load_data
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
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "state",
                            "pii_type": PiiTypes.ADDRESS,
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
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "state",
                            "pii_type": PiiTypes.ADDRESS,
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
                            "pii_type": PiiTypes.PERSON,
                            "sort_order": 0,
                        },
                        {
                            "data_type": "text",
                            "name": "state",
                            "pii_type": PiiTypes.ADDRESS,
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


def test_output_dict(load_data):
    catalog, source_id = load_data
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
    ["piidb", "full_pii", "name", "PiiTypes.PERSON"],
    ["piidb", "full_pii", "state", "PiiTypes.ADDRESS"],
    ["piidb", "partial_data_type", "ssn", "PiiTypes.SSN"],
]

pg_output_tabular = [
    ["public", "full_pii", "name", "PiiTypes.PERSON"],
    ["public", "full_pii", "state", "PiiTypes.ADDRESS"],
    ["public", "partial_data_type", "ssn", "PiiTypes.SSN"],
]

sqlite_output_tabular = [
    ["", "full_pii", "name", "PiiTypes.PERSON"],
    ["", "full_pii", "state", "PiiTypes.ADDRESS"],
    ["", "partial_data_type", "ssn", "PiiTypes.SSN"],
]


def test_output_tabular(load_data):
    catalog, source_id = load_data
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
    ["piidb", "full_pii", "name", "PiiTypes.PERSON"],
    ["piidb", "full_pii", "state", "PiiTypes.ADDRESS"],
    ["piidb", "no_pii", "a", "None"],
    ["piidb", "no_pii", "b", "None"],
    ["piidb", "partial_data_type", "id", "None"],
    ["piidb", "partial_data_type", "ssn", "PiiTypes.SSN"],
    ["piidb", "partial_pii", "a", "None"],
    ["piidb", "partial_pii", "b", "None"],
]

pg_output_tabular_all = [
    ["public", "full_pii", "name", "PiiTypes.PERSON"],
    ["public", "full_pii", "state", "PiiTypes.ADDRESS"],
    ["public", "no_pii", "a", "None"],
    ["public", "no_pii", "b", "None"],
    ["public", "partial_data_type", "id", "None"],
    ["public", "partial_data_type", "ssn", "PiiTypes.SSN"],
    ["public", "partial_pii", "a", "None"],
    ["public", "partial_pii", "b", "None"],
]

sqlite_output_tabular_all = [
    ["", "full_pii", "name", "PiiTypes.PERSON"],
    ["", "full_pii", "state", "PiiTypes.ADDRESS"],
    ["", "no_pii", "a", "None"],
    ["", "no_pii", "b", "None"],
    ["", "partial_data_type", "id", "None"],
    ["", "partial_data_type", "ssn", "PiiTypes.SSN"],
    ["", "partial_pii", "a", "None"],
    ["", "partial_pii", "b", "None"],
]


def test_output_tabular_all(load_data):
    catalog, source_id = load_data
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
