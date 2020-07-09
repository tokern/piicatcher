import json
from argparse import Namespace
from unittest import TestCase

from piicatcher.explorer.explorer import Explorer
from piicatcher.explorer.metadata import Column, Database, Schema, Table
from piicatcher.piitypes import PiiTypeEncoder, PiiTypes


class MockExplorer(Explorer):
    def _open_connection(self):
        pass

    def _get_catalog_query(self):
        pass

    @classmethod
    def parser(cls, sub_parsers):
        pass

    def _load_catalog(self):
        pass


class ExplorerTest(TestCase):
    def setUp(self):
        self.explorer = MockExplorer(
            Namespace(
                host="mock_connection",
                include_schema=(),
                exclude_schema=(),
                include_table=(),
                exclude_table=(),
                catalog=None,
            )
        )

        col1 = Column("c1")
        col2 = Column("c2")
        col2._pii = [PiiTypes.LOCATION]

        schema = Schema("s1")
        table = Table(schema, "t1")
        table.add_child(col1)
        table.add_child(col2)

        schema = Schema("testSchema")
        schema.add_child(table)

        self.explorer._database = Database("database")
        self.explorer._database.add_child(schema)

    def test_tabular_all(self):
        self.assertEqual(
            [["testSchema", "t1", "c1", False], ["testSchema", "t1", "c2", True]],
            self.explorer.get_tabular(True),
        )

    def test_tabular_pii(self):
        self.assertEqual(
            [["testSchema", "t1", "c2", True]], self.explorer.get_tabular(False)
        )

    def test_json(self):
        self.assertEqual(
            """[
  {
    "has_pii": false,
    "name": "testSchema",
    "tables": [
      {
        "columns": [
          {
            "name": "c1",
            "pii_types": []
          },
          {
            "name": "c2",
            "pii_types": [
              {
                "__enum__": "PiiTypes.LOCATION"
              }
            ]
          }
        ],
        "has_pii": false,
        "name": "t1"
      }
    ]
  }
]""",
            json.dumps(
                self.explorer.get_dict(), sort_keys=True, indent=2, cls=PiiTypeEncoder
            ),
        )
