import csv
from abc import abstractmethod, ABC
from argparse import Namespace
from unittest import TestCase

import psycopg2
import pymysql

from piicatcher.explorer.databases import MySQLExplorer, PostgreSQLExplorer


def load_sample_data(connection):
    create_table = """
        CREATE TABLE SAMPLE(
            id VARCHAR(255), gender VARCHAR(255), birthdate DATE, maiden_name VARCHAR(255), lname VARCHAR(255),
            fname VARCHAR(255), address VARCHAR(255), city VARCHAR(255), state VARCHAR(255), zip VARCHAR(255), 
            phone VARCHAR(255), email VARCHAR(255), cc_type VARCHAR(255), cc_number VARCHAR(255), cc_cvc VARCHAR(255),
            cc_expiredate DATE
        )   
    """

    sql = """
        INSERT INTO SAMPLE VALUES(%s,%s,%s,%s,%s,%s,%s,%s,
                                  %s,%s,%s,%s,%s,%s,%s,%s)
    """
    # Get  Data
    with open('tests/samples/sample-data.csv') as csv_file:
        reader = csv.reader(csv_file)

        with connection.cursor() as cursor:
            cursor.execute(create_table)
            header = True
            for row in reader:
                if not header:
                    cursor.execute(sql, row)
                header = False
            connection.commit()


def drop_sample_data(connection):
    drop_table = "DROP TABLE SAMPLE"

    with connection.cursor() as cursor:
        cursor.execute(drop_table)
        connection.commit()


# pylint: disable=too-few-public-methods
class CommonSampleDataTestCases:
    class CommonSampleDataTests(ABC, TestCase):
        @classmethod
        @abstractmethod
        def get_connection(cls):
            raise NotImplementedError

        @classmethod
        def setUpClass(cls):
            connection = cls.get_connection()
            load_sample_data(connection)
            connection.close()

        @classmethod
        def tearDownClass(cls):
            connection = cls.get_connection()
            drop_sample_data(connection)
            connection.close()

        @property
        @abstractmethod
        def explorer(self):
            raise NotImplementedError

        def test_deep_scan(self):
            explorer = self.explorer
            try:
                explorer.scan()
            finally:
                explorer.close_connection()
            print(explorer.get_tabular(True))
            schema = explorer.get_schemas()[0]
            self.assertTrue(schema.has_pii())


class VanillaMySqlExplorerTest(CommonSampleDataTestCases.CommonSampleDataTests):
    @property
    def namespace(self):
        return Namespace(
            host="127.0.0.1",
            user="piiuser",
            password="p11secret",
            database="piidb",
            include_schema=(),
            exclude_schema=(),
            include_table=(),
            exclude_table=(),
            catalog=None
        )

    @classmethod
    def get_connection(cls):
        return pymysql.connect(host="127.0.0.1",
                               user="piiuser",
                               password="p11secret",
                               database="piidb"
                               )

    @property
    def explorer(self):
        return MySQLExplorer(self.namespace)


class SmallSampleMysqlExplorer(MySQLExplorer):
    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        raise NotImplementedError

    @property
    def small_table_max(self):
        return 5


class SmallSampleMySqlExplorerTest(VanillaMySqlExplorerTest):
    @property
    def explorer(self):
        return SmallSampleMysqlExplorer(self.namespace)


class VanillaPGExplorerTest(CommonSampleDataTestCases.CommonSampleDataTests):
    @property
    def namespace(self):
        return Namespace(
            host="127.0.0.1",
            user="piiuser",
            password="p11secret",
            database="piidb",
            include_schema=(),
            exclude_schema=(),
            include_table=(),
            exclude_table=(),
            catalog=None
        )

    @classmethod
    def get_connection(cls):
        return psycopg2.connect(host="127.0.0.1",
                                user="piiuser",
                                password="p11secret",
                                database="piidb"
                                )

    @property
    def explorer(self):
        return PostgreSQLExplorer(self.namespace)


class SmallSamplePGExplorer(MySQLExplorer):
    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        raise NotImplementedError

    @property
    def small_table_max(self):
        return 5


class SmallSamplePGExplorerTest(VanillaMySqlExplorerTest):
    @property
    def explorer(self):
        return SmallSampleMysqlExplorer(self.namespace)
