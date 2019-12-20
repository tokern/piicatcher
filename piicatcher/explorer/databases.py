from abc import abstractmethod

import pymysql
import psycopg2
import pymssql
import cx_Oracle

import logging

from piicatcher.explorer.explorer import Explorer


class RelDbExplorer(Explorer):
    def __init__(self, ns):
        super(RelDbExplorer, self).__init__(ns)
        self.host = ns.host
        self.user = ns.user
        self.password = ns.password
        self.port = int(ns.port) if 'port' in vars(ns) and ns.port is not None else self.default_port

    @property
    @abstractmethod
    def default_port(self):
        pass

    @classmethod
    def factory(cls, ns):
        logging.debug("Relational Db Factory entered")
        explorer = None
        if ns.connection_type == "mysql":
            explorer = MySQLExplorer(ns)
        elif ns.connection_type == "postgres" or ns.connection_type == "redshift":
            explorer = PostgreSQLExplorer(ns)
        elif ns.connection_type == "sqlserver":
            explorer = MSSQLExplorer(ns)
        elif ns.connection_type == "oracle":
            explorer = OracleExplorer(ns)
        assert (explorer is not None)

        return explorer

    @classmethod
    def parser(cls, sub_parsers):
        sub_parser = sub_parsers.add_parser("db")

        sub_parser.add_argument("-s", "--host", required=True,
                                help="Hostname of the database. File path if it is SQLite")
        sub_parser.add_argument("-R", "--port",
                                help="Port of database.")
        sub_parser.add_argument("-u", "--user",
                                help="Username to connect database")
        sub_parser.add_argument("-p", "--password",
                                help="Password of the user")
        sub_parser.add_argument("-d", "--database", default='',
                                help="Name of the database")
        sub_parser.add_argument("-t", "--connection-type", default="sqlite",
                                choices=["sqlite", "mysql", "postgres", "redshift", "oracle", "sqlserver"],
                                help="Type of database")
        sub_parser.add_argument("-f", "--output-format", choices=["ascii_table", "json", "db"],
                                default="ascii_table",
                                help="Choose output format type")

        cls.scan_options(sub_parser)
        sub_parser.set_defaults(func=RelDbExplorer.dispatch)


class MySQLExplorer(RelDbExplorer):
    _catalog_query = """
        SELECT 
            TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE 
        FROM 
            INFORMATION_SCHEMA.COLUMNS 
        WHERE 
            TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql')
            AND DATA_TYPE RLIKE 'char.*|varchar.*|text'
        ORDER BY table_schema, table_name, column_name 
    """

    _sample_query_template = "select {column_list} from {schema_name}.{table_name} limit 10"

    def __init__(self, ns):
        super(MySQLExplorer, self).__init__(ns)
        self.database = ns.database if 'database' in vars(ns) and ns.database is not None else None

    @property
    def default_port(self):
        return 3306

    def _open_connection(self):
        return pymysql.connect(host=self.host,
                               port=self.port,
                               user=self.user,
                               password=self.password,
                               database=self.database)

    def _get_catalog_query(self):
        return self._catalog_query

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls.query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            schema_name=schema_name.get_name(),
            table_name=table_name.get_name()
        )


class PostgreSQLExplorer(RelDbExplorer):
    _catalog_query = """
        SELECT 
            TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE 
        FROM 
            INFORMATION_SCHEMA.COLUMNS 
        WHERE 
            TABLE_SCHEMA NOT IN ('information_schema', 'pg_catalog')
            AND DATA_TYPE SIMILAR TO '%char%|%text%'
        ORDER BY table_schema, table_name, column_name 
    """

    _sample_query_template = "select {column_list} from {schema_name}.{table_name} TABLESAMPLE BERNOULLI (10)"

    def __init__(self, ns):
        super(PostgreSQLExplorer, self).__init__(ns)
        self.database = ns.database if 'database' in vars(ns) and ns.database is not None else None

    @property
    def default_database(self):
        return "public"

    @property
    def default_port(self):
        return 5432

    def _open_connection(self):
        return psycopg2.connect(host=self.host,
                                port=self.port,
                                user=self.user,
                                password=self.password,
                                database=self.database)

    def _get_catalog_query(self):
        return self._catalog_query

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls.query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            schema_name=schema_name.get_name(),
            table_name=table_name.get_name()
        )


class MSSQLExplorer(RelDbExplorer):
    _catalog_query = """
        SELECT 
            TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE 
        FROM 
            INFORMATION_SCHEMA.COLUMNS 
        WHERE 
            DATA_TYPE LIKE '%char%'
        ORDER BY TABLE_SCHEMA, table_name, ordinal_position 
    """

    _sample_query_template = "SELECT TOP 10 * FROM {schema_name}.{table_name} TABLESAMPLE (1000 ROWS)"

    def __init__(self, ns):
        super(MSSQLExplorer, self).__init__(ns)
        self.database = self.database if ns.database is None else ns.database

    @property
    def default_database(self):
        return "public"

    @property
    def default_port(self):
        return 1433

    def _open_connection(self):
        return pymssql.connect(host=self.host,
                               port=self.port,
                               user=self.user,
                               password=self.password,
                               database=self.database)

    def _get_catalog_query(self):
        return self._catalog_query

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls._sample_query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            schema_name=schema_name.get_name(),
            table_name=table_name.get_name()
        )


class OracleExplorer(RelDbExplorer):
    _catalog_query = """
        SELECT 
            '{db}', TABLE_NAME, COLUMN_NAME 
        FROM 
            USER_TAB_COLUMNS 
        WHERE UPPER(DATA_TYPE) LIKE '%CHAR%'
        ORDER BY TABLE_NAME, COLUMN_ID 
    """

    _sample_query_template = "select {column_list} from {table_name} sample(5)"
    _select_query_template = "select {column_list} from {table_name}"
    _count_query = "select count(*) from {table_name}"

    def __init__(self, ns):
        super(OracleExplorer, self).__init__(ns)
        self.database = ns.database

    @property
    def default_port(self):
        return 1521

    def _open_connection(self):
        return cx_Oracle.connect(self.user,
                                 self.password,
                                 "%s:%d/%s" % (self.host, self.port, self.database))

    def _get_catalog_query(self):
        return self._catalog_query.format(db=self.database)

    @classmethod
    def _get_select_query(cls, schema_name, table_name, column_list):
        return cls._select_query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            table_name=table_name.get_name()
        )

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls._sample_query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            table_name=table_name.get_name()
        )

    @classmethod
    def _get_count_query(cls, schema_name, table_name):
        return cls._count_query.format(
            table_name=table_name.get_name()
        )
