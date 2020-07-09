import logging
from argparse import Namespace

import click
import snowflake.connector

from piicatcher.explorer.databases import (
    exclude_schema_help_text,
    exclude_table_help_text,
    schema_help_text,
    table_help_text,
)
from piicatcher.explorer.explorer import Explorer


@click.command('snowflake')
@click.pass_context
@click.option("-a", "--account", required=True, help="Snowflake Account")
@click.option("-w", "--warehouse", required=True, help="Snowflake Warehouse name")
@click.option("-d", "--database", required=True, help="Name of the database")
@click.option("-u", "--user", help="Username to connect database")
@click.option("-p", "--password", help="Password of the user")
@click.option("--okta-account-name", help="Okta account name if authenticator is OKTA")
@click.option("--oauth-host", help="OAuth Host if authenticator is OAUTH")
@click.option("--oauth-token", help="OAuth token if authenticator is OAUTH")
@click.option("--authenticator", type=click.Choice(["userpasswd", "externalbrowser", "oauth", "okta"]),
              default="userpasswd", help="Supported authentication types for Snowflake")
@click.option("-c", "--scan-type", type=click.Choice(["deep", "shallow"]), default='shallow',
              help="Choose deep(scan data) or shallow(scan column names only)")
@click.option("--list-all", default=False, is_flag=True,
              help="List all columns. By default only columns with PII information is listed")
@click.option("-n", "--include-schema", multiple=True, help=schema_help_text)
@click.option("-N", "--exclude-schema", multiple=True, help=exclude_schema_help_text)
@click.option("-t", "--include-table", multiple=True, help=table_help_text)
@click.option("-T", "--exclude-table", multiple=True, help=exclude_table_help_text)
def cli(cxt, account, warehouse, database, user, password, okta_account_name, oauth_host, oauth_token,
        authenticator, scan_type, list_all, include_schema, exclude_schema, include_table, exclude_table):
    if authenticator == "userpasswd":
        if user is None or password is None:
            raise AttributeError("--user AND --password are required for user/password authentication")
    elif authenticator == "okta":
        if okta_account_name is None:
            raise AttributeError("--okta-account-name is required for OKTA authentication")
    elif authenticator == "oauth":
        if oauth_token is None or oauth_host is None or user is None:
            raise AttributeError("--oauth-token AND --oauth-host AND --user are required for OAUTH authentication")

    ns = Namespace(account=account,
                   warehouse=warehouse,
                   database=database,
                   user=user,
                   password=password,
                   authenticator=authenticator,
                   okta_account_name=okta_account_name,
                   oauth_token=oauth_token,
                   oauth_host=oauth_host,
                   scan_type=scan_type,
                   list_all=list_all,
                   catalog=cxt.obj['catalog'],
                   include_schema=include_schema,
                   exclude_schema=exclude_schema,
                   include_table=include_table,
                   exclude_table=exclude_table)

    SnowflakeExplorer.dispatch(ns)


class SnowflakeExplorer(Explorer):
    _catalog_query = """
        SELECT
            TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE
            TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
            AND DATA_TYPE = 'TEXT'
        ORDER BY table_schema, table_name, column_name
    """

    _sample_query_template = "select {column_list} from {schema_name}.{table_name} TABLESAMPLE BERNOULLI (10 ROWS)"

    def __init__(self, ns):
        super(SnowflakeExplorer, self).__init__(ns)
        self.account = ns.account
        self.warehouse = ns.warehouse
        self.database_connection = ns.database
        self.user = ns.user
        self.password = ns.password
        self.authenticator = ns.authenticator
        self.okta_account_name = ns.okta_account_name
        self.oauth_token = ns.oauth_token
        self.oauth_host = ns.oauth_host

    @classmethod
    def factory(cls, ns):
        logging.debug("Sqlite Factory entered")
        return SnowflakeExplorer(ns)

    def _get_connection_args(self):
        if self.authenticator == "userpasswd":
            return {
                "user": self.user,
                "password": self.password,
                "account": self.account,
                "warehouse": self.warehouse,
                "database": self.database_connection,
            }
        elif self.authenticator == "externalbrowser":
            return {
                "account": self.account,
                "warehouse": self.warehouse,
                "database": self.database_connection,
                "authenticator": self.authenticator
            }
        elif self.authenticator == "okta":
            return {
                "user": self.user,
                "password": self.password,
                "account": self.account,
                "warehouse": self.warehouse,
                "database": self.database_connection,
                "authenticator": "https://{}.okta.com/".format(self.okta_account_name)
            }
        else:
            return {
                "user": self.user,
                "host": self.oauth_host,
                "account": self.account,
                "warehouse": self.warehouse,
                "database": self.database_connection,
                "authenticator": "oauth",
                "token": self.oauth_token,
            }

    def _open_connection(self):
        return snowflake.connector.connect(**self._get_connection_args())

    def _get_catalog_query(self):
        return self._catalog_query

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls._sample_query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            schema_name=schema_name.get_name(),
            table_name=table_name.get_name()
        )
