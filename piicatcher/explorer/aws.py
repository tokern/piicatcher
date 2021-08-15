import logging
from argparse import Namespace
from typing import Dict

import click
import pyathena

from piicatcher.catalog.glue import GlueStore
from piicatcher.explorer.databases import (
    exclude_schema_help_text,
    exclude_table_help_text,
    schema_help_text,
    table_help_text,
)
from piicatcher.explorer.explorer import Explorer


@click.command("aws")
@click.pass_context
@click.option("-a", "--access-key", required=True, help="AWS Access Key ")
@click.option("-s", "--secret-key", required=True, help="AWS Secret Key")
@click.option(
    "-d", "--staging-dir", required=True, help="S3 Staging Directory for Athena results"
)
@click.option("-r", "--region", required=True, help="AWS Region")
@click.option(
    "-c",
    "--scan-type",
    default="shallow",
    type=click.Choice(["deep", "shallow"]),
    help="Choose deep(scan data) or shallow(scan column names only)",
)
@click.option(
    "--list-all",
    default=False,
    is_flag=True,
    help="List all columns. By default only columns with PII information is listed",
)
@click.option("-n", "--schema", multiple=True, help=schema_help_text)
@click.option("-N", "--exclude-schema", multiple=True, help=exclude_schema_help_text)
@click.option("-t", "--table", multiple=True, help=table_help_text)
@click.option("-T", "--exclude-table", multiple=True, help=exclude_table_help_text)
# pylint: disable=too-many-arguments
def cli(
    cxt,
    access_key,
    secret_key,
    staging_dir,
    region,
    scan_type,
    list_all,
    schema,
    exclude_schema,
    table,
    exclude_table,
):
    args = Namespace(
        access_key=access_key,
        secret_key=secret_key,
        staging_dir=staging_dir,
        region=region,
        scan_type=scan_type,
        list_all=list_all,
        include_schema=schema,
        exclude_schema=exclude_schema,
        include_table=table,
        exclude_table=exclude_table,
        catalog=cxt.obj["catalog"],
    )

    AthenaExplorer.dispatch(args)


class AthenaExplorer(Explorer):
    _catalog_query = """
            SELECT
                table_schema, table_name, column_name
            FROM
                information_schema.columns
            WHERE data_type LIKE '%char%' AND
                table_schema != 'information_schema'
            ORDER BY table_schema, table_name, ordinal_position
        """

    _sample_query_template = "select {column_list} from {schema_name}.{table_name} TABLESAMPLE BERNOULLI(5) limit 10"
    _select_query_template = (
        "select {column_list} from {schema_name}.{table_name} limit 10"
    )
    _count_query = "select count(*) from {schema_name}.{table_name}"

    def __init__(self, ns):
        super(AthenaExplorer, self).__init__(ns)
        self.config = ns

    @property
    def type(self) -> str:
        return "athena"

    @property
    def connection_parameters(self) -> Dict[str, str]:
        return dict(
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            s3_staging_dir=self.config.staging_dir,
            region_name=self.config.region,
        )

    @classmethod
    def factory(cls, ns):
        logging.debug("AWS Dispatch entered")
        explorer = AthenaExplorer(ns)
        return explorer

    @classmethod
    def output(cls, ns, explorer):
        if ns.catalog["format"] == "glue":
            GlueStore.save_schemas(explorer)
        else:
            super(AthenaExplorer, cls).output(ns, explorer)

    def _open_connection(self):
        return pyathena.connect(
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            s3_staging_dir=self.config.staging_dir,
            region_name=self.config.region,
        )

    def _get_catalog_query(self):
        return self._catalog_query

    @classmethod
    def _get_select_query(cls, schema_name, table_name, column_list):
        return cls._select_query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            table_name=table_name.get_name(),
            schema_name=schema_name.get_name(),
        )

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls._sample_query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            table_name=table_name.get_name(),
            schema_name=schema_name.get_name(),
        )

    @classmethod
    def _get_count_query(cls, schema_name, table_name):
        return cls._count_query.format(
            table_name=table_name.get_name(), schema_name=schema_name.get_name()
        )
