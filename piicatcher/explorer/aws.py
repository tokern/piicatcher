import logging
from argparse import Namespace

import click
import pyathena

from piicatcher.explorer.explorer import Explorer
from piicatcher.store.glue import GlueStore


@click.command('aws')
@click.option("-a", "--access-key", required=True, help="AWS Access Key ")
@click.option("-s", "--secret-key", required=True, help="AWS Secret Key")
@click.option("-d", "--staging-dir", required=True, help="S3 Staging Directory for Athena results")
@click.option("-r", "--region", required=True, help="AWS Region")
@click.option("-f", "--output-format", type=click.Choice(["ascii_table", "json", "db", "glue"]),
              default="ascii_table",
              help="Choose output format type")
@click.option("-c", "--scan-type", default='shallow',
              type=click.Choice(["deep", "shallow"]),
              help="Choose deep(scan data) or shallow(scan column names only)")
@click.option("-o", "--output", default=None, type=click.File(),
              help="File path for report. If not specified, "
                   "then report is printed to sys.stdout")
@click.option("--list-all", default=False, is_flag=True,
              help="List all columns. By default only columns with PII information is listed")
def cli(access_key, secret_key, staging_dir, region, output_format, scan_type, output, list_all):
    ns = Namespace(access_key=access_key,
                   secret_key=secret_key,
                   staging_dir=staging_dir,
                   region=region,
                   output_format=output_format,
                   scan_type=scan_type,
                   output=output,
                   list_all=list_all)
    logging.debug(vars(ns))
    Explorer.dispatch(ns)


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
    _select_query_template = "select {column_list} from {schema_name}.{table_name} limit 10"
    _count_query = "select count(*) from {schema_name}.{table_name}"

    def __init__(self, ns):
        super(AthenaExplorer, self).__init__(ns)
        self.config = ns

    @classmethod
    def factory(cls, ns):
        logging.debug("AWS Dispatch entered")
        explorer = AthenaExplorer(ns)
        return explorer

    @classmethod
    def output(cls, ns, explorer):
        if ns.output_format == "glue":
            GlueStore.save_schemas(explorer)
        else:
            super(AthenaExplorer, cls).output(ns, explorer)

    def _open_connection(self):
        return pyathena.connect(aws_access_key_id=self.config.access_key,
                                aws_secret_access_key=self.config.secret_key,
                                s3_staging_dir=self.config.staging_dir,
                                region_name=self.config.region)

    def _get_catalog_query(self):
        return self._catalog_query

    @classmethod
    def _get_select_query(cls, schema_name, table_name, column_list):
        return cls._select_query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            table_name=table_name.get_name(),
            schema_name=schema_name.get_name()
        )

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls._sample_query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            table_name=table_name.get_name(),
            schema_name=schema_name.get_name()
        )

    @classmethod
    def _get_count_query(cls, schema_name, table_name):
        return cls._count_query.format(
            table_name=table_name.get_name(),
            schema_name=schema_name.get_name()
        )
