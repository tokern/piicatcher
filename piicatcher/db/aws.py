import json
import logging
import tableprint

import pyathena

from piicatcher.db.explorer import Explorer


def dispatch(ns):
    logging.debug("AWS Dispatch entered")
    explorer = AthenaExplorer(ns.access_key, ns.secret_key,
                              ns.staging_dir, ns.region)

    if ns.scan_type is None or ns.scan_type == "deep":
        explorer.scan()
    else:
        explorer.shallow_scan()

    if ns.output_format == "ascii_table":
        headers = ["schema", "table", "column", "has_pii"]
        tableprint.table(explorer.get_tabular(ns.list_all), headers)
    elif ns.output_format == "json":
        print(json.dumps(explorer.get_dict(), sort_keys=True, indent=2))
    elif ns.output_format == "orm":
        from piicatcher.orm.models import Store
        Store.save_schemas(explorer)


def parser(sub_parsers):
    sub_parser = sub_parsers.add_parser("aws")

    sub_parser.add_argument("-a", "--access-key", required=True,
                            help="AWS Access Key ")
    sub_parser.add_argument("-s", "--secret-key",
                            help="AWS Secret Key")
    sub_parser.add_argument("-d", "--staging-dir",
                            help="S3 Staging Directory for Athena results")
    sub_parser.add_argument("-r", "--region",
                            help="AWS Region")

    sub_parser.add_argument("-c", "--scan-type", default='shallow',
                            choices=["deep", "shallow"],
                            help="Choose deep(scan data) or shallow(scan column names only)")

    sub_parser.add_argument("-o", "--output", default=None,
                            help="File path for report. If not specified, "
                                 "then report is printed to sys.stdout")
    sub_parser.add_argument("-f", "--output-format", choices=["ascii_table", "json", "orm"],
                            default="ascii_table",
                            help="Choose output format type")
    sub_parser.add_argument("--list-all", action="store_true", default=False,
                            help="List all columns. By default only columns with PII information is listed")
    sub_parser.set_defaults(func=dispatch)


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

    def __init__(self, access_key, secret_key, staging_dir, region_name):
        super(AthenaExplorer, self).__init__()
        self._access_key = access_key
        self._secret_key = secret_key
        self._staging_dir = staging_dir
        self._region_name = region_name

    def _open_connection(self):
        return pyathena.connect(aws_access_key_id=self._access_key,
                                aws_secret_access_key=self._secret_key,
                                s3_staging_dir=self._staging_dir,
                                region_name=self._region_name)

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
