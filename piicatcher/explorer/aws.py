import logging

import pyathena

from piicatcher.explorer.explorer import Explorer
from piicatcher.store.glue import GlueStore


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
    def parser(cls, sub_parsers):
        sub_parser = sub_parsers.add_parser("aws")

        sub_parser.add_argument("-a", "--access-key", required=True,
                                help="AWS Access Key ")
        sub_parser.add_argument("-s", "--secret-key", required=True,
                                help="AWS Secret Key")
        sub_parser.add_argument("-d", "--staging-dir", required=True,
                                help="S3 Staging Directory for Athena results")
        sub_parser.add_argument("-r", "--region", required=True,
                                help="AWS Region")
        sub_parser.add_argument("-f", "--output-format", choices=["ascii_table", "json", "db", "glue"],
                                default="ascii_table",
                                help="Choose output format type")

        cls.scan_options(sub_parser)
        sub_parser.set_defaults(func=AthenaExplorer.dispatch)

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
