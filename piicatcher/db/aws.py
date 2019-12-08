import logging

import pyathena

from piicatcher.db.explorer import Explorer


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

    @classmethod
    def factory(cls, ns):
        logging.debug("AWS Dispatch entered")
        explorer = AthenaExplorer(ns.access_key, ns.secret_key,
                                  ns.staging_dir, ns.region)
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

        cls.scan_options(sub_parser)
        sub_parser.set_defaults(func=AthenaExplorer.dispatch)

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
