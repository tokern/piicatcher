import unittest
import datetime

from dateutil.tz import tzlocal

from piicatcher.store.glue import GlueStore
from tests.test_models import MockExplorer


class PiiTable(unittest.TestCase):
    def test_no_pii(self):
        pii_table = GlueStore.get_pii_table(MockExplorer.get_no_pii_table())
        self.assertEqual({}, pii_table)

    def test_partial_pii(self):
        pii_table = GlueStore.get_pii_table(MockExplorer.get_partial_pii_table())
        self.assertEqual({'a': ['PiiTypes.PHONE']}, pii_table)

    def test_full_pii(self):
        pii_table = GlueStore.get_pii_table(MockExplorer.get_full_pii_table())
        self.assertEqual({'a': ['PiiTypes.PHONE'], 'b': ['PiiTypes.ADDRESS', 'PiiTypes.LOCATION']}, pii_table)


class UpdateParameters(unittest.TestCase):
    def test_empty_table(self):
        columns = [{
            'Name': 'dispatching_base_num', "Type": "string"
        }, {
            "Name": "pickup_datetime", "Type": "string"
        }, {
            "Name": "dropoff_datetime", "Type": "string"
        }, {
            "Name": "pulocationid", "Type": "bigint"
        }, {
            "Name": "dolocationid", "Type": "bigint"
        }, {
            "Name": "sr_flag", "Type": "bigint"
        }, {
            "Name": "hvfhs_license_num", "Type": "string"
        }
        ]
        updated, is_updated = GlueStore.update_column_parameters(columns, {})
        self.assertFalse(is_updated)
        self.assertEqual(columns, updated)

    def test_for_update(self):
        columns = [
            {'Name': 'locationid', 'Type': 'bigint'}, {'Name': 'borough', 'Type': 'string'},
            {'Name': 'zone', 'Type': 'string'}, {'Name': 'service_zone', 'Type': 'string'}
        ]

        expected = [
            {'Name': 'locationid', 'Type': 'bigint'},
            {'Name': 'borough', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}},
            {'Name': 'zone', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}},
            {'Name': 'service_zone', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}}
        ]

        pii_table = {
            'borough': ['PiiTypes.ADDRESS'],
            'zone': ['PiiTypes.ADDRESS'],
            'service_zone': ['PiiTypes.ADDRESS']
        }

        updated, is_updated = GlueStore.update_column_parameters(columns, pii_table)
        self.assertTrue(is_updated)
        self.assertEqual(expected, columns)

    def test_param_no_update(self):
        columns = [
            {'Name': 'locationid', 'Type': 'bigint', 'Parameters': {'a': 'b'}}, {'Name': 'borough', 'Type': 'string'},
        ]

        updated, is_updated = GlueStore.update_column_parameters(columns, {})
        self.assertFalse(is_updated)
        self.assertEqual(columns, updated)

    def test_param_update(self):
        columns = [
            {'Name': 'locationid', 'Type': 'bigint', }, {'Name': 'borough', 'Type': 'string', 'Parameters': {'a': 'b'}},
        ]

        pii_table = {
            'borough': ['PiiTypes.ADDRESS'],
        }

        expected = [
            {'Name': 'locationid', 'Type': 'bigint'},
            {'Name': 'borough', 'Type': 'string', 'Parameters': {'a': 'b', 'PII': 'PiiTypes.ADDRESS'}}
        ]

        updated, is_updated = GlueStore.update_column_parameters(columns, pii_table)
        self.assertTrue(is_updated)
        self.assertEqual(expected, columns)


class TableParams(unittest.TestCase):
    def test_update(self):
        updated_columns = [
            {'Name': 'locationid', 'Type': 'bigint'},
            {'Name': 'borough', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}},
            {'Name': 'zone', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}},
            {'Name': 'service_zone', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}}
        ]

        table_params = {
            'Name': 'csv_misc', 'DatabaseName': 'taxidata', 'Owner': 'owner',
                             'CreateTime': datetime.datetime(2019, 12, 9, 16, 12, 43, tzinfo=tzlocal()),
                             'UpdateTime': datetime.datetime(2019, 12, 9, 16, 12, 43, tzinfo=tzlocal()),
                             'LastAccessTime': datetime.datetime(2019, 12, 9, 16, 12, 43, tzinfo=tzlocal()),
                             'Retention': 0,
                             'StorageDescriptor': {'Columns': [
                                 {'Name': 'locationid', 'Type': 'bigint'},
                                 {'Name': 'borough', 'Type': 'string'},
                                 {'Name': 'zone', 'Type': 'string'},
                                 {'Name': 'service_zone', 'Type': 'string'}
                             ],
                                 'Location': 's3://nyc-tlc/misc/',
                                 'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                                 'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                                 'Compressed': False, 'NumberOfBuckets': -1,
                                 'SerdeInfo': {
                                     'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                                     'Parameters': {'field.delim': ','}}, 'BucketColumns': [],
                                 'SortColumns': [],
                                 'Parameters': {
                                     'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0',
                                     'UPDATED_BY_CRAWLER': 'TaxiCrawler', 'areColumnsQuoted': 'false',
                                     'averageRecordSize': '36', 'classification': 'csv', 'columnsOrdered': 'true',
                                     'compressionType': 'none', 'delimiter': ',',
                                     'exclusions': '["s3://nyc-tlc/misc/*foil*","s3://nyc-tlc/misc/shared*",'
                                                   '"s3://nyc-tlc/misc/uber*","s3://nyc-tlc/misc/*.html",'
                                                   '"s3://nyc-tlc/misc/*.zip","s3://nyc-tlc/misc/FOIL_*"]',
                                     'objectCount': '1', 'recordCount': '342', 'sizeKey': '12322',
                                     'skip.header.line.count': '1', 'typeOfData': 'file'},
                                 'StoredAsSubDirectories': False
                             },
                             'PartitionKeys': [], 'TableType': 'EXTERNAL_TABLE', 'Parameters': {
                                'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0',
                                'UPDATED_BY_CRAWLER': 'TaxiCrawler', 'areColumnsQuoted': 'false',
                                'averageRecordSize': '36', 'classification': 'csv', 'columnsOrdered': 'true',
                                'compressionType': 'none', 'delimiter': ',',
                                'exclusions':
                                    '["s3://nyc-tlc/misc/*foil*","s3://nyc-tlc/misc/shared*","s3://nyc-tlc/misc/uber*",'
                                    '"s3://nyc-tlc/misc/*.html","s3://nyc-tlc/misc/*.zip","s3://nyc-tlc/misc/FOIL_*"]',
                                'objectCount': '1', 'recordCount': '342', 'sizeKey': '12322',
                                'skip.header.line.count': '1', 'typeOfData': 'file'},
                             'CreatedBy':
                                 'arn:aws:sts::172965158661:assumed-role/LakeFormationWorkflowRole/AWS-Crawler',
                             'IsRegisteredWithLakeFormation': False
                             }

        expected_table_params = {
            'Name': 'csv_misc', 'Owner': 'owner',
            'LastAccessTime': datetime.datetime(2019, 12, 9, 16, 12, 43, tzinfo=tzlocal()),
            'Retention': 0,
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'locationid', 'Type': 'bigint'},
                    {'Name': 'borough', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}},
                    {'Name': 'zone', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}},
                    {'Name': 'service_zone', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}}
                ],
                'Location': 's3://nyc-tlc/misc/', 'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'Compressed': False, 'NumberOfBuckets': -1,
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                    'Parameters': {'field.delim': ','}}, 'BucketColumns': [], 'SortColumns': [],
                'Parameters': {
                    'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0',
                    'UPDATED_BY_CRAWLER': 'TaxiCrawler', 'areColumnsQuoted': 'false', 'averageRecordSize': '36',
                    'classification': 'csv', 'columnsOrdered': 'true', 'compressionType': 'none', 'delimiter': ',',
                    'exclusions': '["s3://nyc-tlc/misc/*foil*","s3://nyc-tlc/misc/shared*","s3://nyc-tlc/misc/uber*",'
                                  '"s3://nyc-tlc/misc/*.html","s3://nyc-tlc/misc/*.zip","s3://nyc-tlc/misc/FOIL_*"]',
                    'objectCount': '1', 'recordCount': '342', 'sizeKey': '12322', 'skip.header.line.count': '1',
                    'typeOfData': 'file'
                },
                'StoredAsSubDirectories': False
            },
            'PartitionKeys': [], 'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0',
                'UPDATED_BY_CRAWLER': 'TaxiCrawler', 'areColumnsQuoted': 'false', 'averageRecordSize': '36',
                'classification': 'csv', 'columnsOrdered': 'true', 'compressionType': 'none', 'delimiter': ',',
                'exclusions': '["s3://nyc-tlc/misc/*foil*","s3://nyc-tlc/misc/shared*","s3://nyc-tlc/misc/uber*",'
                              '"s3://nyc-tlc/misc/*.html","s3://nyc-tlc/misc/*.zip","s3://nyc-tlc/misc/FOIL_*"]',
                'objectCount': '1', 'recordCount': '342', 'sizeKey': '12322', 'skip.header.line.count': '1',
                'typeOfData': 'file'
            }
        }

        updated_table_params = GlueStore.update_table_params(table_params, updated_columns)
        self.assertEqual(updated_table_params, expected_table_params)


if __name__ == '__main__':
    unittest.main()
