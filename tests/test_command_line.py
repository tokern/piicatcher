from unittest import TestCase
from piicatcher.command_line import get_parser

from argparse import ArgumentParser


class ErrorRaisingArgumentParser(ArgumentParser):
    def error(self, message):
        raise ValueError(message)  # reraise an error


class TestDbParser(TestCase):
    def test_host_required(self):
        with self.assertRaises(ValueError):
            get_parser(ErrorRaisingArgumentParser).parse_args(["db"])

    def test_host(self):
        ns = get_parser().parse_args(["db", "-s", "connection_string"])
        self.assertEqual("connection_string", ns.host)

    def test_port(self):
        ns = get_parser().parse_args(["db", "-s", "connection_string", "-R", "6032"])
        self.assertEqual("connection_string", ns.host)
        self.assertEqual("6032", ns.port)

    def test_host_user_password(self):
        ns = get_parser().parse_args(["db", "-s", "connection_string", "-u", "user", "-p", "passwd"])
        self.assertEqual("connection_string", ns.host)
        self.assertEqual("user", ns.user)
        self.assertEqual("passwd", ns.password)

    def test_default_console(self):
        ns = get_parser().parse_args(["db", "-s", "connection_string"])
        self.assertIsNone(ns.output)
        self.assertEqual("ascii_table", ns.output_format)

    def test_default_scan_type(self):
        ns = get_parser().parse_args(["db", "-s", "connection_string"])
        self.assertIsNone(ns.scan_type)

    def test_deep_scan_type(self):
        ns = get_parser().parse_args(["db", "-s", "connection_string", "-c", "deep"])
        self.assertEqual("deep", ns.scan_type)

    def test_default_scan_type(self):
        ns = get_parser().parse_args(["db", "-s", "connection_string", "-c", "shallow"])
        self.assertEqual("shallow", ns.scan_type)


class TestSqliteParser(TestCase):
    def test_path_required(self):
        with self.assertRaises(ValueError):
            get_parser(ErrorRaisingArgumentParser).parse_args(["sqlite"])

    def test_host(self):
        ns = get_parser().parse_args(["sqlite", "-s", "connection_string"])
        self.assertEqual("connection_string", ns.path)

    def test_default_console(self):
        ns = get_parser().parse_args(["sqlite", "-s", "connection_string"])
        self.assertIsNone(ns.output)
        self.assertEqual("ascii_table", ns.output_format)

    def test_default_scan_type(self):
        ns = get_parser().parse_args(["sqlite", "-s", "connection_string"])
        self.assertIsNone(ns.scan_type)

    def test_deep_scan_type(self):
        ns = get_parser().parse_args(["sqlite", "-s", "connection_string", "-c", "deep"])
        self.assertEqual("deep", ns.scan_type)

    def test_default_scan_type(self):
        ns = get_parser().parse_args(["sqlite", "-s", "connection_string", "-c", "shallow"])
        self.assertEqual("shallow", ns.scan_type)


class TestAWSParser(TestCase):
    def test_access_key_required(self):
        with self.assertRaises(ValueError):
            get_parser(ErrorRaisingArgumentParser).parse_args(["aws", "-s", "SSSS", "-d", "s3://dir", "-r", "us-east"])

    def test_secret_key_required(self):
        with self.assertRaises(ValueError):
            get_parser(ErrorRaisingArgumentParser).parse_args(["aws", "-a", "AAAA", "-d", "s3://dir", "-r", "us-east"])

    def test_staging_dir_required(self):
        with self.assertRaises(ValueError):
            get_parser(ErrorRaisingArgumentParser).parse_args(["aws", "-a", "AAAA", "-s", "SSSS", "-r", "us-east"])

    def test_region_required(self):
        with self.assertRaises(ValueError):
            get_parser(ErrorRaisingArgumentParser).parse_args(["aws", "-a", "AAAA", "-s", "SSSS", "-d", "s3://dir"])

    def test_host_user_password(self):
        ns = get_parser().parse_args(["aws", "-a", "AAAA", "-s", "SSSS", "-d", "s3://dir", "-r", "us-east"])
        self.assertEqual("AAAA", ns.access_key)
        self.assertEqual("SSSS", ns.secret_key)
        self.assertEqual("s3://dir", ns.staging_dir)
        self.assertEqual("us-east", ns.region)

