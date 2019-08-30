from unittest import TestCase
from piicatcher.command_line import get_parser

from argparse import ArgumentParser


class ErrorRaisingArgumentParser(ArgumentParser):
    def error(self, message):
        raise ValueError(message)  # reraise an error


class TestParser(TestCase):
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

