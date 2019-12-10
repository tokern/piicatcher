from unittest import TestCase
import yaml

config_file = """
store:
  host: a_host
  port: 3306
  user: a_user
  password: a_password
"""


class TestConfigFile(TestCase):
    def testOrmConfig(self):
        orm = yaml.full_load(config_file)['store']
        self.assertEqual("a_host", orm['host'])
        self.assertEqual(3306, orm["port"])
        self.assertEqual("a_user", orm['user'])
        self.assertEqual("a_password", orm['password'])
