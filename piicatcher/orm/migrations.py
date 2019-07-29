from playhouse.migrate import *


class Migrations:
    @classmethod
    def migrate(cls, host, port, user, password):
        mysqlDb = MySQLDatabase('tokern',
                                host=host,
                                port=port,
                                user=user,
                                password=password)
        migrator = MySQLMigrator(mysqlDb)

        with mysqlDb.atomic():
            migrate(
            )