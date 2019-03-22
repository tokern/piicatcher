class NamedObject:
    def __init__(self, name):
        self.name = name

    def get_name(self):
        return self.name


class Schema(NamedObject):
    def __init__(self, name):
        super(Schema, self).__init__(name)
        self.tables = []

    def add(self, table):
        self.tables.append(table)

    def get_tables(self):
        return self.tables


class Table(NamedObject):
    def __init__(self, name):
        super(Table, self).__init__(name)
        self.columns = []

    def add(self, col):
        self.columns.append(col)

    def get_columns(self):
        return self.columns


class Column(NamedObject):
    def __init__(self, name):
        super(Column, self).__init__(name)


