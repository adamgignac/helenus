"""
Schemaless Postgres connections.

Get the flexibility of Mongo with the
ACID-compliance and SQL queries of Postgres.
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

logging.basicConfig(level=logging.INFO)


class Helenus(object):
    def __init__(self, username=None, password=None,
                 database=None, host=None):
        self.tables = {}
        conn_str = "user={} password={} dbname={} host={}"
        conn_str = conn_str.format(username, password, database, host)
        self.connection = psycopg2.connect(conn_str)
        logging.info("Connection initialized")

    def table(self, name):
        self.tables[name] = self.tables.get(name, Table(self.connection, name))
        return self.tables[name]

    def close(self):
        logging.info("Closing connection")
        self.connection.close()


class Table(object):

    type_mapping = {
        str: 'varchar',
        int: 'bigint',
        float: 'numeric',
        list: 'json',
        dict: 'json'
    }

    def __init__(self, connection, name):
        logging.info("Creating new table object for {}".format(name))
        self.connection = connection
        self.table_name = name
        if self._exists():
            logging.info("Table already exists")
        else:
            logging.info("Creating table")
            cursor = self.connection.cursor()
            cursor.execute("CREATE TABLE {} ()".format(self.table_name))
            self.connection.commit()
        logging.info("Table contains the following columns: %s",
                     self.columns())

    def _exists(self):
        stmt = """
            SELECT EXISTS(
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema='public' AND table_name='%s'
            )""" % self.table_name
        cursor = self.connection.cursor()
        cursor.execute(stmt)
        res = cursor.fetchone()
        return res[0]

    def columns(self):
        stmt = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name='%s'
        """ % self.table_name
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(stmt)
        res = cursor.fetchall()
        return {i['column_name']: i['data_type'] for i in res}

    def add_column(self, name, type):
        stmt = """
            ALTER TABLE %s
            ADD COLUMN IF NOT EXISTS %s %s
        """ % (self.table_name, name, type)
        cursor = self.connection.cursor()
        cursor.execute(stmt)

    def insert(self, obj):
        stmt = "INSERT INTO {} ".format(self.table_name)
        for key, value in obj.items():
            logging.debug("Key: %s", key)
            logging.debug("Value: %s (%s)", value, type(value))
            if key not in self.columns():
                self.add_column(key, self.type_mapping[type(value)])
            # Insert
        stmt += "(" + ",".join(obj.keys()) + ")"
        stmt += " VALUES "
        stmt += "(" + ",".join("%({})s".format(field) for field in obj.keys()) + ")"
        logging.debug(stmt)
        cursor = self.connection.cursor()
        _cols = self.columns()
        cursor.execute(stmt, {
            k: (json.dumps(v) if _cols[k] == 'json' else v)
            for k, v in obj.items()
        })
        self.connection.commit()

    def query(self, *args):
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        stmt = "SELECT * FROM %s" % self.table_name
        if all(isinstance(f, Field) for f in args):
            if len(args):
                stmt += " WHERE "
                stmt += " AND ".join("{} {} %({})s".format(f.name, f.op, f.name) for f in args)
            fmt_values = {f.name: f.value for f in args}
            logging.debug("Query: %s", stmt)
            logging.debug("Arguments: %s", fmt_values)
            cursor.execute(stmt, fmt_values)
        elif len(args) == 1 and isinstance(args[0], str):
            stmt += " WHERE " + args[0]
            cursor.execute(stmt)
        return cursor.fetchall()

    def truncate(self):
        cursor = self.connection.cursor()
        cursor.execute("TRUNCATE TABLE {}".format(self.table_name))
        self.connection.commit()


class Field(object):
    def __init__(self, name):
        self.name = name
        self.value = None
        self.op = None

    def equals(self, value):
        self.value = value
        self.op = "="
        return self

    def exists(self):
        self.value = ""
        self.op = "is not null and '' ="
        return self

    def greater_than(self, value):
        self.value = value
        self.op = ">"
        return self

    def less_than(self, value):
        self.value = value
        self.op = "<"
        return self

    def greater_or_equal(self, value):
        self.value = value
        self.op = ">="
        return self

    def less_or_equal(self, value):
        self.value = value
        self.op = "<="
        return self


if __name__ == '__main__':
    connection = Helenus(username='postgres', password='postgres',
                         host='localhost', database='helenus')
    table = connection.table("test")
    table.truncate()
    table.insert({"integer": 1, "floating_point": 1.3, "string": "stuff"})
    print(table.query())
    print(table.query(Field("floating_point").exists()))
    print(table.query(Field("floating_point").less_than(2)))
    print(table.query(Field("integer").equals(1),
                      Field('string').equals('stuff')))
    table.insert({"list": ['one, two']})
    print(table.query(Field("list").exists()))
    print(table.query("string = 'stuff'"))
    connection.close()
