import psycopg2
import datetime
from .helpers import coroutine, read_sql


class Table(object):
    def __init__(self, name, schema, dsn=None):
        self.name = name
        self.schema = schema
        self.dsn = dsn

    @coroutine
    def insert(self, conflict=None):
        index = set()
        if conflict:
            index = self.get_unique_key_values(conflict)

        with psycopg2.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                try:
                    while True:
                        row = (yield)
                        keys = row.keys()
                        if conflict and row[conflict] in index:
                            stmt = self.get_update_stmt(columns=keys, unique_key=conflict, key_value=row[conflict])
                        else:
                            stmt = self.get_insert_stmt(columns=keys)
                        try:
                            cursor.execute(stmt, list(row.values()))
                        except psycopg2.ProgrammingError as e:
                            print('SQL:\n', stmt, row.values())
                            raise psycopg2.ProgrammingError(e)
                except GeneratorExit:
                    connection.commit()

    def truncate(self):
        truncate_stmt = "TRUNCATE %s.%s" % (self.schema, self.name)
        with psycopg2.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(truncate_stmt)
                connection.commit()

    def get_insert_stmt(self, columns):
        insert_stmt = "INSERT INTO {table} ({columns}) VALUES ({values});"
        return insert_stmt.format(
            table='.'.join([self.schema, self.name]),
            columns=', '.join(columns),
            values=', '.join(['%s' for _ in columns])
        )

    def get_update_stmt(self, columns, unique_key, key_value):
        update_stmt = "UPDATE {table} SET ({columns}) = ({values}) WHERE {key} = {key_value};"
        return update_stmt.format(
            table='.'.join([self.schema, self.name]),
            columns=', '.join(columns),
            values=', '.join(['%s' for _ in columns]),
            key=unique_key,
            key_value=key_value
        )

    def get_unique_key_values(self, unique_key):
        res = set()
        select_stmt = "SELECT {key} FROM {table};"
        select_stmt = select_stmt.format(key=unique_key, table='.'.join([self.schema, self.name]))
        with psycopg2.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(select_stmt)
                for row in cursor:
                    res.add(int(row[0]))
        return res

    def get_max_updated_at(self, platform):
        stmt = "SELECT MAX(updated_at) FROM %s.%s WHERE platform='%s'" % (
            self.schema, self.name, platform
        )
        with psycopg2.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(stmt)
                rows = cursor.fetchall()
        if len(rows) == 1:
            value = rows[0][0]
            if value is None:
                return datetime.datetime(year=2015, month=12, day=1)
            else:
                return value
        else:
            exit('Could not get timestamp from table %s.%s.' % self.schema, self.name)

    def create(self, sql=None, script_path=None):
        if sql:
            create_stmt = sql
        elif script_path:
            create_stmt = read_sql(path=script_path, schema=self.schema, table=self.name)
        else:
            exit('Create SQL statement is missing.')

        with psycopg2.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(create_stmt)
                connection.commit()

    def count(self):
        stmt = "SELECT count(1) FROM %s.%s;" % (self.schema, self.name)
        with psycopg2.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(stmt)
                rows = cursor.fetchall()
        if len(rows) == 1:
            value = rows[0][0]
            return value
        else:
            exit('Could not count from table %s.%s.' % self.schema, self.name)