import psycopg2
from helpers import read_sql


class Table(object):
    """ Database table - The Load step in ETL """

    def __init__(self, name, schema, dsn=None):
        """ """
        self.name = name
        self.schema = schema
        self.dsn = dsn

    def exists(self):
        table_list_stmt = "SELECT COUNT(1)=1 " \
                          "FROM information_schema.tables " \
                          "WHERE table_schema='%s' " \
                          "AND table_name='%s';" % (self.schema, self.name)
        with psycopg2.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(table_list_stmt)
                resp = cursor.fetchone()
        assert isinstance(resp[0], bool)
        return resp[0]

    def create(self, sql=None, script_path=None, drop_if_exists=False):
        """
        TODO: add **kargs to create stmt
        """
        drop_stmt = "DROP TABLE IF EXISTS %s.%s;" % (self.schema, self.name)
        if sql:
            create_stmt = sql
        elif script_path:
            create_stmt = read_sql(path=script_path, schema=self.schema, table=self.name)
        else:
            exit('Create SQL statement is missing.')

        with psycopg2.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                if not self.exists():
                    cursor.execute(create_stmt)
                    connection.commit()
                elif self.exists() and drop_if_exists:
                    cursor.execute(drop_stmt)
                    cursor.execute(create_stmt)
                    connection.commit()
                else:
                    pass

    def truncate(self):
        truncate_stmt = "TRUNCATE %s.%s;" % (self.schema, self.name)
        with psycopg2.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(truncate_stmt)
                connection.commit()

    def vacuum(self, full=True, analyze=True):
        raise NotImplementedError
        # truncate_stmt = "VACUUM FULL ANALYZE %s.%s;" % (self.schema, self.name)
        # with psycopg2.connect(self.dsn) as connection:
        #     with connection.cursor() as cursor:
        #         cursor.execute(truncate_stmt)
        #         connection.commit()

    def insert(self, rows, conflict_on=None):
        """ INSERT rows into existing Postgresql Table.
        :param rows: python generator
        :param conflict_on: name of the column, string

        The conflict_on column must have unique values.
        It is used as a rule by the INSERT command to determine
        if the row needs to be appended to the table (e.g. INSERT)
        or if the row already exists and the values need to be
        updated (e.g. UPDATE).
        """
        first_row = next(rows)
        keys = first_row.keys()

        if conflict_on:
            stmt = self.get_upsert_stmt(columns=keys, constraint=conflict_on)
        else:
            stmt = self.get_insert_stmt(columns=keys)

        with psycopg2.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(stmt, list(first_row.values()))
                for row in rows:
                    cursor.execute(stmt, list(row.values()))
            connection.commit()

    def get_insert_stmt(self, columns):
        """ Generate INSERT statement using a list of columns. """
        insert_stmt = "INSERT INTO {table} ({columns}) VALUES ({values})"
        return insert_stmt.format(
            table='.'.join([self.schema, self.name]),
            columns=', '.join(columns),
            values=', '.join(['%s' for c in columns])
        )

    def get_upsert_stmt(self, columns, constraint):
        """ Generate INSERT ON CONFLICT statement using a list of columns.
        :param columns: list of strings
        :param constraint: string
        """
        merge_stmt = "INSERT INTO {table} ({columns}) VALUES ({values}) "\
                     "ON CONFLICT ({constraint}) DO UPDATE "\
                     "SET ({columns}) = ({excluded_values});"
        return merge_stmt.format(
            table='.'.join([self.schema, self.name]),
            columns=', '.join(columns),
            values=', '.join(['%s' for c in columns]),
            constraint=constraint,
            excluded_values=', '.join(['EXCLUDED.%s' % c for c in columns])
        )