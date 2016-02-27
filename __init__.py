from .client import Source, Query
from .helpers import coroutine, read_text_file

# from .postgresql_legacy import Table
from .postgresql import Table

from .google_docs import GoogleDoc

__all__ = [Source, Query, Table, GoogleDoc, coroutine, read_text_file]

__doc__ = """
Eazy ETL

* USE the postgres_legacy module for PostgreSQL < 9.5

"""
