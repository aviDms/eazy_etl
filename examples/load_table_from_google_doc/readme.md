### From Google Doc to PostgreSQL

Create a simple pipeline for loading data from a Google Doc to a database table in PostgreSQL.

Each ETL job must be a separate folder, containing Python and SQL scripts, as well as other
details or configuration such as .xml config files for Jenkins scheduler.

This folder has three files:

    * a Pyhton script used to run the ETL job
    * an SQL script used to create the target table
    * a readme file, serving as documentation


##### The Google Doc

Name: __BT Obligatiuni__
Sheet: __bteuroclasic__

| Date       | Activ net    | Numar investitori | Numar de unitati in circulatie |
|------------|--------------|-------------------|--------------------------------|
| 11/8/2010  | 1,499,538.47 | 2                 | 150,011.00                     |
| 12/29/2010 | 1,835,482.66 | 35                | 173,383.98                     |
| 1/18/2011  | 1,837,112.17 | 52                | 175,180.20                     |


##### Extract data using [gspread](https://github.com/burnash/gspread)

Simplest way to do this is by getting all records from the target spreadsheet:

    ```python
    gd = GoogleDoc.open('BT Obligatiuni')
    sheet = gd.worksheet('bteuroclasic')
    sheet.get_all_records(empty2zero=False, head=1)
    ```

The first line in the spreadsheet is handled as the header. The rest of the lines are fetched
as a list of key, value pairs: [{filed_name: cell_value}, ...]. This way it will be easier to
automatically construct an INSERT statement for loading each row to postgres.

    ```python
    [{'Ddate': '11/8/2010', 'Activ net': '1,499,538.47'}, {...}, ...]
    ```

Since data in google docs can be quite messy, there needs to be a clean up step. Also, data must
be passed as a generator to the __Table.insert()__ method. Hence, two functions need to be defied,
first to create a stream of records and a second one to handle the cleanup steps.

    ```python
    def extract(sheet):
    for records in sheet.get_all_records(empty2zero=False, head=1):
        yield records
    ```

    ```python
    def transform(records):
    for row in records:
        new_row = dict()
        value = row.pop('Date', None)
        if value:
            try:
                new_row['report_date'] = datetime.datetime.strptime(value, '%m/%d/%Y')
            except ValueError:
                new_row['report_date'] = datetime.datetime.strptime(value, '%d-%b-%Y')
            except Exception as e:
                raise e
        ...
        yield new_row
    ```

##### The PostgreSQL Table

Loading data to PostgreSQL is handled by the [Table]() object.
Its .insert(https://github.com/aviDms/eazy_etl/blob/master/postgresql.py) method accepts
a stream of key, value pairs and dynamically creates the INSERT statement necessary to load
the data to PostgreSQL using the [psycopg2](http://initd.org/psycopg/docs/index.html) module.

    ```python
    bt_euro_classic = Table(name='bt_euro_classic', schema='public', dsn=local)
    bt_euro_classic.create(script_path=create_table_script, drop_if_exists=False)
    bt_euro_classic.insert(transform(extract(sheet)), conflict_on='report_date')
    ```
There are 3 steps defining the data pipeline:

1. Initiate the Table object. The table does not have to be already created,
it can be created later.

* name: name of the table as string
* schema: database schema, where the table is stored, as string
* dsn: postgresql connection string (should not be stored in the script but rather imported
from a location outside of the repository)

2. Create the table if it does not exit, do nothing if it does. The SQL script should have the
following content, where the {schema} and {table} wildcards will be replaced by the parameters
passed during Table()'s initialization.

    ```sql
    CREATE TABLE {schema}.{table} (
        id SERIAL NOT NULL,
        report_date DATE,
        nb_investors INTEGER,
        volume INTEGER,
        total_value DOUBLE PRECISION,
        CONSTRAINT pk_bt_euro_classic PRIMARY KEY (id),
        CONSTRAINT unique_date UNIQUE (report_date)
    );
    ```

Having a primary key is good practice. The 'report_date' column is used to decide if the row
must be appended to the table or updated. Hence it is mandatory that this column is part of
a unique constraint. It can also be set as the primary key in this case, but it will make a
terrible primary key due the the fact that it is a date field.

3. Extract, Transform, Load

Last line of code in the the main function, is basically creating the ETL pipeline.