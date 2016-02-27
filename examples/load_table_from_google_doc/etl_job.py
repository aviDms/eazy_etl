import os
import datetime
from eazy_etl import Table, GoogleDoc


def main():
    local = 'postgres://user:password@localhost:5432/mydb?application_name=eazy_etl'
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    create_table_script = os.path.join(curr_dir, 'create_table.sql')

    # A Google Doc spreadsheet
    gd = GoogleDoc.open('BT Obligatiuni')
    sheet = gd.worksheet('bteuroclasic')

    # A PostgreSQL table
    bt_euro_classic = Table(name='bt_euro_classic', schema='public', dsn=local)
    bt_euro_classic.create(script_path=create_table_script, drop_if_exists=False)
    bt_euro_classic.insert(transform(extract(sheet)), conflict_on='report_date')


def extract(sheet):
    """ Generate a stream of records from the google doc """
    for records in sheet.get_all_records(empty2zero=False, head=1):
        yield records


def transform(records):
    """ Clean up the mess from the google doc """
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

        new_row['nb_investors'] = row.pop('Numar investitori', None)

        value = row.pop('Numar de unitati in circulatie', None)
        if value:
            new_row['volume'] = value.replace(',', '').split('.')[0]

        value = row.pop('Activ net', None)
        if value:
            new_row['total_value'] = value.replace(',', '')

        yield new_row

if __name__ == '__main__':
    main()