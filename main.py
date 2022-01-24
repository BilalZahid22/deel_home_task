# import Python's built-in JSON library
import json

from psycopg2._json import Json
import utils.db_util as util
from scripts import task_scripts


def ingest_data(file_path, table_name):
    # use Python's open() function to load the JSON data
    with open(file_path) as json_data:
        # use load() rather than loads() for JSON files
        record_list = json.load(json_data)

        columns = [list(x.keys()) for x in record_list][0]
        sql_string = 'INSERT INTO {} '.format(table_name)
        sql_string += "(" + ', '.join(columns) + ")\nVALUES "

        values = [list(x.values()) for x in record_list]

        # value string for the SQL string
        values_str = ""

        # enumerate over the records' values
        for i, record in enumerate(values):

            # declare empty list for values
            val_list = []
            # append each value to a new list of values
            for v, val in enumerate(record):
                val = val if len(str(val).strip()) > 0 else ''
                if type(val) == str:
                    val = str(Json(val)).replace('"', '')

                val_list += [str(val)]

            # put parenthesis around each record string
            values_str += "(" + ', '.join(val_list) + "),\n"

        # remove the last comma and end SQL with a semicolon
        values_str = values_str[:-2] + ";"

        sql_string += values_str

        print(sql_string)
        util.ingest_records(sql_string)

    # Press the green button in the gutter to run the script.


def create_ddl():
    util.create_ddl()


def data_ingestion():
    ingest_data('raw_data/contracts.json', 'public.contracts_raw')
    ingest_data('raw_data/invoices.json', 'public.invoices_raw')


def task_1():
    result = util.fetch_result(task_scripts.task_1)
    print("Count   :   Received_date     : Deleted")
    for row in result:
        print(str(row[0]) + "   :   " + str(row[1]) + "  :   " + str(row[2]) + " \n")


def task_2():
    result = util.fetch_result(task_scripts.task_2)
    print("Contract_id:      Received_at   :   Deleted     : Count")
    for row in result:
        print(str(row[0]) + "   :   " + str(row[1]) + "  :   " + str(row[2]) + "  :   " + str(row[3]) + " \n")


def task_3():
    result = util.fetch_result(task_scripts.task_3)
    print("Contract_id:      Received_at   :   Currency     : SUM")
    for row in result:
        print(str(row[0]) + "   :   " + str(row[1]) + "  :   " + str(row[2]) + "  :   " + str(row[3]) + " \n")


def task_4():
    result = util.fetch_result(task_scripts.task_4)
    print("""Invoice_id   :   Contract_id :      First_received_at(Invoice)   :   First_received_at(Contracts)     : Client_id""")
    for row in result:
        print(str(row[0]) + "   :   " + str(row[1]) + "  :   " + str(row[2]) + "  :   " + str(row[3]) + "  :   " + str(
            row[4]) + " \n")


if __name__ == '__main__':
    create_ddl()
    data_ingestion()
    task_1()
    task_2()
    task_3()
    task_4()
    print('')
