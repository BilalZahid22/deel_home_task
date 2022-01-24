# import Python's built-in JSON library
import json, sys

# import the psycopg2 database adapter for PostgreSQL
import psycopg2
from scripts import ddl_scripts,task_scripts


def get_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="Testing",
        user="postgres",
        port="5434",
        password="1122")
    cursor = conn.cursor()

    return cursor, conn


def run_ddl(query):
    cursor, conn = get_connection()

    cursor.execute(query)

    conn.commit()
    cursor.close()

def fetch_result(query):
    try:
        cursor, conn = get_connection()

        cursor.execute(query)
        result_db = cursor.fetchall()

        conn.commit()
        cursor.close()
        return result_db
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)


def ingest_records(query):
    cursor, conn = get_connection()

    cursor.execute(query)

    conn.commit()
    cursor.close()


def create_ddl():
    run_ddl(ddl_scripts.invoices)
    run_ddl(ddl_scripts.contracts)
    run_ddl(ddl_scripts.contracts_raw)
    run_ddl(ddl_scripts.invoices_raw)
    run_ddl(ddl_scripts.sp_invoices)
    run_ddl(ddl_scripts.sp_contracts)


# fetch_result(task_scripts.task_1)
#

