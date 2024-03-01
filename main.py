import contextlib
import logging
import typing
import psycopg2

DBNAME = "postgres"
USER = "postgres"
PASSWORD = "my_password"
HOST = "localhost"
PORT = "5432"


def create_con(dbname: str, user: str, password: str, host: str, port: str):
    con = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    return con

def get_fhir_json():


def main():
    scripts_logger = logging.getLogger()
    with contextlib.closing(
        create_con(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST, port=PORT)
    ) as db_con:
        fhir_json = get_fhir_json()



if __name__ == "__main__":
    main()
