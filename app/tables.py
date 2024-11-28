import database
import raw_sqls
import logging
logger = logging.getLogger()

CON_PARAMS = [
    'postgis16',
    'cnpj',
    'postgres',
    'postgresql'
]


def create():
    database.craete_db(*CON_PARAMS)


def drop(force=False):
    database.drop_database(*CON_PARAMS)


def create_csv_tables():
    database.sql_exec(*CON_PARAMS, raw_sqls.SQL_CREATE_CSV_TABLES)


def create_prod_tables():

    database.sql_exec(*CON_PARAMS, raw_sqls.DROP_PROD_TABLE)
    database.sql_exec(*CON_PARAMS, "vacuum;")

    database.sql_exec(*CON_PARAMS, raw_sqls.CREATE_PROD_TABLES)
