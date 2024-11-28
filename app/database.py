import logging

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logger = logging.getLogger()


def get_database_stats():

    dsn = {'host': 'postgis16', 'dbname': 'cnpj', 'user': 'postgres', 'password': 'postgresql'}

    con = psycopg2.connect(**dsn)

    with con.cursor() as cur:

        sql_stt = "SELECT pg_size_pretty( pg_database_size('cnpj'));"
        cur.execute(sql_stt)
        r = cur.fetchall()

        logger.info(f'Database Disk Size: {r[0][0]}')

        sql_stt = (
            "SELECT relname, reltuples::bigint AS estimate FROM pg_class WHERE oid in ( "
            "    'public.empresa'::regclass, "
            "    'public.simples'::regclass, "
            "    'public.estabelecimento'::regclass, "
            "    'public.socio'::regclass, "
            "    'public.cnae'::regclass, "
            "    'public.motivo'::regclass, "
            "    'public.natureza_juridica'::regclass, "
            "    'public.municipio'::regclass, "
            "    'public.pais'::regclass, "
            "    'public.qualificacao'::regclass "
            "    ); "
        )
        cur.execute(sql_stt)
        r = cur.fetchall()
        for t, s in r:
            logger.info(f'Tabela {t:.<20} {int(s):,} registros')


def create_db(host, new_db_name, user_name, user_password, maintenance_db_name='postgres', drop_exists=True):
    '''
    db_name='mydb'
    dsn = {'host': 'postgis16', 'dbname': 'mypostdatabase', 'user': 'postgres', 'password': 'postgresql'}

    '''
    dsn = {'host': host, 'dbname': maintenance_db_name, 'user': user_name, 'password': user_password}

    sql_stt = (
        "CREATE DATABASE {} "
        "WITH OWNER = postgres ENCODING = 'UTF8' LC_COLLATE = 'pt_BR.UTF-8' "
        "LC_CTYPE = 'pt_BR.UTF-8' TEMPLATE = template0;"
    )
    con = psycopg2.connect(**dsn)
    try:
        # porque connection não fecha automat. dentro do bloco `with` - só a seção.

        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with con.cursor() as cur:
            logger.debug(f'Database CNPJ - Cria database {new_db_name}')
            try:
                cur.execute(sql_stt.format(new_db_name))
            except psycopg2.errors.DuplicateDatabase:
                if drop_exists:
                    cur.execute(f'DROP DATABASE {new_db_name};')
                    cur.execute(sql_stt.format(new_db_name))

            cur.execute("ALTER DATABASE cnpj SET constraint_exclusion='OFF';")
            cur.execute("ALTER DATABASE cnpj SET maintenance_work_mem='512MB';")
            cur.execute("ALTER DATABASE cnpj SET synchronous_commit='OFF';")
            cur.execute("ALTER DATABASE cnpj SET work_mem='1GB';")
            cur.execute("SELECT pg_reload_conf();")
            sql_stt = (
                " SELECT name, setting, unit FROM pg_settings WHERE name IN ("
                "'autovacuum', "
                "'max_wal_size', "
                "'fsync', "
                "'full_page_writes', "
                "'checkpoint_timeout', "
                "'constraint_exclusion', "
                "'maintenance_work_mem', "
                "'synchronous_commit', "
                "'work_mem', "
                "'shared_buffers', "
                "'effective_cache_size', "
                "'checkpoint_completion_target', "
                "'wal_buffers', "
                "'default_statistics_target', "
                "'random_page_cost', "
                "'effective_io_concurrency', "
                "'huge_pages', "
                "'min_wal_size', "
                "'max_wal_size'"
                ")"
            )
            cur.execute(sql_stt)
            r = cur.fetchall()

            for message in [f'PGre settings - {descript:.<21}{value:.>15} {m if m else '':<2}' for descript, value, m in r]:  # noqa
                logger.info(message)

    finally:
        con.close()


def drop_database(host, db_name_2b_droped, user_name, user_password, maintenance_db_name='postgres', force=False):
    '''drop db
    dsn = {'host': 'postgis16', 'db_name_2b_droped': 'mypostdatabase', 'user': 'postgres', 'password': 'postgresql'}
    '''
    dsn = {'host': host, 'dbname': maintenance_db_name, 'user': user_name, 'password': user_password}
    if force:
        sql_stt = "drop database {} with (FORCE);"
    else:
        sql_stt = "drop database {};"
    con = psycopg2.connect(**dsn)
    try:
        # porque connection não fecha automat. dentro do bloco `with` - só a seção.
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with con.cursor() as cur:
            cur.execute(sql_stt.format(db_name_2b_droped))
    finally:
        con.close()


def sql_exec_autocommit(host, db_name, user_name, user_password, sql_stt=''):
    '''drop db
    ex.: drop_database('postgis16', 'mypostdatabase', 'postgres', 'postgresql')
    '''
    dsn = {'host': host, 'dbname': db_name, 'user': user_name, 'password': user_password}
    con = psycopg2.connect(**dsn)
    # isso esta errado.
    # como esta seno usado principalmente  para executar queries
    # que dependenem de concatenacao deveia ser usado  `psycopg2.sql`
    try:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with con.cursor() as cur:
            cur.execute(sql_stt)
    finally:
        con.close()


def sql_exec(host, db_name, user_name, user_password, sql_stt='', params=None):
    '''
    WARNING: ESta funcao deve ser depcrecada em faor da `sql_exec_autocommit`
    drop db
    ex.: drop_database('postgis16', 'mypostdatabase', 'postgres', 'postgresql')
    dsn = {'host': 'postgis16', 'dbname': 'cnpj', 'user': 'postgres', 'password': 'postgresql'}
    '''
    dsn = {'host': host, 'dbname': db_name, 'user': user_name, 'password': user_password}
    con = psycopg2.connect(**dsn)
    rows_affected = 0
    # isso esta errado.
    # como esta seno usado principalmente  para executar queries
    # que dependenem de concatenacao deveia ser usado  `psycopg2.sql`
    # time_exe = time.time()
    # time_cpu = time.process_time()
    try:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        con.autocommit = True
        with con.cursor() as cur:
            if params:
                cur.execute(sql_stt, params)
            else:
                cur.execute(sql_stt)

            rows_affected = cur.rowcount

    except psycopg2.errors.CharacterNotInRepertoire:
        logger.warning(f'Error: psycopg2.errors.CharacterNotInRepertoire - Charater não recohecido  {sql_stt}')
        raise
    finally:
        con.close()

    # total_time_exe = time.time() - time_exe
    # total_time_cpu = time.process_time() - time_cpu

    # logger.debug(
    #     f'\t Total EXE: {total_time_exe:>10.5f} / {time.strftime("%H:%M:%S", time.gmtime(total_time_exe))}'
    #     f' - Total CPU: {total_time_cpu:>10.5f} / {time.strftime("%H:%M:%S", time.gmtime(total_time_cpu))}'
    # )
    return rows_affected


def prepare_prod_db(host, new_db_name, user_name, user_password, maintenance_db_name='postgres', drop_exists=True):
    '''
    db_name='mydb'
    dsn = {'host': 'postgis16', 'dbname': 'mypostdatabase', 'user': 'postgres', 'password': 'postgresql'}

    '''
    dsn = {'host': host, 'dbname': maintenance_db_name, 'user': user_name, 'password': user_password}

    con = psycopg2.connect(**dsn)
    try:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with con.cursor() as cur:
            cur.execute("ALTER DATABASE cnpj SET constraint_exclusion='OFF';")
            cur.execute("ALTER DATABASE cnpj SET maintenance_work_mem='256MB';")
            cur.execute("ALTER DATABASE cnpj SET synchronous_commit='ON';")
            cur.execute("ALTER DATABASE cnpj SET work_mem='3276kB';")
            cur.execute("ALTER DATABASE cnpj SET effective_cache_size = '768MB';")
            cur.execute("ALTER DATABASE cnpj SET default_statistics_target = '500';")
            cur.execute("ALTER DATABASE cnpj SET random_page_cost = '1.1';")
            cur.execute("ALTER DATABASE cnpj SET effective_io_concurrency = '200';")

            cur.execute("SELECT pg_reload_conf();")

            sql_stt = (
                " SELECT name, setting, unit FROM pg_settings WHERE name IN ("
                "'autovacuum', "
                "'max_wal_size', "
                "'fsync', "
                "'full_page_writes', "
                "'checkpoint_timeout', "
                "'constraint_exclusion', "
                "'maintenance_work_mem', "
                "'synchronous_commit', "
                "'work_mem', "
                "'shared_buffers', "
                "'effective_cache_size', "
                "'checkpoint_completion_target', "
                "'wal_buffers', "
                "'default_statistics_target', "
                "'random_page_cost', "
                "'effective_io_concurrency', "
                "'huge_pages', "
                "'min_wal_size', "
                "'max_wal_size'"
                ")"
            )
            cur.execute(sql_stt)
            r = cur.fetchall()

            for message in [f'PGre settings - {descript:.<21}{value:.>15} {m if m else '':<2}' for descript, value, m in r]:  # noqa
                logger.info(message)

    finally:
        con.close()
