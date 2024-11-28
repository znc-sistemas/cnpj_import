import logging
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import database
import psycopg2

logger = logging.getLogger()
# logger.debug('Start Create tables csv_xxx')

CON_PARAMS = ['postgis16', 'cnpj', 'postgres', 'postgresql']

CSV_DIR_NAMES = [
    ('cnaes', 'csv_cnaes'),
    ('motivos', 'csv_motivos'),
    ('municipios', 'csv_municipios'),
    ('simples', 'csv_simples'),
    ('naturezas', 'csv_naturezas'),
    ('paises', 'csv_paises'),
    ('qualificacoes', 'csv_qualificacoes'),
    ('empresas', 'csv_empresas'),
    ('estabelecimentos', 'csv_estabelecimentos'),
    ('socios', 'csv_socios'),
    ]


def copy_csv_file(table_name, csv_path, con_params=['postgis16', 'cnpj', 'postgres', 'postgresql']):
    '''
    '''
    sql_stt_template = (
        f"COPY {table_name} "
        f"FROM '{csv_path}' DELIMITER ';' CSV HEADER QUOTE '\"' ENCODING  'LATIN1';"
    )
    rows = database.sql_exec(*con_params, sql_stt_template)
    return rows


def csv_path_list(csvs_dir_path, glob_string='**/*.*'):
    '''gera lista de apths dos arwuivos CSV de 1 diretorio'''

    csvs_dir_path = Path(csvs_dir_path)
    csv_files = []
    for file_path in csvs_dir_path.glob(glob_string):
        if '$' in str(file_path):  # porque alguns arquivs tem `$` no nome depois de extraidos.
            file_path = file_path.rename(str(file_path).replace('$', '_'))
            logger.info(f'file renamed - to:"{file_path}"')
        csv_files.append(file_path)
    return csv_files


def copy_csv_to_table(file_path, table_name):
    '''
    set template COPY table_name FROM csv_file_path
    usa rotina externa para execução
    Os campos da tabela e do CSV tem que ser iguais - não há definicao de campos no SQL statement.
    '''
    sql_stt_template = (
        f"COPY {table_name} "
        f"FROM '{file_path}' DELIMITER ';' CSV HEADER QUOTE '\"' ENCODING  'LATIN1';"
    )

    database.sql_exec(*CON_PARAMS, sql_stt_template)
    return


def csv2table(csv_dir, table_name, delete_before=True):
    '''
    ex.:
        csv_dir  = /usr/src/data_files/exted/cnaes
        table_name = cnaes

    some day I would like to understand why this doenst work
    sql_stt = sql.SQL(
            "COPY {table_name} FROM '{csv_name}' DELIMITER ';' CSV HEADER QUOTE '\"' ENCODING 'LATIN1'"
        ).format(
            table_name=sql.Identifier(table_name),
            csv_name=sql.Identifier(str(file_path)),
        )
    '''

    csv_path = Path(csv_dir)
    if delete_before:
        try:
            sql_stt_template = f"DELETE FROM {table_name};"
            database.sql_exec(*CON_PARAMS, sql_stt_template)
        except psycopg2.errors.UndefinedTable:
            pass

    for file_path in csv_path.glob('**/*.*'):
        logger.debug(f'COPY CSV file:"{file_path}" to Table ')
        if '$' in str(file_path):
            #  os putos colocam $ no nome do arquivo da erro no postgresq/psql
            file_path = file_path.rename(str(file_path).replace('$', '_'))
            logger.debug(f'file renamed - to:"{file_path}"')
        sql_stt_template = (
            f"COPY {table_name} "
            f"FROM '{file_path}' DELIMITER ';' CSV HEADER QUOTE '\"' ENCODING  'LATIN1';"
        )
        database.sql_exec(*CON_PARAMS, sql_stt_template)


def csv2tables_list():
    time_exe = time.time()
    time_cpu = time.process_time()

    base_path = Path('/usr/src/data_files/')

    for t in CSV_DIR_NAMES:
        dir = base_path / 'exted' / t[0]
        csv2table(dir, t[1], delete_before=True)
    total_time_exe = time.time() - time_exe
    total_time_cpu = time.process_time() - time_cpu

    logger.info(
        f'\t Copy CSV - EXEC Time: {total_time_exe:<10.5f} / {time.strftime("%H:%M:%S", time.gmtime(total_time_exe))}'
        f' - Total CPU: {total_time_cpu:<10.5f} / {time.strftime("%H:%M:%S", time.gmtime(total_time_cpu))}'
    )


def task(files):
    tot_rows = 0

    for fle in files:

        rows = 0
        try:
            rows = copy_csv_file(f'csv_{fle.parts[-2]}', fle.resolve(), con_params=CON_PARAMS)
        except psycopg2.errors.CharacterNotInRepertoire:
            logger.warning(f"info: psycopg2.errors.CharacterNotInRepertoire - tentando corrigir arquivo {fle}")
            perl_command = f"perl -i -pe 's/\\x00//g' {fle}"
            rows = 0
            try:
                # Execute the Perl command using subprocess
                subprocess.run(perl_command, shell=True, check=True)
                try:
                    rows = copy_csv_file(f'csv_{fle.parts[-2]}', fle.resolve(), con_params=CON_PARAMS)
                    logger.warning(f"Importação (COPY) arquivo corrigido e importado: {fle.name} - Nº linhas :{rows}")
                except psycopg2.errors.CharacterNotInRepertoire:
                    logger.error(f"impossível corrigir erro no arquivo: {fle.name}")

            except subprocess.CalledProcessError:
                logger.info(f"Erro ao tentar corrigir o arquivo: {fle.name}")

        except psycopg2.errors.UndefinedTable:
            logger.error(f'table não existente  - {fle}')
        except psycopg2.errors.UndefinedFile:
            logger.error(f'Arquivo Não Encontrado  - {fle}')
        logger.debug(f"Importação (COPY) Arquivo CSV: {fle.name} - Nº linhas: {rows}")
        tot_rows += rows
    logger.info(f'Importação (COPY) Arquivos "{fle.parts[-2]}" - total de nº linhas: {tot_rows}')
    return tot_rows


def thread_csv2table():
    current_path = Path(os.getcwd())
    csv_base_dir = '../data_files/exted/'
    csv_dir = current_path / Path(csv_base_dir)
    files = list(csv_dir.glob('**/*.[!DS_Store]*', case_sensitive=False))
    # 1 thread per table - evitar concorrencia na tabela por mais que tenhamos configurado o postgresql
    # O nome da tabela é o nome do diretório do CSV file
    logger.info(f'Arquivos CSV encontrados: {len(files)}')
    tables = dict()

    for fle in files:
        # tables['cnae'] -> foo/bar/cnae/ffoobar01.foo
        try:
            tables[fle.parts[-2]].append(fle)
        except KeyError:
            tables[fle.parts[-2]] = list()
            tables[fle.parts[-2]].append(fle)

    with ThreadPoolExecutor() as executor:
        _ = {executor.submit(task, tables[table]): tables[table] for table in tables}
