import logging
import psycopg2
import database
import raw_sqls
from concurrent import futures

CON_PARAMS = ['postgis16', 'cnpj', 'postgres', 'postgresql']

logger = logging.getLogger()


def data_prod_municipio():
    '''
    '''
    logger.debug('Copia (INSERTS) Tabelas CSV -> CNPJ: municipio')
    rows_aff = database.sql_exec(*CON_PARAMS, raw_sqls.DATA_PROD_MUNICIPIO)
    logger.debug('Copia Finalizada Tabelas CSV -> CNPJ: municipio')
    return rows_aff


def data_prod_pais():
    '''
    '''
    logger.debug('Copia (INSERTS) Tabelas CSV -> CNPJ: país')
    rows_aff = database.sql_exec(*CON_PARAMS, raw_sqls.DATA_PROD_PAIS)
    logger.debug('Copia Finalizada Tabelas CSV -> CNPJ: país')
    return rows_aff


def data_prod_cnae():
    '''
    '''
    logger.debug('Copia (INSERTS) Tabelas CSV -> CNPJ: cnae')
    rows_aff = database.sql_exec(*CON_PARAMS, raw_sqls.DATA_PROD_CNAE)
    logger.debug('Copia Finalizada Tabelas CSV -> CNPJ: cnae')
    return rows_aff


def data_prod_motivo():
    '''
    '''
    logger.debug('Copia (INSERTS) Tabelas CSV -> CNPJ: motivo')
    rows_aff = database.sql_exec(*CON_PARAMS, raw_sqls.DATA_PROD_MOTIVO)
    logger.debug('Copia Finalizada Tabelas CSV -> CNPJ: motivo')
    return rows_aff


def data_prod_natureza():
    logger.debug('Copia (INSERTS) Tabelas CSV -> CNPJ: natureza')
    rows_aff = database.sql_exec(*CON_PARAMS, raw_sqls.DATA_PROD_NATUREZA)
    logger.debug('Copia Finalizada Tabelas CSV -> CNPJ: natureza')
    return rows_aff


def data_prod_qualificacao():
    logger.debug('Copia (INSERTS) Tabelas CSV -> CNPJ: qualificacao ')
    rows_aff = database.sql_exec(*CON_PARAMS, raw_sqls.DATA_PROD_QUALIFICACAO)
    logger.debug('Copia Finalizada Tabelas CSV -> CNPJ: qualificacao ')
    return rows_aff


def data_prod_simples():
    '''
    tempo com
    '''
    logger.debug('Copia (INSERTS) Tabelas CSV -> CNPJ: simples ')
    rows_aff = database.sql_exec(*CON_PARAMS, raw_sqls.DATA_PROD_SIMPLES)
    logger.debug('Copia Finalizada Tabelas CSV -> CNPJ: simples ')
    return rows_aff


def data_prod_empresa():
    '''
    tempo com
    '''
    logger.debug('Copia (INSERTS) Tabelas CSV -> CNPJ: empresa ')
    rows_aff = database.sql_exec(*CON_PARAMS, raw_sqls.DATA_PROD_EMPRESA)
    logger.debug('Copia Finalizada Tabelas CSV -> CNPJ: empresa ')
    return rows_aff


def data_prod_estabelecimento():
    '''
    '''
    logger.debug('Copia (INSERTS) Tabelas CSV -> CNPJ: estabelecimento ')
    rows_aff = database.sql_exec(*CON_PARAMS, raw_sqls.DATA_PROD_ESTABELECIMENTO)
    logger.debug('Copia Finalizada Tabelas CSV -> CNPJ: estabelecimento ')
    return rows_aff


def data_prod_socio():
    '''
    '''
    logger.debug('Copia (INSERTS) Tabelas CSV -> CNPJ: socio')
    rows_aff = database.sql_exec(*CON_PARAMS, raw_sqls.DATA_PROD_SOCIO)
    logger.debug('Copia Finalizada Tabelas CSV -> CNPJ: socio')
    return rows_aff


def data_prod():

    logger.info('inica threads data prod')
    with futures.ThreadPoolExecutor() as executor:
        fs = []

        fs.append(executor.submit(data_prod_municipio))
        fs.append(executor.submit(data_prod_cnae))
        fs.append(executor.submit(data_prod_motivo))
        fs.append(executor.submit(data_prod_pais))
        fs.append(executor.submit(data_prod_natureza))
        fs.append(executor.submit(data_prod_qualificacao))
        fs.append(executor.submit(data_prod_simples))
        fs.append(executor.submit(data_prod_empresa))
        fs.append(executor.submit(data_prod_estabelecimento))
        fs.append(executor.submit(data_prod_socio))

        for future in futures.as_completed(fs):
            _ = future.result()

    logger.info('drop CSV tables')
    database.sql_exec(*CON_PARAMS, raw_sqls.DROP_TEMP_CSV_TABLE_CSV_SIMPLES)
    database.sql_exec(*CON_PARAMS, raw_sqls.DROP_TEMP_CSV_TABLE_CSV_EMPRESAS)
    database.sql_exec(*CON_PARAMS, raw_sqls.DROP_TEMP_CSV_TABLE_CSV_ESTABELECIMENTOS)
    database.sql_exec(*CON_PARAMS, raw_sqls.DROP_TEMP_CSV_TABLE_CSV_SOCIOS)
    database.sql_exec(*CON_PARAMS, raw_sqls.DROP_TEMP_CSV_TABLE_CSV_MUNICIPIOS)
    database.sql_exec(*CON_PARAMS, raw_sqls.DROP_TEMP_CSV_TABLE_CSV_PAISES)
    database.sql_exec(*CON_PARAMS, raw_sqls.DROP_TEMP_CSV_TABLE_CSV_CNAES)
    database.sql_exec(*CON_PARAMS, raw_sqls.DROP_TEMP_CSV_TABLE_CSV_MOTIVOS)
    database.sql_exec(*CON_PARAMS, raw_sqls.DROP_TEMP_CSV_TABLE_CSV_NATUREZAS)
    database.sql_exec(*CON_PARAMS, raw_sqls.DROP_TEMP_CSV_TABLE_CSV_QUALIFICACOES)

    logger.info('Vacuum full')
    database.sql_exec(*CON_PARAMS, 'vacuum full;')


def create_indexes():

    # 2024-11-12 09:25:07,219 root         DEBUG    Create Index Table: empresa
    # 2024-11-12 09:28:32,476 root         DEBUG    	 Total time exe 205.22987031936646
    # 2024-11-12 09:28:32,477 root         DEBUG    	 Total time cpu 0.009733360999999996
    logger.debug('Create Index Table: empresa')

    database.sql_exec(*CON_PARAMS, raw_sqls.INDEX_EMPRESA)
    # coorecao dados
    database.sql_exec(*CON_PARAMS, "DELETE FROM empresa where cnpj_8='07947717' AND nome_empresarial='';")


def create_pkeys():

    tables_names = [
        'CNAE',
        'MOTIVO',
        'MUNICIPIO',
        'PAIS',
        'NATUREZA_JURIDICA',
        'QUALIFICACAO',
        'SIMPLES',
        'EMPRESA',
        'ESTABELECIMENTO',
        # 'SOCIO',
    ]

    for name in tables_names:
        logger.debug(f'Create Primary Key Table: {name}')
        sql_stt = getattr(raw_sqls, f'PRIMARY_KEY_{name}')
        try:
            database.sql_exec(*CON_PARAMS, sql_stt)
        except psycopg2.errors.UniqueViolation:
            logger.warning(f'ERROR: psycopg2.errors.UniqueViolation `{sql_stt}`')
        except psycopg2.errors.InvalidTableDefinition:
            pass

    logger.debug('\t vacuum after PK')
    database.sql_exec(*CON_PARAMS, 'vacuum;')

#       cnpj_8  |    nome_empresarial     | natureza_juridica  ...
# ----------+-------------------------+-------------------+--- ...
#  07947717 |                         |                   |    ...
#  07947717 | REINALDO BORGES E OUTRA |              4120 |    ...
# (2 rows)
