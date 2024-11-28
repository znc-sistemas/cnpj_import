import logging
import time
from datetime import datetime
from logging.config import dictConfig

from csv2tables import thread_csv2table
from data_prod import create_indexes, create_pkeys, data_prod
from database import create_db, get_database_stats, prepare_prod_db
from tables import create_csv_tables, create_prod_tables
from zip_mng import thread_unzip_cnpj_files

logging_config = dict(
    version=1,
    formatters={
        'f': {'format': '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
    },
    handlers={
        'stream_hndlr': {
            'class': 'logging.StreamHandler',
            'formatter': 'f',
            'level': logging.INFO
        },
        'file_hndlr': {
            'class': 'logging.FileHandler',
            'formatter': 'f',
            'filename': '{:%Y%m%d_%H%M}.log'.format(datetime.now()),
            'level': logging.DEBUG
        }
    },

    root={
        'handlers': ['file_hndlr', 'stream_hndlr'],
        'level': logging.DEBUG
    },
)
dictConfig(logging_config)

logger = logging.getLogger()

# FILES_TLS = [
#         ('*/**/Cnaes*.zip',             'cnae',            )
#         ('*/**/Motivos*.zip',           'motivo',          )
#         ('*/**/Municipios*.zip',        'municipio',       )
#         ('*/**/Naturezas*.zip',         'natureza',        )
#         ('*/**/Paises*.zip',            'paise',           )
#         ('*/**/Qualificacoes*.zip',     'qualificacoe',    )
#         ('*/**/Empresas*.zip',          'empresa',         )
#         ('*/**/Estabelecimentos*.zip',  'estabelecimento', )
#         ('*/**/Simples*.zip',           'simple',          )
#         ('*/**/Socios*.zip',            'socio',           )
# ]

if __name__ == '__main__':

    TIME_START = time.time()
    TIME_PROC_START = time.process_time()
    TIME_ENDS = time.time()
    TIME_PROC_ENDS = time.time()
    TIME_INTERVAL = 0
    TIME_PROC_INTERVAL = 0

    logger.info('Extração Arquivos ZIP ')
    # unzip_cnpj_files()
    thread_unzip_cnpj_files()  # quase 2x mais rapido

    logger.info('Cria Database CNPJ')
    create_db('postgis16', 'cnpj', 'postgres', 'postgresql')

    logger.info('Cria Tabelas CSV')
    create_csv_tables()

    # # # iconv -f ISO-8859-1 -t UTF-8 yourfile.csv -o yourfile_utf8.csv
    logger.info('Importação (COPY) Arquivos CSV')
    # csv2tables_list()
    thread_csv2table()

    logger.info('Cria Tabelas CNPJ')
    create_prod_tables()

    logger.info('Copia (INSERTS) Tabelas CSV -> CNPJ tabelas CNPJ')
    data_prod()

    logger.info('Criando Índices')
    create_indexes()

    logger.info('Criando "Primary Keys"')
    create_pkeys()

    logger.info('ETAPA: Reset configurações do Banco de Dados')
    prepare_prod_db('postgis16', 'cnpj', 'postgres', 'postgresql')

    get_database_stats()

    # ----------  TIMING
    TIME_ENDS = time.time()
    TIME_PROC_ENDS = time.process_time()

    TIME_INTERVAL = TIME_ENDS - TIME_START
    TIME_PROC_INTERVAL = TIME_PROC_ENDS - TIME_PROC_START

    logger.info(
        f'APP Execution Time: {time.strftime("%H:%M:%S", time.gmtime(TIME_INTERVAL))} ({TIME_INTERVAL:>12.5f} s) - '
        f' - CPU Time: {time.strftime("%H:%M:%S", time.gmtime(TIME_PROC_INTERVAL))} ({TIME_PROC_INTERVAL:>12.5f})'
    )
