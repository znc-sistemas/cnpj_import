import logging
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

logger = logging.getLogger()


def dir_from_zip_name(path):
    filename = path.stem  # Get the filename without extension
    # Remove trailing numbers from the filename
    last_word = ''.join(filter(lambda c: not c.isdigit(), filename))
    return last_word.lower()  # Return in lowercase for uniformity


def unzip_file(file_path, extract_dir):
    '''
    file_name  as string
    return extrated path
    '''
    logger.debug(f'Extraindo arquivo: {file_path.stem}')
    file_path = Path(file_path)
    extracted_path = Path(extract_dir)
    extracted_path.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_path)
    logger.info(f'Finalizada extração: {file_path.name}')
    return extracted_path


def unzip_list(base_dir, glob_exp, extract_dir=None):
    '''
    base_path  - path inicial
    glob_exp  = **/*.zip, ../**/*.py , ../*.py  - desde que seja reltivo ao base_path
    '''
    base_path = Path(base_dir)

    if extract_dir:
        exted_path = Path(extract_dir)
    else:
        exted_path = base_path / 'exted'

    paths = []

    for file_path in base_path.glob(glob_exp):
        # exted_path = f"{base_path / 'exted' / base_path.name}".lower().replace('.', '_')
        if not extract_dir:

            exted_path = exted_path / file_path.name.lower().replace('.', '_')
        paths.append(unzip_file(file_path, exted_path))

    return paths


def unzip_cnpj_files():

    globs_list = [
        '*/**/Cnaes*.zip',
        '*/**/Empresas*.zip',
        '*/**/Estabelecimentos*.zip',
        '*/**/Motivos*.zip',
        '*/**/Municipios*.zip',
        '*/**/Naturezas*.zip',
        '*/**/Paises*.zip',
        '*/**/Qualificacoes*.zip',
        '*/**/Simples*.zip',
        '*/**/Socios*.zip',
    ]

    base_dir = '/usr/src/data_files'

    for glob in globs_list:
        extracted_path = Path(glob)
        extracted_dir = Path(base_dir) / 'exted' / extracted_path.parts[-1].lower().replace('*', '').replace('.zip', '') or 'extracted'  # noqa
        unzip_list(base_dir, glob, extracted_dir)


def task(zip_file):
    dir_name = dir_from_zip_name(zip_file)
    res = unzip_file(zip_file, '/usr/src/data_files/exted/' / Path(dir_name))
    return res


def thread_unzip_cnpj_files():

    current_path = Path(os.getcwd())
    zip_files_path = current_path / '../data_files/'
    zip_files_list = list(zip_files_path.glob('**/*.zip'))

    # Start a thread pool executor
    with ThreadPoolExecutor() as executor:

        _ = {executor.submit(task, zip_file): zip_file for zip_file in zip_files_list}
