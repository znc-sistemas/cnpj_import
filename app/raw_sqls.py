
SQL_CREATE_CSV_TABLES = '''
    CREATE TABLE csv_cnaes (
        codigo VARCHAR(7),
        descricao VARCHAR(256)
    );


    CREATE TABLE csv_municipios (
        codigo VARCHAR(7),
        municipio VARCHAR(256)
    );

    CREATE TABLE csv_motivos (
        codigo VARCHAR(7),
        motivo VARCHAR(256)
    );

    CREATE TABLE csv_naturezas(
        codigo VARCHAR(7),
        descricao VARCHAR(256)
    );

    CREATE TABLE csv_qualificacoes (
        codigo VARCHAR(7),
        descricao VARCHAR(256)
    );

    CREATE TABLE csv_paises (
        codigo VARCHAR(7),
        nome VARCHAR(256)
    );

    CREATE TABLE csv_simples(
        cnpj_8 VARCHAR(8),
        simples_op VARCHAR(1),
        simples_data VARCHAR(255),
        simples_data_exc VARCHAR(255),
        mei_op VARCHAR(1),
        mei_data VARCHAR(255),
        mei_data_ex VARCHAR(255)
    );

    CREATE TABLE csv_empresas(
        cnpj_8 VARCHAR(8),
        nome_empresarial VARCHAR(255),
        natureza_juridica VARCHAR(10),
        qualificacao_representante_entidade VARCHAR(10),
        capital_social VARCHAR(30),
        porte VARCHAR(5),
        ente_federativo_responsavel VARCHAR(255)
    );

    CREATE TABLE csv_estabelecimentos(
        cnpj_part_1 VARCHAR(255),
        cnpj_part_2 VARCHAR(255),
        cnpj_part_3 VARCHAR(255),
        tipo VARCHAR(255),
        titulo_estabelecimento VARCHAR(255),

        situacao_cadastral VARCHAR(255),
        data_situacao_cadastral VARCHAR(255),
        motivo_situacao_cadastral VARCHAR(255),

        situacao_especial VARCHAR(255),
        data_situacao_especial VARCHAR(255),

        data_inscricao VARCHAR(255),
        cnae_principal VARCHAR(255),
        cnae_secundaria text,

        tipo_logradouro VARCHAR(255),
        logradouro VARCHAR(255),
        numero VARCHAR(255),
        complemento VARCHAR(255),
        bairro VARCHAR(255),
        cep VARCHAR(255),

        uf VARCHAR(255),
        municipio_num VARCHAR(255),
        ddd VARCHAR(255),
        telefone VARCHAR(255),

        pais VARCHAR(255),
        cidade_ext VARCHAR(255),

        campo_x_1 VARCHAR(255),
        campo_x_2 VARCHAR(255),

        email VARCHAR(255),
        fax VARCHAR(255),

        campo_x_3 VARCHAR(255)
    );

    CREATE TABLE csv_socios(
        cnpj VARCHAR(255),
        tipo_registro VARCHAR(255), -- contem valor 2 detalhes
        nome VARCHAR(255),
        cpf_cnpj_socio_administrador VARCHAR(255),
        qualificacao VARCHAR(255),
        data_inclusao VARCHAR(10),
        nome_responsavel_legal_socio_administrador VARCHAR(255),
        cpf_responsavel_legal_socio_administrador VARCHAR(255),
        codigo_pais_socio_administrador VARCHAR(255),
        faixa_etaria_socio_administrador VARCHAR(10),
        qualificacao_responsavel_legal_socio_administrador VARCHAR(255)
    );
'''

# é bizarro mas pensa que é possível customizar. por isso as variáveis
DROP_TEMP_CSV_TABLE_CSV_CNAES = 'DROP TABLE IF EXISTS csv_cnaes;'
DROP_TEMP_CSV_TABLE_CSV_MUNICIPIOS = 'DROP TABLE IF EXISTS csv_municipios;'
DROP_TEMP_CSV_TABLE_CSV_MOTIVOS = 'DROP TABLE IF EXISTS csv_motivos;'
DROP_TEMP_CSV_TABLE_CSV_NATUREZAS = 'DROP TABLE IF EXISTS csv_naturezas;'
DROP_TEMP_CSV_TABLE_CSV_QUALIFICACOES = 'DROP TABLE IF EXISTS csv_qualificacoes;'
DROP_TEMP_CSV_TABLE_CSV_PAISES = 'DROP TABLE IF EXISTS csv_paises;'
DROP_TEMP_CSV_TABLE_CSV_SIMPLES = 'DROP TABLE IF EXISTS csv_simples;'
DROP_TEMP_CSV_TABLE_CSV_EMPRESAS = 'DROP TABLE IF EXISTS csv_empresas;'
DROP_TEMP_CSV_TABLE_CSV_ESTABELECIMENTOS = 'DROP TABLE IF EXISTS csv_estabelecimentos;'
DROP_TEMP_CSV_TABLE_CSV_SOCIOS = 'DROP TABLE IF EXISTS csv_socios;'


DROP_PROD_TABLE = '''
    DROP TABLE IF EXISTS socio;
    DROP TABLE IF EXISTS estabelecimento;
    DROP TABLE IF EXISTS simples;
    DROP TABLE IF EXISTS empresa;
    DROP TABLE IF EXISTS natureza_juridica;
    DROP TABLE IF EXISTS qualificacao;
'''

# --SERIAL PRIMARY KEY,
# --SERIAL PRIMARY KEY,
# em empresas ... REFERENCES qualificacao(id) NULL
CREATE_PROD_TABLES = '''

    CREATE TABLE cnae (
        id INTEGER,
        descricao VARCHAR(256)
    );

    CREATE TABLE motivo (
        id INTEGER,
        descricao VARCHAR(256)
    );

    CREATE TABLE municipio (
        id INTEGER,
        descricao VARCHAR(256)
    );

    CREATE TABLE pais (
        id INTEGER,
        descricao VARCHAR(256)
    );

    CREATE TABLE natureza_juridica (
        codigo INTEGER,
        descricao VARCHAR(256)
    );

    CREATE TABLE qualificacao (
        id INTEGER,
        descricao VARCHAR(256)
    );

    CREATE TABLE simples(
        cnpj_8 VARCHAR(8),
        simples_op boolean,
        simples_data date,
        simples_data_exc date,
        mei_op boolean,
        mei_data date,
        mei_data_ex date
    );

    CREATE TABLE empresa(
        cnpj_8 VARCHAR(8),
        nome_empresarial VARCHAR(255),
        natureza_juridica INTEGER NULL,
        qualificacao_representante_entidade INTEGER NULL,
        capital_social money,
        porte INTEGER,
        ente_federativo_responsavel VARCHAR(255)
    );

    CREATE TABLE estabelecimento(
        id SERIAL PRIMARY KEY,
        cnpj_8 VARCHAR(8),
        cnpj_14 VARCHAR(14),
        cnae_principal VARCHAR(10),
        titulo_estabelecimento VARCHAR(255),
        situacao_cadastral VARCHAR(255),
        data_situacao_cadastral VARCHAR(255),
        motivo_situacao_cadastral VARCHAR(255),
        data_inscricao date,
        tipo_logradouro VARCHAR(20),
        logradouro VARCHAR(255),
        numero VARCHAR(255),
        complemento VARCHAR(255),
        bairro VARCHAR(255),
        cep VARCHAR(12),
        uf VARCHAR(10),
        municipio_num VARCHAR(10),
        pais VARCHAR(255),
        cidade_ext VARCHAR(255)
    );
    CREATE TABLE socio(
        cnpj_8 VARCHAR(8),
        tipo_registro int, -- contem valor 2 detalhes
        nome VARCHAR(255),
        cpf_cnpj_socio_admin VARCHAR(15),
        qualificacao INTEGER,
        data_inclusao DATE,
        nome_resp_legal_socio_admin VARCHAR(255),
        cpf_resp_legal_socio_admin VARCHAR(255),
        codigo_pais_socio_admin VARCHAR(255),
        faixa_etaria_socio_admin VARCHAR(10),
        qualificacao_responsavel_legal_socio_admin VARCHAR(255)
    );
'''

# ===============================================================
# INSERT data prod  =============================================

# cnae
DATA_PROD_CNAE = '''
    ALTER TABLE cnae DISABLE TRIGGER ALL;
    INSERT INTO cnae (id, descricao)
    SELECT
        TRIM(csv_cnaes.codigo)::int,
        INITCAP(csv_cnaes.descricao)
    FROM
        csv_cnaes;
    ALTER TABLE cnae ENABLE TRIGGER ALL;
'''


# municipio
DATA_PROD_MUNICIPIO = '''
    ALTER TABLE municipio DISABLE TRIGGER ALL;
    INSERT INTO municipio (id, descricao)
    SELECT
        TRIM(csv_municipios.codigo)::int,
        INITCAP(csv_municipios.municipio)
    FROM
        csv_municipios;
    ALTER TABLE municipio ENABLE TRIGGER ALL;
'''

# motivo
DATA_PROD_MOTIVO = '''
    ALTER TABLE motivo DISABLE TRIGGER ALL;
    INSERT INTO motivo (id, descricao)
    SELECT
        TRIM(csv_motivos.codigo)::int,
        INITCAP(csv_motivos.motivo)
    FROM
        csv_motivos;
    ALTER TABLE motivo ENABLE TRIGGER ALL;
'''

# pais
DATA_PROD_PAIS = '''
    ALTER TABLE pais DISABLE TRIGGER ALL;
    INSERT INTO pais (id, descricao)
    SELECT
        TRIM(csv_paises.codigo)::int,
        INITCAP(csv_paises.nome)
    FROM
        csv_paises;
    ALTER TABLE pais ENABLE TRIGGER ALL;
'''

DATA_PROD_NATUREZA = '''
    ALTER TABLE natureza_juridica DISABLE TRIGGER ALL;
    INSERT INTO natureza_juridica (codigo, descricao)
    SELECT
        TRIM(csv_naturezas.codigo)::int,
        INITCAP(csv_naturezas.descricao)
    FROM
        csv_naturezas;
    SELECT setval(
        (SELECT pg_get_serial_sequence('natureza_juridica', 'codigo')),
        (SELECT MAX(codigo) FROM natureza_juridica) + 1
    );
    ALTER TABLE natureza_juridica ENABLE TRIGGER ALL;
'''

DATA_PROD_QUALIFICACAO = '''
    ALTER TABLE qualificacao DISABLE TRIGGER ALL;
    INSERT INTO qualificacao (id, descricao)
    SELECT
        TRIM(csv_qualificacoes.codigo)::int,
        INITCAP(csv_qualificacoes.descricao)
    FROM
        csv_qualificacoes;
    SELECT setval((SELECT pg_get_serial_sequence('qualificacao', 'id')), (SELECT MAX(id) FROM qualificacao) + 1);
    ALTER TABLE qualificacao ENABLE TRIGGER ALL;
'''

DATA_PROD_SIMPLES = '''
    ALTER TABLE simples DISABLE TRIGGER ALL;
    INSERT INTO simples (
        cnpj_8,
        simples_op,
        simples_data,
        simples_data_exc,
        mei_op,
        mei_data,
        mei_data_ex
    )
    SELECT
        cnpj_8,
        CASE WHEN simples_op = 'S' THEN TRUE ELSE FALSE END,
        CASE WHEN simples_data = '00000000' THEN NULL ELSE simples_data::date END,
        CASE WHEN simples_data_exc = '00000000' THEN NULL ELSE simples_data_exc::date END,
        CASE WHEN mei_op = 'S' THEN TRUE ELSE FALSE END,
        CASE WHEN mei_data = '00000000' THEN NULL ELSE mei_data::date END,
        CASE WHEN mei_data_ex = '00000000' THEN NULL ELSE mei_data_ex::date END
    FROM
        csv_simples;
    ALTER TABLE empresa ENABLE TRIGGER ALL;
'''

DATA_PROD_EMPRESA = '''
    ALTER TABLE empresa DISABLE TRIGGER ALL;
    INSERT INTO empresa (
        cnpj_8,
        nome_empresarial,
        natureza_juridica,
        qualificacao_representante_entidade,
        capital_social,
        porte,
        ente_federativo_responsavel
        )
    SELECT
        csv_empresas.CNPJ_8,
        TRIM(csv_empresas.nome_empresarial),
        NULLIF(csv_empresas.natureza_juridica::INTEGER, 0),
        NULLIF(csv_empresas.qualificacao_representante_entidade::INTEGER, 0),
        csv_empresas.capital_social::money,
        COALESCE(NULLIF(csv_empresas.porte, ''), '0')::INTEGER,
        TRIM(csv_empresas.ente_federativo_responsavel)
    FROM
        csv_empresas;
    ALTER TABLE empresa ENABLE TRIGGER ALL;
'''

# INDEXES ============================

# -- Time: 150723.141 ms (02:30.723)
INDEX_EMPRESA = '''
    CREATE INDEX idx_empresa_cnpj_8 ON empresa (CNPJ_8);
'''

DATA_PROD_ESTABELECIMENTO = '''
    ALTER TABLE estabelecimento DISABLE TRIGGER ALL;
    INSERT INTO estabelecimento (
        cnpj_8,
        cnpj_14,
        cnae_principal,
        titulo_estabelecimento,
        situacao_cadastral,
        data_situacao_cadastral,
        motivo_situacao_cadastral,
        data_inscricao,
        tipo_logradouro,
        logradouro,
        numero,
        complemento,
        bairro,
        cep,
        uf,
        municipio_num,
        pais,
        cidade_ext
        )
    SELECT
        TRIM(csv_estabelecimentos.cnpj_part_1),
        TRIM(concat(
            csv_estabelecimentos.cnpj_part_1,
            csv_estabelecimentos.cnpj_part_2,
            csv_estabelecimentos.cnpj_part_3)),
        csv_estabelecimentos.cnae_principal,
        TRIM(csv_estabelecimentos.titulo_estabelecimento),
        TRIM(csv_estabelecimentos.situacao_cadastral),
        csv_estabelecimentos.data_situacao_cadastral,
        TRIM(csv_estabelecimentos.motivo_situacao_cadastral),
        csv_estabelecimentos.data_inscricao::date,
        trim(csv_estabelecimentos.tipo_logradouro),
        trim(csv_estabelecimentos.logradouro),
        trim(csv_estabelecimentos.numero),
        trim(csv_estabelecimentos.complemento),
        trim(csv_estabelecimentos.bairro),
        trim(csv_estabelecimentos.cep),
        trim(csv_estabelecimentos.uf),
        trim(csv_estabelecimentos.municipio_num),
        trim(csv_estabelecimentos.pais),
        trim(csv_estabelecimentos.cidade_ext)
    FROM
        csv_estabelecimentos;
    ALTER TABLE estabelecimento ENABLE TRIGGER ALL;
'''


DATA_PROD_SOCIO = '''
    ALTER TABLE socio DISABLE TRIGGER ALL;
    INSERT INTO socio (
        cnpj_8,
        tipo_registro,
        nome,
        cpf_cnpj_socio_admin,
        qualificacao,
        data_inclusao,
        nome_resp_legal_socio_admin,
        cpf_resp_legal_socio_admin,
        codigo_pais_socio_admin,
        faixa_etaria_socio_admin,
        qualificacao_responsavel_legal_socio_admin
        )
    SELECT
        TRIM(csv_socios.cnpj),
        TRIM(csv_socios.tipo_registro)::INTEGER,
        TRIM(csv_socios.nome),
        TRIM(csv_socios.cpf_cnpj_socio_administrador),
        TRIM(csv_socios.qualificacao)::INTEGER,
        TRIM(csv_socios.data_inclusao)::DATE,
        TRIM(csv_socios.nome_responsavel_legal_socio_administrador),
        TRIM(csv_socios.cpf_responsavel_legal_socio_administrador),
        TRIM(csv_socios.codigo_pais_socio_administrador),
        TRIM(csv_socios.faixa_etaria_socio_administrador),
        TRIM(csv_socios.qualificacao_responsavel_legal_socio_administrador)
    FROM
        csv_socios;
    ALTER TABLE socio ENABLE TRIGGER ALL;
    '''

INDEX_EMPRESA = 'CREATE INDEX idx_empresa_cnpj_8 ON empresa (cnpj_8);'
INDEX_SOCIO = 'CREATE INDEX idx_socio_cnpj_8 ON socio (cnpj_8);'
INDEX_ESTABELECIMENTO = 'CREATE INDEX idx_estabelecimento_cnpj_8 ON estabelecimento (cnpj_8);'

# PRIMARY KEYs
PRIMARY_KEY_CNAE = 'ALTER TABLE cnae ADD PRIMARY KEY (id);'
PRIMARY_KEY_SIMPLES = 'ALTER TABLE simples ADD PRIMARY KEY (cnpj_8);'
PRIMARY_KEY_EMPRESA = 'ALTER TABLE empresa ADD PRIMARY KEY (cnpj_8);'
PRIMARY_KEY_ESTABELECIMENTO = 'ALTER TABLE estabelecimento ADD PRIMARY KEY (cnpj_14);'
PRIMARY_KEY_MOTIVO = 'ALTER TABLE motivo ADD PRIMARY KEY (id);'
PRIMARY_KEY_MUNICIPIO = 'ALTER TABLE municipio ADD PRIMARY KEY (id);'
PRIMARY_KEY_NATUREZA_JURIDICA = 'ALTER TABLE natureza_juridica ADD PRIMARY KEY (codigo);'
PRIMARY_KEY_PAIS = 'ALTER TABLE pais ADD PRIMARY KEY (id);'
PRIMARY_KEY_QUALIFICACAO = 'ALTER TABLE qualificacao  ADD PRIMARY KEY (id);'
