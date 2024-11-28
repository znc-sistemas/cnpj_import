CNPJ - Banco de Dados com dados abertos do Cadastro Nacional de Pessoa Jurídica
===============================================================================


Resultados
----------

Mais de 150 Milhões de registros, em 35 arquivos importados em **~40 min**

O Banco de dados resultante tem o dado estruturado, tipado e com definições de "Primary Keys" e índices.

O banco final tem ~24GB no disco.


.. csv-table:: Database Created
   :delim: |
   :header: "Table" , "Number of Records"
   :widths: 70, 30

   cnae                | 1,358
   motivo              | 60
   municipio           | 5,570
   pais                | 254
   natureza_juridica   | 89
   qualificacao        | 67
   simples             | 41,093,712
   empresa             | 60,294,472
   estabelecimento     | 63,333,636
   socio               | 24,933,508


.. csv-table:: Setup
   :delim: |
   :header: "CPU" , "Memory"  , "Time (min)"
   :widths: 50, 40, 10

   3.1 GHz Dual-Core Intel Core i7  | 16 GB 1867 MHz DDR3 | 39
   2.60 GHz Intel i5 * 12 | 32 GB  | 21


.. note::

    - Os dados foram retirados do `log do sistema.
    - As estatísticas não contemplam o tempo de downlod dos arquivos `zip`
    - As estatísticas não contemplam o tempo de build do "docker"
    - O sistema foi testado com dados do CNPJ de OUT 2023 e NOVEMBRO de 2024


Utilização
----------

O sistema é composto por um arquivo "Docker Compose"
que "sobe" o container com PostgreSQL 16  e um container com código Python.

Por hora, é necessário acessar o container Python e iniciar manualmente o script.


1. Clone o projeto
2. Crie o diretório `data_files` - o nome é nao pode ser alterado.
3. Baixe os arquivos do CNPJ em https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/?C=N;O=D
4. **Não é necessário** criar o diretório onde os arquivos serão extraídos.
5. Abra um shell e execute o "docker compose" ex.: `docker compose build`
6. Possivelmente no mesmo terminal execute o container: `docker compose up`.
7. Abra um outro terminal e execute o container da aplicação `docker exec -it cnpjcheck bash`
8. No shell do container `cnpjcheck` execute o script Python - ex.: `root@<seuContainerId>: /usr/src/app# python main.py` - o comando é somente (`python main.py`)


A aplicação possui um sistema de "log".
Ao iniciar a aplicação, você deve ver mensagens na tela como a abaixo ...

::

    2024-11-24 13:26:00,473 root         INFO     Extração Arquivos ZIP

Um arquivo de log é também criado com mais detalhes, o nome é `YYYYMMHH_HHMM.log` (data e hora em GMT).


Este repositório
----------------

O sistema utiliza um "docker compose", definido no repositório.

O aplicativo irá extrair os arquivos `csv` dos arquivos `zip` para um diretórios `EXTED` dentro do diretórios `dat_files`

.. code-block::

    ├── Dockerfile
    ├── app                               # arquivos da aplicação
    │        ├── csv2tables.py
    │        ├── data_prod.py
    │        ├── database.py
    │        ├── main.py
    │        ├── raw_sqls.py
    │        ├── requirements.txt
    │        ├── tables.py
    │        └── zip_mng.py
    ├── data_files                         # <---  diretório data_files & DOCKER VOLUME
    │        ├── dados_abertos_cnpj
    │        │       └── 2023_05
    │        └── exted                     # <--- diretório onde os zip serão extraídos
    │            ├── cnaes
    │            │       └── F.K03200_Z.D30513.CNAECSV
    │            ├── empresas
    │            │       ├── K3241.K03200Y0.D30513.EMPRECSV
    │            │       ├── ...                    # outros
    │            │       └── K3241.K03200Y9.D30513.EMPRECSV
    │            ├── estabelecimentos
    │            │       ├── K3241.K03200Y1.D30513.ESTABELE
    │            │       ├── ...                    # outros
    │            │       └── K3241.K03200Y8.D30513.ESTABELE
    │            ├── ...                            # outros
    ├── docker-compose.yaml
    ├── notas_comparacoes.txt
    └── readme.rst


Premissas
---------

1. O sistema não gerencia o download dos arquivos `zip`
2. Os arquivos `zip` deverão estar em diretório específico ("docker volume" chamado `data_files`).
3. As estatísticas não contemplam o tempo de download dos arquivos `zip`
4. As estatísticas não contemplam o tempo de "build" do "docker"


Hardware e Configurações
------------------------

.. note::

    Os comentários sobre hardware e configurações foram explicitadas aqui para serem criticadas e se possível testadas em outras máquinas.
    mas acredito que qualquer outro computador de uso geral consiga rodar com as configurações atuais do sistema.

    OU seja, se você não sabe o que é, não altere os parâmetros.


Não foram feitos testes em diferentes máquinas, está rodando em um notebook  MacOS antigo 16GB de memória.

.. note:: **(contribuições, críticas e "benchmark" de testes em outras máquinas são muto bem vindos.)**

O "docker Engine" está configurado para disponibilizar até 3GB de memória.

E o "docker compose" da aplicação tem a seguinte configuração


::
    # container app Pyton
    mem_limit: 512m
    mem_reservation: 512m
    ...

    postgis16:
      image: postgis/postgis:16-3.5-alpine
      mem_limit: 2.5GB
      mem_reservation: 1.5GB
      shm_size: '1gb'


Esta configuração pode ser alterada mas a diminuição irá impactar no aumento do tempo de importação.

Outras configurações importantes são feitas no PostgreSQL vide arquivo "docker-compose"

As configurações abaixo foram feitas com o intuito de otimizar o tempo de comandos de `COPY` e `INSERT` nas tabelas
e não devem ser utilizadas para realização de consultas "sql" ou com o banco de dados em produção.

::

    ...
        shm_size: '1gb'  # shared memory necessária para construção de índices

    command:
      - "postgres"
      - "-c"
      - "autovacuum=OFF"
      - "-c"
      - "min_wal_size=1GB"
      - "-c"
      - "max_wal_size=5GB"
      - "-c"
      - "shared_buffers=768MB"
      - "-c"
      - "fsync=OFF"
      - "-c"
      - "full_page_writes=off"
      - "-c"
      - "checkpoint_timeout=15min" # min tem que ser minusculo


.. important:: **Os parâmetros de configurações acima não devem ser utilizadas NUNCA em produção**


É possível rodar com menos memória, mas implicará em maior tempo de importação.

O espaço em disco deve ser de pelo menos **30GB**

*O banco final terá em torno de 24GB, mas durante o processo de importação, necessita de mais espaço.*

Tempo estimado de importação: ~40 min (desde a extração dos arquivos `.zip` já baixados até o `vacuum` após criação dos índices).



Sobre Cadastro Nacional de Pessoa Jurídica - CNPJ
-------------------------------------------------

1. Baixxe os arquivos em https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj


Problemas conhecidos e resolvidos
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. o ARquivo 5 de estabelecimentos tem um caracter nao `LATIM1` e dá erro no `COPY`

     #) o que resolveu foi  `perl -i -pe 's/\x00//g' /Users/cadu/projs/cnpjcheck/data_files/K3241.K03200Y5.D41012.ESTABELE`

#. Nomes de arquivos com `$` podem causar problemas no `COPY` (psql), e são renomeados.

Estes problemas são tratados automaticamente pela aplicação.
bem como uma `CNPJ_8` duplicado no arquivos de "empresas".


Espaço em Disco
---------------

1. Dados Baixados ainda "zipados" = ~5 GB
2. Dados extraídos = ~15 GB
3. Ainda necessário espaço para o Banco de dados (pgdata)

    Total de espaço necessário = 30GB

    É possível apagar os arquivos ZIP após expandidos,
    mas estes ainda serão copiados no banco de dados e durante a normalização dos dados
    o banco tem os dados todos duplicados. Por isso seria seguro afirmar ao menos 30GB.



Sobre o Banco de Dados
----------------------

O aplicativo python, cria o banco de dados e todas as tabelas necessárias.
Não é necessário intervenção manual no banco de dados.

Características e configurações
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. É usada uma imagem PotgreSQL com Postgis 16
#. O PostgreSQL é executado com uma configuração não padrão - (vide seção `command` no serviço `postgis16` no "docker compose")
#. O app Python seta configurações do PostgreSQL em tempo de execução - vide database.py::`database.createdb`
