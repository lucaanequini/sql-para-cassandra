# Importações padrão
import os
from typing import Any
from decimal import Decimal

# Bibliotecas
import psycopg2
from dotenv import load_dotenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Carregar variáveis de ambiente
load_dotenv()

# Importação das queries
from queries import query_student_academic_record, disciplinas_professor, alunos_formados, chefes_departamento, grupo_de_tcc

# Configuração das conexões com os bancos
postgres = psycopg2.connect(os.getenv('SQL_URL'))
cloud_config= {
  'secure_connect_bundle': os.getenv('ASTRA_SECURE_BUNDLE')
}
auth_provider = PlainTextAuthProvider(os.getenv('CLIENT_ID'), os.getenv('CLIENT_SECRET'))
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

def listar_tabelas() -> list[str]:
    print("Listando tabelas disponíveis no PostgreSQL...")
    with postgres.cursor() as db_context:
        db_context.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE';
        """)
        resultado = db_context.fetchall()
        postgres.commit()
        return [tabela[0] for tabela in resultado]

def buscar_todos_registros(nome_tabela: str):
    print(f"Buscando todos os registros de '{nome_tabela}'...")
    with postgres.cursor() as db_context:
        db_context.execute(f'SELECT * FROM "{nome_tabela}";')
        registros = db_context.fetchall()
        postgres.commit()
        return registros

def obter_colunas(nome_tabela: str):
    print(f"Obtendo colunas da tabela '{nome_tabela}'...")
    with postgres.cursor() as db_context:
        db_context.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{nome_tabela}';")
        resposta = db_context.fetchall()
        postgres.commit()
        return [coluna[0] for coluna in resposta]

def migrar_dados(nome_tabela: str, colunas: list[str], registros: list[tuple[Any]]):
    for registro in registros:
        formatted = [str(value) if isinstance(value, Decimal) or isinstance(value, int) else f"'{value}'" for value in registro]
        session.execute(f"INSERT INTO {nome_tabela} ({', '.join(colunas)}) VALUES ({', '.join(formatted)});")

def cria_tabelas():
    tabelas = listar_tabelas()
    session.set_keyspace("faculdade")

    for tabela in tabelas:
        colunas = obter_colunas(tabela)
        registros = buscar_todos_registros(tabela)

        if tabela == 'course':
            session.execute("""
                CREATE TABLE course (
                    id TEXT,
                    title TEXT,
                    PRIMARY KEY (id)
                );
            """)
            migrar_dados(tabela, colunas, registros)
        elif tabela == 'student':
            session.execute("""
                CREATE TABLE student (
                    id TEXT, 
                    name TEXT,
                    course_id TEXT,
                    group_id TEXT,
                    PRIMARY KEY (id)
                );
            """)
            migrar_dados(tabela, colunas, registros)
        elif tabela == 'professor':
            session.execute("""
                CREATE TABLE professor (
                    id TEXT,
                    name TEXT,
                    dept_name TEXT, 
                    salary DECIMAL,
                    PRIMARY KEY (id)
                );
            """)
            migrar_dados(tabela, colunas, registros)
        elif tabela == 'department':
            session.execute("""
                CREATE TABLE department (
                    dept_name TEXT,
                    budget DECIMAL,
                    boss_id TEXT,
                    PRIMARY KEY (dept_name)
                );
            """)
            migrar_dados(tabela, colunas, registros)
        elif tabela == 'tcc_group':
            session.execute("""
                CREATE TABLE tcc_group (
                    id TEXT,
                    professor_id TEXT,
                    PRIMARY KEY (id)
                );                                
            """)
            migrar_dados(tabela, colunas, registros)
        elif tabela == 'subj':
            session.execute("""
                CREATE TABLE subj (
                    id TEXT,
                    title TEXT,
                    dept_name TEXT,
                    PRIMARY KEY (id)
                );
            """)
            migrar_dados(tabela, colunas, registros)
        elif tabela == 'takes':
            session.execute("""
                CREATE TABLE takes (
                    student_id TEXT,
                    subj_id TEXT,
                    semester INT,
                    year INT,
                    subjroom TEXT,
                    grade DECIMAL,
                    PRIMARY KEY (student_id, subj_id, semester, year)
                );
            """)
            migrar_dados(tabela, colunas, registros)
        elif tabela == 'teaches':
            session.execute("""
                CREATE TABLE teaches (
                    professor_id TEXT,
                    subj_id TEXT,
                    semester INT,
                    year INT,
                    PRIMARY KEY (professor_id, subj_id, semester, year)
                );
            """)
            migrar_dados(tabela, colunas, registros)
        elif tabela == 'req':
            session.execute("""
                CREATE TABLE req (
                    course_id TEXT,
                    subj_id TEXT,
                    PRIMARY KEY (course_id, subj_id)
                );
            """)
            migrar_dados(tabela, colunas, registros)
        elif tabela == 'graduate':
            session.execute("""
                CREATE TABLE graduate (
                    student_id TEXT,
                    course_id TEXT,
                    semester INT,
                    year INT,
                    PRIMARY KEY (student_id, course_id)
                );
            """)
            migrar_dados(tabela, colunas, registros)
    print("Migração de dados (com relacionamentos) concluída com sucesso!")

def drop_tables_from_astra():
    session.set_keyspace("faculdade")
    tabelas = session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'faculdade';")

    print("Deletando tudo...")

    for tabela in tabelas:
        session.execute(f"DROP TABLE {tabela.table_name}")

if __name__ == '__main__':
    drop_tables_from_astra()
    cria_tabelas()
    print("Consultas no AstraDB com Cassandra:")

    if not os.path.exists('./resultados_cassandra'):
        os.makedirs('./resultados_cassandra')

    query_student_academic_record()
    disciplinas_professor()
    alunos_formados ()
    chefes_departamento()
    grupo_de_tcc()