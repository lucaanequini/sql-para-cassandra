# Standard
import os
import json

# Third Party
from dotenv import load_dotenv
from cassandra.cluster import Cluster, ResultSet
from cassandra.auth import PlainTextAuthProvider

load_dotenv()

cloud_config= {
  'secure_connect_bundle': os.getenv('ASTRA_SECURE_BUNDLE')
}
auth_provider = PlainTextAuthProvider(os.getenv('CLIENT_ID'), os.getenv('CLIENT_SECRET'))
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

def query_student_academic_record():
    print("Buscando o histórico escolar do aluno de RA 241220555")
    session.set_keyspace("faculdade")
    relacao_estudante: ResultSet = session.execute("SELECT * FROM takes WHERE student_id = '241220555';")

    resultado = []

    for r in relacao_estudante:
        subj: ResultSet = session.execute(f"SELECT * FROM subj WHERE id = '{r.subj_id}'")
        resultado.append({ 
            "subj_id": r.subj_id,
            "title": subj[0].title,
            "grade": float(r.grade),
            "semester": r.semester,
            "year": r.year
        })

    with open('./resultados_cassandra/historico-escolar.json', 'w') as f:
        json.dump({ "student_id": "241220555", "historico": resultado }, f, ensure_ascii=False)

def disciplinas_professor():
    print("Buscando o histórico de disciplinas ministradas pelo professor de ID P010")
    session.set_keyspace("faculdade")
    relacao_professor: ResultSet = session.execute("SELECT * FROM teaches WHERE professor_id = 'P010';")

    resultado = []

    for r in relacao_professor:
        subj: ResultSet = session.execute(f"SELECT * FROM subj WHERE id = '{r.subj_id}'")
        resultado.append({ 
            "subj_id": r.subj_id,
            "title": subj[0].title,
            "semester": r.semester,
            "year": r.year
        })

    with open('./resultados_cassandra/disc-professor.json', 'w') as f:
        json.dump({ "professor_id": "P010", "leciona": resultado }, f, ensure_ascii=False)

def alunos_formados():
    print("Buscando os alunos que se formaram no segundo semestre de 2018")
    session.set_keyspace("faculdade")
    relacao_formando: ResultSet = session.execute("SELECT * FROM graduate WHERE year = 2018 AND semester = 2 ALLOW FILTERING;")

    resultado = []

    for r in relacao_formando:
        student: ResultSet = session.execute(f"SELECT * FROM student WHERE id = '{r.student_id}'")
        resultado.append({ 
            "student_id": r.student_id,
            "student_name": student[0].name
        })

    with open('./resultados_cassandra/alunos-formados.json', 'w') as f:
        json.dump({ "semester": 2, "year": 2018, "formados": resultado }, f, ensure_ascii=False)

def chefes_departamento():
    print("Buscando os professores que são chefes de departamento")
    session.set_keyspace("faculdade")
    relacao_chefe: ResultSet = session.execute("SELECT * FROM department;")

    resultado = []

    for r in relacao_chefe:
        professor: ResultSet = session.execute(f"SELECT * FROM professor WHERE id = '{r.boss_id}'")
        resultado.append({ 
            "professor_id": r.boss_id,
            "professor_name": professor[0].name,
            "department": r.dept_name,
            "budget": float(r.budget)
        })

    with open('./resultados_cassandra/prof-chefes.json', 'w') as f:
        json.dump(resultado, f, ensure_ascii=False)

def grupo_de_tcc():
    print("Buscando os alunos que formaram o grupo de TCC de ID CC1234567")
    session.set_keyspace("faculdade")

    resultado = []

    student: ResultSet = session.execute(f"SELECT * FROM student WHERE group_id = 'CC1234567' ALLOW FILTERING;")
    resultado.append({ 
        "student_id": student[0].id,
        "student_name": student[0].name
    })

    with open('./resultados_cassandra/grupo-tcc.json', 'w') as f:
        json.dump(resultado, f, ensure_ascii=False)