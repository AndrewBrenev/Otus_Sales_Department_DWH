
#imports
from datetime import datetime
import requests
import logging as _log

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

#Global variables
URL = "https://opentdb.com/api.php?amount=1"

DB_CONN_ID = "postgres_otus"

INSERT_QUERY = """
    INSERT INTO quiz_questions (type, difficulty, category, question, correct_answer)
    VALUES('{c1}','{c2}','{c3}','{c4}','{c5}');
"""

#defs
def _request_data(**context):
    """
    Metod requests by API quiz questions data
    """

    url = URL
    payload = {}
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    
    req_quiz = requests.request("GET", url, headers=headers, data=payload)
    quiz_json = req_quiz.json()['results'][0]
    context["task_instance"].xcom_push(key="quiz_json", value=quiz_json)

def _parse_data(**context):
    """
    Metod pasre incoming JSON into structure
    """

    quiz_json = context["task_instance"].xcom_pull(task_ids="request_data", key="quiz_json")
    quiz_data = [quiz_json['type'], quiz_json['difficulty'], quiz_json['category'], quiz_json['question'],quiz_json['correct_answer'] ]
    _log.info(quiz_data)

    context["task_instance"].xcom_push(key="quiz_data", value=quiz_data)

def _insert_data(**context):
    """
    Metod insert incoming quiz question structure into PostgreSQL DB
    """

    quiz_data = context["task_instance"].xcom_pull(task_ids="parse_data", key="quiz_data")
    dest = PostgresHook(postgres_conn_id=DB_CONN_ID)
    dest_conn = dest.get_conn()
    dest_cursor = dest_conn.cursor()

    dest_cursor.execute(INSERT_QUERY.format(c1=quiz_data[0],c2=quiz_data[1],c3=quiz_data[2],c4=quiz_data[3],c5=quiz_data[4]))
    dest_conn.commit()
    dest_conn.close()

#DAG init
args = {'owner': 'airflow'}

with DAG(
    dag_id="OTUS_HW_DAG",
    default_args=args,
    description='DAG parses quiz questions from API to PostgreSQL DB',
    start_date=datetime(2024,9,1),
    tags=['otus'],
    schedule_interval='*/2 * * * *',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1
 ) as dag:
#task init
    request_data = PythonOperator(task_id='request_data', python_callable=_request_data, provide_context=True)
    parse_data = PythonOperator(task_id='parse_data', python_callable=_parse_data, provide_context=True)
    insert_data = PythonOperator(task_id='insert_data', python_callable=_insert_data, provide_context=True)

#task instance order
request_data >> parse_data >> insert_data
