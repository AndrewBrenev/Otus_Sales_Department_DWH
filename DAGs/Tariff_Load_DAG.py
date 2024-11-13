
#imports
from datetime import datetime
import requests
import logging as _log
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from confluent_kafka import  Producer
import json

#Global variables
URL = "https://localhost:9000/tariff"

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

OUTPUT_TOPIC = 'tariffs'

#defs
def _request_data(**context):
    """
    Metod requests by API sales department employee data
    """

    payload = {}
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    
    req_tariff = requests.request("GET", URL, headers=headers, data=payload)
    tariff_json = req_tariff.json()
    _log.info(tariff_json)
    context["task_instance"].xcom_push(key="tariff_json", value=tariff_json)

def _validate_data(**context):
    """
    Metod validate incoming structure
    """

    tariff_json = context["task_instance"].xcom_pull(task_ids="request_data", key="tariff_json")

    tariff_data = json.loads(tariff_json)

    if not isinstance(tariff_data["id"], int) or tariff_data["id"] <= 0:
        _log.info("Tariff ID must be a positive integer.")

    if not isinstance(tariff_data["name"], str) or not tariff_data["name"].strip():
        _log.info("Tariff name must be a non-empty string.")

    if not isinstance(tariff_data["description"], str):
        _log.info("Tariff description must be a string.")

    if not isinstance(tariff_data["price"], (int, float)) or not (990 <= tariff_data["price"] <= 99990):
        _log.info("Price must be a number between 990 and 99990.")

    if not isinstance(tariff_data["start_at"], str):  # Так как мы изменили формат
        _log.info("Start date must be a string in ISO format.")

    if not isinstance(tariff_data["ends_at"], str):
        _log.info("End date must be a string in ISO format.")

    start_at = datetime.fromisoformat(tariff_data["start_at"])
    ends_at = datetime.fromisoformat(tariff_data["ends_at"])
    
    if start_at >= ends_at:
        _log.info("Start date must be before end date.")

    if not isinstance(tariff_data["tariff_size"], int) or not (1 <= tariff_data["tariff_size"] <= 6):
        _log.info("Tariff size must be an integer between 1 and 6.")

    context["task_instance"].xcom_push(key="tariff_data", value=tariff_data)


def _produce_message(**context):
    tariff_data = context['task_instance'].xcom_pull(task_ids='process_message')
    
    if context is None:
        _log.info("No context to produce")
        return

    # Инициализация Producer
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    # Отправка сообщения в другой топик
    producer.produce(OUTPUT_TOPIC, value=json.dumps(tariff_data))
    producer.flush()
    _log.info(f"Context sent to {OUTPUT_TOPIC}: {tariff_data}")

#DAG init
args = {'owner': 'airflow'}

with DAG(
    dag_id="Tariff_Load_DAG",
    default_args=args,
    description='DAG validates tariffs data from API to send it Kafka',
    start_date=datetime(2024,9,1),
    tags=['otus'],
    schedule_interval='0 0 * * *',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1
 ) as dag:
#task init
    request_data = PythonOperator(task_id='request_data', python_callable=_request_data, provide_context=True)
    validate_data = PythonOperator(task_id='validate_data', python_callable=_validate_data, provide_context=True)
    produce_message = PythonOperator(task_id='produce_message', python_callable=_produce_message, provide_context=True)

#task instance order
request_data >> validate_data >> produce_message
