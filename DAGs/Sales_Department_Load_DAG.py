
#imports
from datetime import datetime
import requests
import logging as _log
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from confluent_kafka import  Producer
import json

#Global variables
URL = "http://localhost:9000/seller"

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

OUTPUT_TOPIC = 'sellers'

#defs
def _request_data(**context):
    """
    Metod requests by API sales department employee data
    """

    url = URL
    payload = {}
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    
    req_seller = requests.request("GET", url, headers=headers, data=payload)
    seller_json = req_seller.json()['results'][0]
    context["task_instance"].xcom_push(key="seller_json", value=seller_json)

def _validate_data(**context):
    """
    Metod validate incoming JSON into structure
    """

    seller_json = context["task_instance"].xcom_pull(task_ids="request_data", key="seller_json")
    seller_data = json.loads(seller_json)

    if not isinstance(seller_data["id"], int) or seller_data["id"] <= 0:
        _log.info("seller_data ID must be a positive integer.")
    
    if not isinstance(seller_data["name"], str) or not seller_data["name"].strip():
        _log.info("seller_data name must be a non-empty string.")
    
    if not isinstance(seller_data["position"], str) or not seller_data["position"].strip():
        _log.info("seller_data position must be a non-empty string.")
    
    if not isinstance(seller_data["phone"], str) or not (seller_data["phone"].startswith('+7') and len(seller_data["phone"]) == 12):
        _log.info("Phone must be a string starting with '+7' and 11 digits long.")
    
    if not isinstance(seller_data["department_code"], str) or not seller_data["department_code"].strip():
        _log.info("Department code must be a non-empty string.")
    
    try:
        datetime.strptime(seller_data["employed_at"], '%Y-%m-%d %H:%M:%S')
    except ValueError:
        _log.info("Employed at must be in 'YYYY-MM-DD HH:MM:SS' format.")
    
    context["task_instance"].xcom_push(key="seller_data", value=seller_data)


def _produce_message(**context):
    seller_data = context['task_instance'].xcom_pull(task_ids='process_message')
    
    if context is None:
        _log.info("No context to produce")
        return

    # Инициализация Producer
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    # Отправка сообщения в другой топик
    producer.produce(OUTPUT_TOPIC, value=json.dumps(seller_data))
    producer.flush()
    _log.info(f"Context sent to {OUTPUT_TOPIC}: {seller_data}")

#DAG init
args = {'owner': 'airflow'}

with DAG(
    dag_id="Sales_Department_Load_DAG",
    default_args=args,
    description='DAG validates sellers data from API to send it Kafka',
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
