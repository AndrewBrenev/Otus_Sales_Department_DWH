import json
import logging as _log

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from confluent_kafka import Consumer, Producer, KafkaException

# Настройки Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
INPUT_TOPIC = 'companies_activity_data_in'
OUTPUT_TOPIC = 'companies_activity_data'

def _consume_message():
    # Инициализация Consumer
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([INPUT_TOPIC])

    try:
        msg = consumer.poll(1.0)  # Ждем сообщения в течение 1 сек
        if msg is None:
            _log.info("No message received")
            return None  # Если сообщений нет, завершаем таск без ошибок

        if msg.error():
            raise KafkaException(msg.error())

        context = json.loads(msg.value().decode('utf-8'))
        _log.info(f"Received context: {context}")

        return context  # Возвращаем контекст для XCom

    except Exception as e:
        _log.info(f"Error: {str(e)}")
    finally:
        consumer.close()

def _validate_message(**kwargs):
    activity_data = kwargs['ti'].xcom_pull(task_ids='consume_message')
    
    if activity_data is None:
        _log.info("No context to validate")
        return None

  
    # Проверка наличия необходимых ключей
    required_keys = ["id", "company_id", "event_time", "event_type"]
    for key in required_keys:
        if key not in activity_data:
            _log.info(f"Missing required key: {key}")
    
    # Проверка типа id
    if not isinstance(activity_data["id"], int) or not (1 <= activity_data["id"] <= 1000):
        _log.info("ID must be an integer between 1 and 1000.")
    
    # Проверка типа company_id
    if not isinstance(activity_data["company_id"], int) or not (1 <= activity_data["company_id"] <= 50):
        _log.info("Company ID must be an integer between 1 and 50.")
    
    # Проверка event_time
    try:
        datetime.strptime(activity_data["event_time"], '%Y-%m-%d %H:%M:%S')
    except ValueError:
        _log.info("Event time must be a string in format 'YYYY-MM-DD HH:MM:SS'.")

    # Проверка event_type
    if activity_data["event_type"] not in ['SIGN', 'CANCEL', 'REQUEST', 'APPROVE']:
        _log.info("Event type must be one of: 'SIGN', 'CANCEL', 'REQUEST', 'APPROVE'.")

    return activity_data

def _produce_message(**kwargs):
    context = kwargs['ti'].xcom_pull(task_ids='process_message')
    
    if context is None:
        _log.info("No context to produce")
        return

    # Инициализация Producer
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    # Отправка сообщения в другой топик
    producer.produce(OUTPUT_TOPIC, value=json.dumps(context))
    producer.flush()
    _log.info(f"Context sent to {OUTPUT_TOPIC}: {context}")

# Определяем DAG
args = {'owner': 'airflow'}

with DAG(
    dag_id="Companies_Actions_DAG",
    default_args=args,
    description='DAG read company ativity from Kafka, validate, process and public to Kafka',
    start_date=datetime(2024,11,9),
    tags=['otus'],
    schedule_interval='* * * * *',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1
 ) as dag:
#task init
    consume_task = PythonOperator(task_id='consume_message',python_callable=_consume_message,provide_context=True)
    validate_task = PythonOperator(task_id='validate_message',python_callable=_validate_message,provide_context=True)
    produce_task = PythonOperator(task_id='produce_message',python_callable=_produce_message,provide_context=True)

# Определяем порядок выполнения задач
consume_task >> validate_task  >> produce_task
