from datetime import datetime
from airflow import DAG
import logging as _log
from airflow.operators.python_operator import PythonOperator
from confluent_kafka import Consumer, Producer, KafkaException
import json

# Настройки Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
INPUT_TOPIC = 'sellers_activity_in'
OUTPUT_TOPIC = 'sellers_activity'

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

        activity_data = json.loads(msg.value().decode('utf-8'))
        _log.info(f"Received context: {activity_data}")

        return activity_data  # Возвращаем контекст для XCom

    except Exception as e:
        _log.info(f"Error: {str(e)}")
    finally:
        consumer.close()

def _validate_message(**kwargs):
    activity_data = kwargs['ti'].xcom_pull(task_ids='consume_message')

    if activity_data is None:
        print("No context to validate")
        return None

    required_fields = [
        "seller_id", "tariff_id", "sale_date", "price",
        "discount", "company_id", "company_name",
        "company_inn", "company_size", "company_created_at"
    ]
    
    for field in required_fields:
        if field not in activity_data:
            raise ValueError(f"Missing required field: {field}")
    
    # Проверка типов и данных
    if not isinstance(activity_data["seller_id"], int):
        raise ValueError("seller_id must be an integer.")

    if not isinstance(activity_data["tariff_id"], int):
        raise ValueError("tariff_id must be an integer.")

    sale_date = activity_data["sale_date"]
    try:
        datetime.strptime(sale_date, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError("sale_date must be in 'YYYY-MM-DD HH:MM:SS' format.")

    if not isinstance(activity_data["price"], (int, float)):
        raise ValueError("price must be a number.")
    if not (10.0 <= activity_data["price"] <= 500.0):
        raise ValueError("price must be between 10.0 and 500.0.")

    if not isinstance(activity_data["discount"], (int, float)):
        raise ValueError("discount must be a number.")
    if not (0.0 <= activity_data["discount"] <= 50.0):
        raise ValueError("discount must be between 0.0 and 50.0.")

    if not isinstance(activity_data["company_id"], int):
        raise ValueError("company_id must be an integer.")
    if not (1 <= activity_data["company_id"] <= 50):
        raise ValueError("company_id must be between 1 and 50.")

    if not isinstance(activity_data["company_name"], str):
        raise ValueError("company_name must be a string.")
    
    if not isinstance(activity_data["company_inn"], int):
        raise ValueError("company_inn must be an integer.")

    company_created_at = activity_data["company_created_at"]
    try:
        datetime.strptime(company_created_at, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError("company_created_at must be in 'YYYY-MM-DD HH:MM:SS' format.")

    return activity_data

def _process_message(**kwargs):
    activity_data = kwargs['ti'].xcom_pull(task_ids='validate_message')
    
    if activity_data is None:
        _log.info("No context to process")
        return None

    # Определение company_type в зависимости от employes_count
    if activity_data['employes_count'] < 100:
        activity_data['company_type'] = 'S'
    elif 100 <= activity_data['employes_count'] < 4000:
        activity_data['company_type'] = 'M'
    else:
        activity_data['company_type'] = 'L'

    return activity_data  # Возвращаем обработанный контекст для передачи его дальше

def _produce_message(**kwargs):
    activity_data = kwargs['ti'].xcom_pull(task_ids='process_message')
    
    if activity_data is None:
        _log.info("No context to produce")
        return

    # Инициализация Producer
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    # Отправка сообщения в другой топик
    producer.produce(OUTPUT_TOPIC, value=json.dumps(activity_data))
    producer.flush()
    _log.info(f"Context sent to {OUTPUT_TOPIC}: {activity_data}")

# Определяем DAG
args = {'owner': 'airflow'}

with DAG(
    dag_id="Sales_Actions_DAG",
    default_args=args,
    description='DAG read sales ativity from Kafka, validate, process and public to Kafka',
    start_date=datetime(2024,11,9),
    tags=['otus'],
    schedule_interval='*/2 * * * *',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1
 ) as dag:
#task init
    consume_task = PythonOperator(task_id='consume_message',python_callable=_consume_message,provide_context=True)
    validate_task = PythonOperator(task_id='validate_message',python_callable=_validate_message,provide_context=True)
    process_task = PythonOperator(task_id='process_message',python_callable=_process_message,provide_context=True)
    produce_task = PythonOperator(task_id='produce_message',python_callable=_produce_message,provide_context=True)

# Определяем порядок выполнения задач
consume_task >> validate_task >> process_task >> produce_task
