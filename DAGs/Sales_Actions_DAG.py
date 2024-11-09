from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from confluent_kafka import Consumer, Producer, KafkaException
import json

# Настройки Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
INPUT_TOPIC = 'input_topic'
OUTPUT_TOPIC = 'output_topic'

def consume_message(**kwargs):
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
            print("No message received")
            return None  # Если сообщений нет, завершаем таск без ошибок

        if msg.error():
            raise KafkaException(msg.error())

        context = json.loads(msg.value().decode('utf-8'))
        print(f"Received context: {context}")

        return context  # Возвращаем контекст для XCom

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        consumer.close()

def validate_message(**kwargs):
    context = kwargs['ti'].xcom_pull(task_ids='consume_message')
    
    if context is None:
        print("No context to validate")
        return None

    required_fields = ['seller_id', 'company_name', 'company_inn', 'date_begin_contract', 'tariff_code', 'employes_count']

    for field in required_fields:
        if field not in context:
            raise ValueError(f'Missing field: {field}')

    # Дополнительные проверки (например, типы данных)
    if not isinstance(context['seller_id'], int):
        raise ValueError('Invalid type for seller_id, expected int')
    if not isinstance(context['company_name'], str):
        raise ValueError('Invalid type for company_name, expected str')
    if not isinstance(context['company_inn'], int):
        raise ValueError('Invalid type for company_inn, expected int')
    if not isinstance(context['date_begin_contract'], str):
        raise ValueError('Invalid type for date_begin_contract, expected str')
    if not isinstance(context['tariff_code'], str):
        raise ValueError('Invalid type for tariff_code, expected str')
    if not isinstance(context['employes_count'], int):
        raise ValueError('Invalid type for employes_count, expected int')

    return context

def process_message(**kwargs):
    context = kwargs['ti'].xcom_pull(task_ids='validate_message')
    
    if context is None:
        print("No context to process")
        return None

    # Определение company_type в зависимости от employes_count
    if context['employes_count'] < 100:
        context['company_type'] = 'S'
    elif 100 <= context['employes_count'] < 4000:
        context['company_type'] = 'M'
    else:
        context['company_type'] = 'L'

    return context  # Возвращаем обработанный контекст для передачи его дальше

def produce_message(**kwargs):
    context = kwargs['ti'].xcom_pull(task_ids='process_message')
    
    if context is None:
        print("No context to produce")
        return

    # Инициализация Producer
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    # Отправка сообщения в другой топик
    producer.produce(OUTPUT_TOPIC, value=json.dumps(context))
    producer.flush()
    print(f"Context sent to {OUTPUT_TOPIC}: {context}")

# Определяем DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

dag = DAG('kafka_message_processing', default_args=default_args, schedule_interval='@once')

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
    consume_task = PythonOperator(task_id='consume_message',python_callable=consume_message,provide_context=True)
    validate_task = PythonOperator(task_id='validate_message',python_callable=validate_message,provide_context=True)
    process_task = PythonOperator(task_id='process_message',python_callable=process_message,provide_context=True)
    produce_task = PythonOperator(task_id='produce_message',python_callable=produce_message,provide_context=True)

# Определяем порядок выполнения задач
consume_task >> validate_task >> process_task >> produce_task


