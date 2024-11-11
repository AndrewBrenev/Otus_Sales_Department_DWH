import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

# Настройки Kafka
KAFKA_BROKER = 'localhost:9092'  # Укажите адрес вашего Kafka брокера
KAFKA_TOPIC = 'companies_activity'  # Укажите имя топика

# Функция для генерации случайной даты
def random_date(start_date, end_date):
    return start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))

# Генерация случайных данных
def generate_company_activity():
    id = random.randint(1, 1000)  # id от 1 до 1000
    company_id = random.randint(1, 50)  # company_id от 1 до 50
    event_time = random_date(datetime(2024, 9, 1), datetime(2024, 12, 31))
    event_type = random.choice(['SIGN', 'CANCEL', 'REQUEST', 'APPROVE'])  # Случайный тип события

    return {
        "id": id,
        "company_id": company_id,
        "event_time": event_time.strftime('%Y-%m-%d %H:%M:%S'),
        "event_type": event_type
    }

# Инициализация Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Генерация и отправка 30 сообщений
for _ in range(30):  # Отправка 30 сообщений
    activity = generate_company_activity()
    producer.send(KAFKA_TOPIC, activity)
    print("Отправленное сообщение:", activity)

# Закрытие продюсера
producer.flush()
producer.close()

# Вывод сгенерированных сообщений
print("Отправленное сообщение:", activity)