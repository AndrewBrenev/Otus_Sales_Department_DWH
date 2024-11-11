import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

# Настройки Kafka
KAFKA_BROKER = 'localhost:9092'  # Укажите адрес вашего Kafka брокера
KAFKA_TOPIC = 'sellers_activity'  # Укажите имя топика

# Функция для генерации случайной даты
def random_date(start_date, end_date):
    return start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))

# Генерация случайных данных
def generate_seller_activity():
    seller_id = random.randint(1, 100)  # seller_id от 1 до 100
    tariff_id = random.randint(1, 10)    # tariff_id от 1 до 10
    sale_date = random_date(datetime(2023, 1, 1), datetime(2024, 12, 31))
    price = round(random.uniform(10.0, 500.0), 2)  # Случайная цена от 10.0 до 500.0
    discount = round(random.uniform(0.0, 50.0), 2)  # Случайная скидка от 0 до 50.0
    company_id = random.randint(1, 50)  # company_id от 1 до 50
    company_name = f"Company_{company_id}"
    company_inn = random.randint(1000000000, 9999999999)  # Случайный ИНН
    company_size = random.choice(['small', 'medium', 'large'])  # Случайный размер компании
    company_created_at = random_date(datetime(2010, 1, 1), datetime(2023, 1, 1))

    return {
        "seller_id": seller_id,
        "tariff_id": tariff_id,
        "sale_date": sale_date.strftime('%Y-%m-%d %H:%M:%S'),
        "price": price,
        "discount": discount,
        "company_id": company_id,
        "company_name": company_name,
        "company_inn": company_inn,
        "company_size": company_size,
        "company_created_at": company_created_at.strftime('%Y-%m-%d %H:%M:%S')
    }

# Инициализация Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Генерация и отправка данных
for _ in range(20):  # Отправка 20 сообщений
    activity = generate_seller_activity()
    producer.send(KAFKA_TOPIC, activity)
    print("Отправленное сообщение:", activity)

# Закрытие продюсера
producer.flush()
producer.close()

print("Данные успешно отправлены в Kafka!")