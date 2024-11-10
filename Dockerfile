FROM apache/airflow:2.5.0

# Вернуться к пользователю airflow
USER airflow

# Установить Python пакеты
RUN pip install --no-cache-dir confluent_kafka