CREATE DATABASE sales_statistics;

--Информация по тарифам организации
CREATE TABLE tariffs (
    id UInt32,
    name String,
    description String,
    price Float64,
    start_at DateTime DEFAULT now(),
    ends_at DateTime,
    tariff_size UInt32,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

CREATE TABLE kafka_tariffs (
    id UInt32,
    name String,
    description String,
    price Float64,
    start_at DateTime,
    ends_at DateTime,
    tariff_size UInt32,
    updated_at DateTime
) ENGINE = Kafka SETTINGS
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'tariffs',
    kafka_group_name = 'tariffs_group',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW mv_tariffs TO tariffs AS 
SELECT * FROM kafka_tariffs;

--Таблица сотрудников отдела продаж
CREATE TABLE sellers (
    id UInt32,
    name String,
    position LowCardinality(String), 
    phone String,
    department_code LowCardinality(String),
    employed_at DateTime DEFAULT now(),
    fired_at DateTime,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree( updated_at)
ORDER BY id;

CREATE TABLE kafka_sellers (
    id UInt32,
    name String,
    position String, 
    phone String,
    department_code String,
    employed_at DateTime,
    fired_at DateTime,
    updated_at DateTime
) ENGINE = Kafka SETTINGS
    kafka_broker_list = 'localhost:9092', 
    kafka_topic_list = 'sellers',
    kafka_group_name = 'sellers_group',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW mv_sellers TO sellers AS 
SELECT * FROM kafka_sellers;

--Таблица активности подключенных компаний
CREATE TABLE company (
    id UInt32,
    name String,
    inn UInt32,
    size LowCardinality(String),   -- S/M/L
    created_at DateTime DEFAULT now(),
    company_type LowCardinality(String),  -- IP/SELF/OOO
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;


CREATE TABLE companies_activity (
    id UInt32,
    company_id UInt32,
    event_time DateTime,
    event_type LowCardinality(String)  --SIGN/CANCEL/REQUEST/APROVE
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE kafka_companies_activity (
    id UInt32,
    company_id UInt32,
    event_time DateTime,
    event_type String
) ENGINE = Kafka SETTINGS
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'companies_activity',
    kafka_group_name = 'companies_activity_group',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW mv_companies_activity TO companies_activity AS 
SELECT * FROM kafka_companies_activity;

--Таблица фактов продаж тарифов другим компаниям 
CREATE TABLE sellers_activity (
    seller_id UInt32,
    company_id UInt32,
    tariff_id UInt32,
    sale_date DateTime,
    price Float64,
    discount Float64 DEFAULT 0,
    sales_date DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (seller_id, company_id);

CREATE TABLE kafka_sellers_activity (
    seller_id UInt32,
    tariff_id UInt32,
    sale_date DateTime,
    price Float64,
    discount Float64,
    company_id UInt32,
    company_name String,
    company_inn UInt32,
    company_size String,
    company_created_at DateTime
) ENGINE = Kafka SETTINGS
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'sellers_activity',
    kafka_group_name = 'sellers_activity_group',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW mv_sellers_activity_seller TO sellers_activity AS 
SELECT seller_id,
    tariff_id,
    sale_date,
    price,
    discount,
    company_id FROM kafka_sellers_activity;

CREATE MATERIALIZED VIEW mv_sellers_activity_company TO company AS 
SELECT company_id,
    company_name,
    company_inn,
    company_size,
    company_created_at FROM kafka_sellers_activity;