# 🚦 Mobility Analytics — Real-Time Traffic Data Engineering Pipeline

A production-style, end-to-end streaming data pipeline that ingests, cleans, transforms, and serves real-time urban traffic data using **Apache Kafka**, **PySpark Structured Streaming**, **Delta Lake**, and **Power BI**.

> Medallion Architecture (Bronze → Silver → Gold) built entirely from scratch.

---

## 📐 Architecture Overview

```
Traffic Producer (Python)
        │
        ▼
   Apache Kafka
  (traffic-topic)
        │
        ▼
  ┌─────────────┐
  │   BRONZE    │  Raw ingestion — Kafka → Delta Lake
  └─────────────┘
        │
        ▼
  ┌─────────────┐
  │   SILVER    │  DQ checks, deduplication, type casting, feature engineering
  └─────────────┘
        │
        ▼
  ┌─────────────┐
  │    GOLD     │  Star schema — Fact + Dimension tables
  └─────────────┘
        │
        ▼
  Hive Metastore → Spark Thrift Server → Power BI
```

---

## 🧰 Tech Stack

| Layer | Tool |
|---|---|
| Containerization | Docker / Docker Compose |
| Message Broker | Apache Kafka (KRaft mode) |
| Stream Processing | Apache Spark 3.5.1 / PySpark |
| Storage Format | Delta Lake 3.2.0 |
| Metastore | Apache Hive 3.1.3 + PostgreSQL 13 |
| BI / Dashboarding | Power BI (via Thrift Server / ODBC) |
| Data Simulation | Python (`kafka-python`, `faker`, `pytz`) |

---

## 📁 Project Structure

```
mobility-analytics/
├── apps/
│   ├── traffic_dirty_producer.py   # Simulates real-world dirty data
│   ├── traffic_bronze.py           # Bronze: Kafka → Delta raw ingestion
│   ├── traffic_silver.py           # Silver: DQ, cleaning, feature engineering
│   └── traffic_gold.py             # Gold: Star schema (fact + dims)
├── hive-conf/
│   └── hive-site.xml               # Hive metastore configuration
├── warehouse/                      # Shared Delta Lake storage (bind-mounted)
├── spark-ivy/                      # Ivy cache for Spark packages
├── docker-compose.yml
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Python 3.8+ with pip
- Power BI Desktop (for dashboarding)

### 1. Start the Infrastructure

```bash
docker compose up -d
```

This starts: Kafka, Kafka UI, Spark Master, Spark Worker, Hive Metastore, and PostgreSQL.

| Service | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| Kafka UI | http://localhost:8090 |
| Hive Metastore | thrift://localhost:9083 |

---

### 2. Install Producer Dependencies

```bash
pip install kafka-python faker pytz
```

### 3. Create the Kafka Topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic traffic-topic \
  --bootstrap-server kafka:9092 \
  --partitions 3 --replication-factor 1
```

### 4. Start the Traffic Producer

```bash
python apps/traffic_dirty_producer.py
```

Produces a mix of ~70% clean and ~30% intentionally dirty events to simulate real-world conditions.

---

### 5. Run the Pipeline Layers

Run each layer in a **separate terminal**:

```bash
# Bronze — raw ingestion from Kafka
docker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/traffic_bronze.py

# Silver — cleaning, validation, deduplication
docker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/traffic_silver.py

# Gold — star schema output
docker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/traffic_gold.py
```

---

### 6. Register Tables in Hive & Query via Spark SQL

```bash
docker exec -it spark-worker bash

/opt/spark/bin/spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.jars.ivy=/tmp/.ivy \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.sql.warehouse.dir=/tmp/spark-warehouse
```

Then run the SQL in `SQL.txt` to create the `mobility` database, register Delta tables, and create BI views.

---

### 7. Connect Power BI via Thrift Server

Start the Spark Thrift Server on the worker node:

```bash
# Download Delta jars first (one-time)
cd /opt/spark/jars
wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar
wget https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

# Start Thrift Server
/opt/spark/sbin/start-thriftserver.sh \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.sql.warehouse.dir=/opt/spark/warehouse
```

In Power BI Desktop: **Get Data → Spark** → connect to `localhost:10000`.

---

## 🧪 Dirty Data Types Simulated

The producer deliberately generates the following fault types to test pipeline resilience:

| Type | Description |
|---|---|
| `null_speed` | Speed field is `null` |
| `negative_speed` | Speed is a negative integer |
| `extreme_speed` | Speed exceeds 160 km/h |
| `duplicate_vehicle` | Replays a previously seen vehicle ID |
| `late_event` | Event timestamp 10–120 minutes in the past |
| `future_event` | Event timestamp up to 60 minutes ahead |
| `wrong_datatype` | Speed sent as a string (`"FAST"`) |
| `schema_drift` | Extra unknown field added to payload |
| `corrupt_json` | Completely malformed payload |

---

## 🗂️ Data Model (Star Schema)

### `fact_traffic`
| Column | Type | Description |
|---|---|---|
| `vehicle_id` | STRING | Unique vehicle identifier |
| `road_id` | STRING | Road segment ID |
| `city_zone` | STRING | Urban zone |
| `speed_int` | DOUBLE | Vehicle speed (km/h) |
| `congestion_level` | INT | 1–5 congestion score |
| `event_ts` | TIMESTAMP | Event time |
| `peak_flag` | STRING | Peak hour indicator |
| `speed_band` | STRING | LOW / MEDIUM / HIGH |
| `weather` | STRING | Weather condition |
| `date` | DATE | Partition date |
| `hour` | INT | Hour of event |

### `dim_zone`
| Column | Type | Description |
|---|---|---|
| `city_zone` | STRING | Zone identifier |
| `zone_type` | STRING | Commercial / IT HUB / Transit Hub / Residential |
| `traffic_risk` | STRING | HIGH / MEDIUM / LOW |

### `dim_road`
| Column | Type | Description |
|---|---|---|
| `road_id` | STRING | Road identifier |
| `road_type` | STRING | Highway / City Road |
| `speed_limit` | INT | Speed limit (km/h) |

---
