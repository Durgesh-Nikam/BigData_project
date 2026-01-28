# 🌾 Smart Farming Decision Support System  
**Big Data | Kafka | Spark | ML | Airflow | Streaming**

---

## 📌 Project Overview

The **Smart Farming Decision Support System** is a Big Data–driven analytics platform designed to support agricultural decision-making using **batch processing**, **machine learning**, and **real-time data streaming**.

The system processes historical agricultural and rainfall datasets using **Apache Spark**, trains ML models using **Spark MLlib**, and applies these models on real-time farming data streamed via **Apache Kafka**. The entire pipeline is orchestrated using **Apache Airflow (Standalone)**.

---

## 🧠 One-Line Summary

Historical agricultural and rainfall datasets are processed using Spark batch jobs to train ML models, while real-time farming data is streamed through Kafka and analyzed using Spark Structured Streaming, orchestrated using Apache Airflow.

---

## 🏗️ Architecture Overview
```bash
Historical CSV Data
└── Kafka Batch Producer
└── Kafka Topic
└── Spark Batch Processing
└── Data Enrichment (Rainfall + Soil)
└── ML Model Training (Spark MLlib)
└── Saved ML Model

Real-Time Data Generator
└── Kafka Stream Producer
└── Kafka Topic
└── Spark Structured Streaming
└── Apply Trained ML Model
└── Yield Predictions
└── Parquet Storage
```

---

## 📂 Project Structure
```bash
BigData_project/
│
├── data/
│ └── historical/
│ ├── indian_agriculture.csv
│ └── Monthly_Rainfall_From_1901_to_2017.csv
│
├── kafka/
│ ├── batch_producer.py
│ └── realtime_producer.py
│
├── spark/
│ ├── batch_processing.py
│ ├── batch_enrichment.py
│ ├── model_training_enriched.py
│ └── streaming_prediction_enriched.py
│
├── models/
│ └── smart_farming_yield_model_enriched
│
├── storage/
│ ├── parquet/
│ │ ├── agriculture_all_crops/
│ │ └── agriculture_ml_enriched/
│ └── streaming_predictions/
│
├── airflow/
│ └── dags/
│ └── smart_farming_dag.py
│
└── README.md
```

---

## 📊 Datasets Used

1. **Indian Agriculture Dataset**
   - Crop area, production, and yield data

2. **Monthly Rainfall Dataset (1901–2017)**
   - State-wise rainfall data

> Soil parameters are simulated using rainfall-based indices for analytical enrichment.

---

## ⚙️ System Requirements

- **OS**: Ubuntu 20.04 / 22.04  
- **Java**: OpenJDK 11  
- **Python**: 3.8+  
- **Apache Kafka**
- **Apache Spark**
- **Apache Airflow (Standalone)**

---

## 🔧 Installation & Setup

### 1️⃣ Install Java
```bash
sudo apt install openjdk-11-jdk -y
java -version
```
### 2️⃣ Setup Apache Kafka
```bash
tar -xzf kafka_2.13-3.x.x.tgz
export KAFKA_HOME=~/kafka_2.13-3.x.x
export PATH=$PATH:$KAFKA_HOME/bin
```

### 3️⃣ Start Kafka Services (Run in Separate Terminals)

Terminal 1 – Zookeeper
```bash
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
```

Terminal 2 – Kafka Broker
```bash
kafka-server-start.sh $KAFKA_HOME/config/server.properties
```

### 4️⃣ Create Kafka Topics (ONE TIME ONLY)

topic - 1
```bash
kafka-topics.sh --create \
--topic agriculture_batch \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1
```
topic - 2
```bash
kafka-topics.sh --create \
--topic agriculture_stream \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1
```


### 5️⃣ Verify Apache Spark
```bash
spark-submit --version
```

### 6️⃣ Start Apache Airflow (Standalone)
```bash
pip install apache-airflow
```
```bash
airflow standalone
```

- Web UI: http://localhost:8080
- Login credentials are printed on first run

---

## 🚀 How to Run the Project
### Step 1: Ensure Kafka is Running

(Zookeeper and Kafka broker must be active)

### Step 2: Start Airflow
```bash
airflow standalone
```

### Step 3: Deploy DAG
```bash
cp airflow/dags/smart_farming_dag.py ~/airflow/dags/
```

### Step 4: Trigger DAG

1. Open Airflow UI
2. Enable DAG: ```bash smart_farming_decision_support ``` 
3. Click Trigger DAG

---

## 🔄 DAG Execution Flow

1. Kafka batch producer sends historical data
2. Spark batch processing transforms data
3. Spark batch enrichment adds rainfall & soil features
4. Spark ML model is trained
5. Kafka real-time producer generates live data
6. Spark Structured Streaming applies ML model on live data
7. Real-time tasks run in parallel and stop gracefully after a fixed duration.

--- 

## 📈 Outputs

- Trained ML model → ```bash models/ ```
- Batch processed data → ```bash storage/parquet/ ```
- Streaming predictions → ```bash storage/streaming_predictions/ ```

---
