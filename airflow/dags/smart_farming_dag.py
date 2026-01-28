
# this th final dag
# ---------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="smart_farming_decision_support",
    description="Smart Farming Big Data Pipeline (Kafka + Spark + ML)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bigdata", "spark", "kafka", "ml"],
) as dag:

    # 1️⃣ Batch Kafka Producer (Historical Data)
    batch_kafka_producer = BashOperator(
        task_id="batch_kafka_producer",
        bash_command="""
        python3 ~/Downloads/BigData_project/kafka/batch_producer.py
        """,
    )

    # 2️⃣ Spark Batch Processing
    spark_batch_processing = BashOperator(
        task_id="spark_batch_processing",
        bash_command="""
        spark-submit ~/Downloads/BigData_project/spark/batch_processing.py
        """,
    )

    # 3️⃣ Spark Batch Enrichment
    spark_batch_enrichment = BashOperator(
        task_id="spark_batch_enrichment",
        bash_command="""
        spark-submit ~/Downloads/BigData_project/spark/batch_enrichment.py
        """,
    )

    # 4️⃣ ML Model Training
    ml_model_training = BashOperator(
        task_id="ml_model_training",
        bash_command="""
        spark-submit ~/Downloads/BigData_project/spark/model_training_enriched.py
        """,
    )

    # 5️⃣ Real-time Kafka Producer (IMPORTANT)
    realtime_kafka_producer = BashOperator(
        task_id="realtime_kafka_producer",
        bash_command="""
        python3 ~/Downloads/BigData_project/kafka/realtime_producer.py
        """,
    )

    # 6️⃣ Spark Structured Streaming
    spark_streaming_predictions = BashOperator(
        task_id="spark_streaming_predictions",
        bash_command="""
        spark-submit ~/Downloads/BigData_project/spark/streaming_prediction_enriched.py
        """,
    )

    # DAG Dependencies
    batch_kafka_producer \
        >> spark_batch_processing \
        >> spark_batch_enrichment \
        >> ml_model_training \
        >> realtime_kafka_producer \
        >> spark_streaming_predictions






# ---------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------















































from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="smart_farming_decision_support_new",
    description="Kafka + Spark + ML Smart Farming Pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",   # ✅ Airflow 2.x (NOT schedule_interval)
    catchup=False,
    tags=["bigdata", "spark", "kafka", "ml"],
) as dag:

    start_kafka_batch = BashOperator(
        task_id="kafka_batch_producer",
        bash_command="python3 ~/Downloads/BigData_project/kafka/batch_producer.py"
    )

    spark_batch = BashOperator(
        task_id="spark_batch_processing",
        bash_command="spark-submit ~/Downloads/BigData_project/spark/batch_processing.py"
    )

    spark_enrichment = BashOperator(
        task_id="spark_batch_enrichment",
        bash_command="spark-submit ~/Downloads/BigData_project/spark/batch_enrichment.py"
    )

    model_training = BashOperator(
        task_id="ml_model_training",
        bash_command="spark-submit ~/Downloads/BigData_project/spark/model_training_enriched.py"
    )

    spark_streaming = BashOperator(
        task_id="spark_streaming_predictions",
        bash_command="spark-submit ~/Downloads/BigData_project/spark/streaming_prediction_enriched.py"
    )

    start_kafka_batch >> spark_batch >> spark_enrichment >> model_training >> spark_streaming

























# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime

# # -----------------------------
# # Default arguments
# # -----------------------------
# default_args = {
#     "owner": "airflow",
#     "retries": 1,
# }

# # -----------------------------
# # DAG definition (Airflow 2.x)
# # -----------------------------
# with DAG(
#     dag_id="smart_farming_decision_support",
#     description="Big Data Smart Farming Pipeline (Kafka + Spark + ML)",
#     default_args=default_args,
#     start_date=datetime(2024, 1, 1),
#     schedule="@daily",          # Airflow 2.x syntax
#     catchup=False,
#     tags=["bigdata", "spark", "kafka", "ml", "farming"],
# ) as dag:

#     # -----------------------------
#     # 1. Start Kafka Producers
#     # -----------------------------
#     start_kafka_producers = BashOperator(
#         task_id="start_kafka_producers",
#         bash_command="""
#         echo "Starting Kafka Producers"
#         python3 ~/Downloads/BigData_project/kafka/batch_producer.py &
#         python3 ~/Downloads/BigData_project/kafka/realtime_producer.py &
#         """,
#     )

#     # -----------------------------
#     # 2. Spark Batch Processing
#     # -----------------------------
#     spark_batch_processing = BashOperator(
#         task_id="spark_batch_processing",
#         bash_command="""
#         echo "Running Spark Batch Job"
#         spark-submit ~/Downloads/BigData_project/spark/batch_processing.py
#         """,
#     )


# 		# -----------------------------
#     # 3. Spark Batch enrichment
#     # -----------------------------
#     spark_batch_enrichment = BashOperator(
#         task_id="spark_batch_enrichment",
#         bash_command="""
#         echo "Running Spark Batch Job"
#         spark-submit ~/Downloads/BigData_project/spark/batch_enrichment.py
#         """,
#     )
    
    
#     # -----------------------------
#     # 4. ML Model Training
#     # -----------------------------
#     model_training = BashOperator(
#         task_id="ml_model_training",
#         bash_command="""
#         echo "Training ML Models"
#         spark-submit ~/Downloads/BigData_project/spark/model_training_enriched.py
#         """,
#     )

#     # -----------------------------
#     # 4. Spark Structured Streaming
#     # -----------------------------
#     spark_streaming = BashOperator(
#         task_id="spark_streaming_predictions",
#         bash_command="""
#         echo "Starting Spark Streaming"
#         spark-submit ~/Downloads/BigData_project/spark/streaming_prediction_enriched.py
#         """,
#     )

# #    # -----------------------------
# #    # 5. Store Predictions
# #    # -----------------------------
# #    store_predictions = BashOperator(
# #        task_id="store_predictions",
# #        bash_command="""
# #        echo "Storing predictions to Parquet / DB"
# #        python3 ~/Downloads/BigData_project/storage/store_predictions.py
# #        """,
# #    )


#     # -----------------------------
#     # DAG Flow
#     # -----------------------------
#     start_kafka_producers \
#         >> spark_batch_processing \
#         >> spark_batch_enrichment \
#         >> model_training \
#         >> spark_streaming \
# #        >> store_predictions






