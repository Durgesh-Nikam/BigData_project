from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

spark = SparkSession.builder \
    .appName("SmartFarming-Streaming-Prediction") \
    .getOrCreate()

# Load trained enriched ML model
model = PipelineModel.load(
    "/home/sunbeam/Downloads/BigData_project/models/smart_farming_yield_model_enriched"
)

# Kafka stream
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "agriculture_stream") \
    .load()

schema = StructType([
    StructField("year", IntegerType()),
    StructField("crop", StringType()),
    StructField("area", DoubleType()),
    StructField("production", DoubleType()),
    StructField("annual_rainfall", DoubleType()),
    StructField("kharif_rainfall", DoubleType()),
    StructField("soil_moisture_index", DoubleType()),
    StructField("soil_quality_index", DoubleType())
])

parsed_df = stream_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Apply ML model
predictions = model.transform(parsed_df)

output = predictions.select(
    "crop",
    "area",
    "production",
    "annual_rainfall",
    "soil_moisture_index",
    "prediction"
)

query = output.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query = output.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/home/sunbeam/Downloads/BigData_project/storage/streaming_predictions") \
    .option("checkpointLocation", "/home/sunbeam/Downloads/BigData_project/storage/checkpoints/streaming_predictions") \
    .start()

query.awaitTermination()
