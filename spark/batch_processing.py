from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import *

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("SmartFarmingBatchProcessing-AllCrops") \
    .getOrCreate()

# --------------------------------------------------
# 2. Read data from Kafka
# --------------------------------------------------
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "agriculture_batch") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json")

# --------------------------------------------------
# 3. Read JSON dynamically (no strict schema)
# --------------------------------------------------
raw_df = spark.read.json(json_df.rdd.map(lambda r: r.json))

# Basic identifiers
base_cols = ["State Name", "Dist Name", "Year"]

# Detect crop columns automatically
area_cols = [c for c in raw_df.columns if "AREA" in c and "1000 ha" in c]
prod_cols = [c for c in raw_df.columns if "PRODUCTION" in c]
yield_cols = [c for c in raw_df.columns if "YIELD" in c]

final_df = None

# --------------------------------------------------
# 4. Convert WIDE → LONG (ALL CROPS)
# --------------------------------------------------
for area_col in area_cols:
    crop_name = area_col.split(" AREA")[0]

    prod_col = f"{crop_name} PRODUCTION (1000 tons)"
    yield_col = f"{crop_name} YIELD (Kg per ha)"

    if prod_col in prod_cols and yield_col in yield_cols:
        temp_df = raw_df.select(
            col("State Name").alias("state"),
            col("Dist Name").alias("district"),
            col("Year").cast("int").alias("year"),
            lit(crop_name).alias("crop"),
            col(area_col).cast("double").alias("area"),
            col(prod_col).cast("double").alias("production"),
            col(yield_col).cast("double").alias("yield")
        ).dropna()

        if final_df is None:
            final_df = temp_df
        else:
            final_df = final_df.union(temp_df)

# --------------------------------------------------
# 5. Show result
# --------------------------------------------------
final_df.show(10, truncate=False)

# --------------------------------------------------
# 6. Save processed data
# --------------------------------------------------
final_df.write.mode("overwrite").parquet(
    "/home/sunbeam/Downloads/BigData_project/storage/parquet/agriculture_all_crops"
)

print("✅ Spark batch processing for ALL crops completed successfully")
