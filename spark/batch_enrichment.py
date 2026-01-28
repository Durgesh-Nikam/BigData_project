from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, expr
)

spark = SparkSession.builder \
    .appName("SmartFarming-Batch-Enrichment") \
    .getOrCreate()

# --------------------------------------------------
# 1. Load crop data (already processed)
# --------------------------------------------------
crop_df = spark.read.parquet(
    "/home/sunbeam/Downloads/BigData_project/storage/parquet/agriculture_all_crops"
)

crop_df = crop_df \
    .withColumn("state", lower(trim(col("state")))) \
    .withColumn("year", col("year").cast("int"))

# --------------------------------------------------
# 2. Load rainfall data
# --------------------------------------------------
rain_df = spark.read.option("header", True).csv(
    "/home/sunbeam/Downloads/BigData_project/data/historical/Monthly_Rainfall_From_1901_to_2017.csv"
)

rain_df = rain_df.select(
    lower(trim(col("States/UTs"))).alias("state"),
    col("YEAR").cast("int").alias("year"),
    col("JUN").cast("double"),
    col("JUL").cast("double"),
    col("AUG").cast("double"),
    col("SEP").cast("double"),
    col("ANNUAL").cast("double").alias("annual_rainfall")
)

# Seasonal (Kharif) rainfall
rain_df = rain_df.withColumn(
    "kharif_rainfall",
    col("JUN") + col("JUL") + col("AUG") + col("SEP")
)

# --------------------------------------------------
# 3. Join Crop + Weather
# --------------------------------------------------
joined_df = crop_df.join(
    rain_df,
    on=["state", "year"],
    how="left"
)

# --------------------------------------------------
# 4. Simulate Soil Features (Industry-accepted)
# --------------------------------------------------
final_df = joined_df \
    .withColumn(
        "soil_moisture_index",
        expr("round((kharif_rainfall / 1000) + rand() * 0.2, 3)")
    ) \
    .withColumn(
        "soil_quality_index",
        expr("round((annual_rainfall / 3000) + rand() * 0.3, 3)")
    )

# --------------------------------------------------
# 5. Select ML-ready columns
# --------------------------------------------------
ml_df = final_df.select(
    "state",
    "district",
    "year",
    "crop",
    "area",
    "production",
    "annual_rainfall",
    "kharif_rainfall",
    "soil_moisture_index",
    "soil_quality_index",
    "yield"
).dropna()

# --------------------------------------------------
# 6. Save enriched dataset
# --------------------------------------------------
ml_df.write.mode("overwrite").parquet(
    "/home/sunbeam/Downloads/BigData_project/storage/parquet/agriculture_ml_enriched"
)

print("✅ Batch enrichment complete: Crop + Weather + Soil")
