from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

# --------------------------------------------------
# 1. Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("SmartFarming-ML-Enriched") \
    .getOrCreate()

# --------------------------------------------------
# 2. Load enriched dataset
# --------------------------------------------------
df = spark.read.parquet(
    "/home/sunbeam/Downloads/BigData_project/storage/parquet/agriculture_ml_enriched"
    # "storage/parquet/agriculture_all_crops"
)
df = df.filter(col("yield") > 0)

# --------------------------------------------------
# 3. Encode categorical feature (crop)
# --------------------------------------------------
crop_indexer = StringIndexer(
    inputCol="crop",
    outputCol="crop_index",
    handleInvalid="keep"
)

# --------------------------------------------------
# 4. Assemble features
# --------------------------------------------------
assembler = VectorAssembler(
    inputCols=[
        "year",
        "crop_index",
        "area",
        "production",
        "annual_rainfall",
        "kharif_rainfall",
        "soil_moisture_index",
        "soil_quality_index"
    ],
    outputCol="raw_features"
)

# --------------------------------------------------
# 5. Scale features
# --------------------------------------------------
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True
)

# --------------------------------------------------
# 6. Regression Model
# --------------------------------------------------
lr = LinearRegression(
    featuresCol="features",
    labelCol="yield"
)

# --------------------------------------------------
# 7. Pipeline
# --------------------------------------------------
pipeline = Pipeline(stages=[
    crop_indexer,
    assembler,
    scaler,
    lr
])

# --------------------------------------------------
# 8. Train / Test Split
# --------------------------------------------------
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# --------------------------------------------------
# 9. Train model
# --------------------------------------------------
model = pipeline.fit(train_df)

# --------------------------------------------------
# 10. Evaluate
# --------------------------------------------------
predictions = model.transform(test_df)
predictions.select(
    "crop", "yield", "prediction"
).show(10, truncate=False)

from pyspark.ml.evaluation import RegressionEvaluator

# --------------------------------------------------
# 11. Model Evaluation
# --------------------------------------------------
rmse_evaluator = RegressionEvaluator(
    labelCol="yield",
    predictionCol="prediction",
    metricName="rmse"
)

mae_evaluator = RegressionEvaluator(
    labelCol="yield",
    predictionCol="prediction",
    metricName="mae"
)

r2_evaluator = RegressionEvaluator(
    labelCol="yield",
    predictionCol="prediction",
    metricName="r2"
)

rmse = rmse_evaluator.evaluate(predictions)
mae = mae_evaluator.evaluate(predictions)
r2 = r2_evaluator.evaluate(predictions)

print("📊 MODEL EVALUATION RESULTS")
print(f"RMSE : {rmse}")
print(f"MAE  : {mae}")
print(f"R2   : {r2}")



# --------------------------------------------------
# 11. Save final enriched model
# --------------------------------------------------
model.write().overwrite().save(
    "/home/sunbeam/Downloads/BigData_project/models/smart_farming_yield_model_enriched"
)

print("✅ Enriched ML model trained and saved successfully")