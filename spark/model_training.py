from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

# --------------------------------------------------
# 1. Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("SmartFarmingMLTraining-Improved") \
    .getOrCreate()

# --------------------------------------------------
# 2. Load processed data
# --------------------------------------------------
df = spark.read.parquet("/home/sunbeam/Downloads/BigData_project/storage/parquet/agriculture_all_crops")

# --------------------------------------------------
# 3. Encode categorical feature (crop)
# --------------------------------------------------
crop_indexer = StringIndexer(
    inputCol="crop",
    outputCol="crop_index"
)

# --------------------------------------------------
# 4. Assemble raw features
# --------------------------------------------------
assembler = VectorAssembler(
    inputCols=["year", "crop_index", "area", "production"],
    outputCol="raw_features"
)

# --------------------------------------------------
# 5. Feature Scaling (KEY IMPROVEMENT)
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
# 8. Train/Test Split
# --------------------------------------------------
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# --------------------------------------------------
# 9. Train Model
# --------------------------------------------------
model = pipeline.fit(train_df)

# --------------------------------------------------
# 10. Predict
# --------------------------------------------------
predictions = model.transform(test_df)

predictions.select("crop", "yield", "prediction").show(10)

# --------------------------------------------------
# 11. Save Improved Model
# --------------------------------------------------
model.write().overwrite().save(
    "/home/sunbeam/Downloads/BigData_project/models/multicrop_yield_model_scaled"
)

print("✅ Improved ML model trained with feature scaling")
