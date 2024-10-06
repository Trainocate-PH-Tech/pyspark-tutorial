from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("RegressionExample").getOrCreate()

# Step 2: Create a sample DataFrame with features and labels
data = spark.createDataFrame([
    (1.0, 1.0, 2.0, 3.0),
    (2.0, 2.0, 3.0, 4.0),
    (3.0, 3.0, 4.0, 5.0),
    (4.0, 4.0, 5.0, 6.0),
    (5.0, 5.0, 6.0, 7.0)
], ["label", "feature1", "feature2", "feature3"])

# Step 3: Assemble the feature columns into a single column 'features'
assembler = VectorAssembler(
    inputCols=["feature1", "feature2", "feature3"], 
    outputCol="features"
)

# Step 4: Define Linear Regression model
lr = LinearRegression(featuresCol='features', labelCol='label')

# Step 5: Create a pipeline with the assembler and linear regression
pipeline = Pipeline(stages=[assembler, lr])

# Step 6: Split the data into training (80%) and test (20%) sets
train_data, test_data = data.randomSplit([0.8, 0.2])

# Step 7: Train the model
model = pipeline.fit(train_data)

# Step 8: Make predictions on the test data
predictions = model.transform(test_data)

# Step 9: Show predictions
predictions.select("features", "label", "prediction").show()

# Step 10: Evaluate the model using Regression Evaluator
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")

# Compute RMSE (Root Mean Squared Error)
rmse = evaluator.evaluate(predictions)
print(f"Test RMSE: {rmse}")

# Stop the Spark session
spark.stop()
