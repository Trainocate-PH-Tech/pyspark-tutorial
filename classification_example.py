from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("ClassificationExample").getOrCreate()

# Step 2: Create a sample DataFrame with features and label
data = spark.createDataFrame([
    (0, 1.0, 2.0, 3.0),
    (1, 2.0, 3.0, 4.0),
    (0, 3.0, 4.0, 5.0),
    (1, 4.0, 5.0, 6.0),
    (0, 5.0, 6.0, 7.0)
], ["label", "feature1", "feature2", "feature3"])

# Step 3: Assemble the features into a single column
assembler = VectorAssembler(
    inputCols=["feature1", "feature2", "feature3"], 
    outputCol="features"
)

# Step 4: Define the Logistic Regression model
lr = LogisticRegression(featuresCol='features', labelCol='label')

# Step 5: Create a pipeline with the assembler and logistic regression
pipeline = Pipeline(stages=[assembler, lr])

# Step 6: Split the data into training (80%) and test (20%) sets
train_data, test_data = data.randomSplit([0.8, 0.2])

# Step 7: Train the model
model = pipeline.fit(train_data)

# Step 8: Make predictions on the test data
predictions = model.transform(test_data)

# Step 9: Show predictions
predictions.select("features", "label", "prediction").show()

# Step 10: Evaluate the model using Binary Classification Evaluator
evaluator = BinaryClassificationEvaluator(labelCol='label', rawPredictionCol='rawPrediction', metricName='areaUnderROC')

# Compute the Area Under ROC (Receiver Operating Characteristic)
roc_auc = evaluator.evaluate(predictions)
print(f"Test AUC (Area Under ROC): {roc_auc}")

# Stop the Spark session
spark.stop()
