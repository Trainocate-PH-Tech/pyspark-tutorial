from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("ClusteringExample").getOrCreate()

# Step 2: Create a sample DataFrame with features
data = spark.createDataFrame([
    (1.0, 2.0, 3.0),
    (2.0, 3.0, 4.0),
    (3.0, 4.0, 5.0),
    (8.0, 9.0, 10.0),
    (9.0, 10.0, 11.0),
    (10.0, 11.0, 12.0)
], ["feature1", "feature2", "feature3"])

# Step 3: Assemble the features into a single column
assembler = VectorAssembler(
    inputCols=["feature1", "feature2", "feature3"], 
    outputCol="features"
)

# Transform the data to get a 'features' column
assembled_data = assembler.transform(data)

# Step 4: Define the KMeans model with k=2 clusters
kmeans = KMeans(featuresCol='features', k=2, seed=1)

# Step 5: Train the model
model = kmeans.fit(assembled_data)

# Step 6: Make predictions (cluster assignment)
predictions = model.transform(assembled_data)

# Step 7: Show the predicted cluster for each point
predictions.select("features", "prediction").show()

# Step 8: Evaluate the model using ClusteringEvaluator
evaluator = ClusteringEvaluator()

# Silhouette with squared Euclidean distance
silhouette = evaluator.evaluate(predictions)
print(f"Silhouette Score: {silhouette}")

# Step 9: Stop the Spark session
spark.stop()
