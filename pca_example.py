from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, PCA

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("PCADimensionalityReduction").getOrCreate()

# Step 2: Create a sample DataFrame with features
data = spark.createDataFrame([
    (1.0, 2.0, 3.0, 4.0),
    (2.0, 3.0, 4.0, 5.0),
    (3.0, 4.0, 5.0, 6.0),
    (4.0, 5.0, 6.0, 7.0),
    (5.0, 6.0, 7.0, 8.0)
], ["feature1", "feature2", "feature3", "feature4"])

# Step 3: Assemble the feature columns into a single column 'features'
assembler = VectorAssembler(
    inputCols=["feature1", "feature2", "feature3", "feature4"], 
    outputCol="features"
)

# Transform the data to get a 'features' column
assembled_data = assembler.transform(data)

# Step 4: Apply PCA to reduce dimensions
# Let's reduce the dimensionality to 2 principal components
pca = PCA(k=2, inputCol="features", outputCol="pcaFeatures")

# Step 5: Fit the PCA model and transform the data
pca_model = pca.fit(assembled_data)
pca_result = pca_model.transform(assembled_data)

# Show the original features and the PCA-transformed features
pca_result.select("features", "pcaFeatures").show(truncate=False)

# Step 6: Stop the Spark session
spark.stop()
