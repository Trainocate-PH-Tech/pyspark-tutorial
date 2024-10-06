from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

# Initialize Spark session
spark = SparkSession.builder.appName("ReadJSONWithArrays").getOrCreate()

# Read the JSON file into a DataFrame
array_df = spark.read.json("data_with_arrays.json")

# Show the DataFrame
array_df.show(truncate=False)

# Explode the interests array to create multiple rows for each interest
exploded_df = array_df.withColumn("interest", explode(array_df.interests))

# Show the exploded DataFrame
exploded_df.show(truncate=False)

# Stop the Spark session
spark.stop()
