# Import the SparkSession class from the pyspark.sql module
from pyspark.sql import SparkSession

# Create a SparkSession, which is the entry point to using Spark
spark = SparkSession.builder \
    .appName('Practice') \  # Set the application name to 'Practice'
    .getOrCreate()          # Create a new session or retrieve an existing one

# Read a CSV file into a DataFrame
# Specify that the first row contains headers
df = spark.read.option('header', 'true').csv("data/test.csv")

# Display the contents of the DataFrame
df.show()

# Print the schema of the DataFrame to understand its structure
df.printSchema()
