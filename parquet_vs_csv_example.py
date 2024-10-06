import os
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CSVvsParquet").getOrCreate()

# Sample data to convert to DataFrame (let's generate 1 million rows)
data = [{"id": i, "name": f"Name_{i}", "age": i % 100, "salary": (i % 50) * 1000} for i in range(1, 1000001)]

# Convert list of dictionaries to DataFrame
df = spark.createDataFrame(data)

# Write the DataFrame to a CSV file
csv_output = "/tmp/output.csv"
df.write.csv(csv_output, header=True)

# Write the DataFrame to a Parquet file
parquet_output = "/tmpt/output.parquet"
df.write.parquet(parquet_output)

# Check file sizes in bytes
csv_size = sum(os.path.getsize(f) for f in os.listdir(csv_output) if f.endswith(".csv"))
parquet_size = sum(os.path.getsize(f) for f in os.listdir(parquet_output) if f.endswith(".parquet"))

print(f"CSV file size: {csv_size} bytes")
print(f"Parquet file size: {parquet_size} bytes")
