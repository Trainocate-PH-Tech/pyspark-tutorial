from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("CustomHashPartitioning").getOrCreate()

# Create an RDD with key-value pairs
data = [("key1", 1), ("key2", 2), ("key3", 3), ("key4", 4), ("key5", 5)]

# Parallelize the data and create an RDD
rdd = spark.sparkContext.parallelize(data)

# Define a custom partitioning function (e.g., partition by length of the key)
def custom_partitioner(key):
    return len(key) % 3  # Partition based on the length of the key

# Apply custom hash partitioning with 3 partitions
partitioned_rdd = rdd.partitionBy(3, custom_partitioner)

# Inspect the partitioned data
partitioned_data = partitioned_rdd.mapPartitionsWithIndex(print_partition).collect()

# Print the partitioned data
for part in partitioned_data:
    print(part)

# Stop the Spark session
spark.stop()
