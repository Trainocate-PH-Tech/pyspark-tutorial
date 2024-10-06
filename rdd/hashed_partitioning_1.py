from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("HashedPartitioningExample").getOrCreate()

# Create an RDD with key-value pairs
data = [("key1", 100), ("key2", 200), ("key3", 300), ("key1", 400), ("key2", 500)]

# Parallelize the data and create an RDD with 3 partitions
rdd = spark.sparkContext.parallelize(data)

# Apply hash partitioning on the key with 3 partitions
partitioned_rdd = rdd.partitionBy(3)

# Inspect the partitioning
def print_partition(index, iterator):
    yield f"Partition {index}: {list(iterator)}"

partitioned_data = partitioned_rdd.mapPartitionsWithIndex(print_partition).collect()

# Print the partitioned data
for part in partitioned_data:
    print(part)

# Stop the Spark session
spark.stop()
