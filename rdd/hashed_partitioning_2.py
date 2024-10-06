from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("JoinHashPartitioningExample").getOrCreate()

# Create two RDDs with key-value pairs
rdd1 = spark.sparkContext.parallelize([("key1", 1), ("key2", 2), ("key3", 3), ("key1", 4)])
rdd2 = spark.sparkContext.parallelize([("key1", "A"), ("key2", "B"), ("key3", "C"), ("key1", "D")])

# Apply hash partitioning to both RDDs on the key with 2 partitions
rdd1_partitioned = rdd1.partitionBy(2)
rdd2_partitioned = rdd2.partitionBy(2)

# Perform a join on the partitioned RDDs
joined_rdd = rdd1_partitioned.join(rdd2_partitioned)

# Collect and print the result of the join
result = joined_rdd.collect()
print(result)

# Stop the Spark session
spark.stop()
