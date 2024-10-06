# RDD Exercises

## Exercise 1: Basic Hash Partitioning from CSV

Create an RDD of customer purchases by reading the customers_purchases.csv file. Apply hash partitioning to the RDD using the `partitionBy()` method and verify that all records for the same customer end up in the same partition.

### Code

```python
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("Exercise1").getOrCreate()

# Read the data from CSV
rdd = spark.read.csv("customers_purchases.csv", header=True).rdd

# 1. Create key-value pairs with customer names as the key
customer_rdd = rdd.map(lambda row: (row['customer_name'], int(row['purchase_amount'])))

# 2. Apply hash partitioning
partitioned_rdd = customer_rdd.partitionBy(3)

# Inspect partitions
partitioned_data = partitioned_rdd.mapPartitionsWithIndex(
    lambda index, it: [f"Partition {index}: {list(it)}"]
).collect()

# Print partitioned data
for partition in partitioned_data:
    print(partition)

spark.stop()
```

### Data (`customers_purchases.csv`)
```
customer_id,customer_name,purchase_amount
1,John Doe,100
2,Jane Smith,200
3,Mary Johnson,300
4,John Doe,400
5,Jane Smith,500
6,David Miller,150
7,Linda Brown,250
8,John Doe,50
```

## Exercise 2: Optimize Join Using Hash Partitioning

Create two RDDs by reading from two CSV files: `customers_purchases.csv` and `customers_demographics.csv`. Perform a join operation on `customer_id` after applying hash partitioning to improve performance.

### Data (`customers_demographics.csv`)

```
customer_id,customer_name,age,gender
1,John Doe,34,Male
2,Jane Smith,28,Female
3,Mary Johnson,45,Female
6,David Miller,31,Male
7,Linda Brown,40,Female
```

### Code

```python
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("Exercise2").getOrCreate()

# Read the purchases and demographics data
purchases_rdd = spark.read.csv("customers_purchases.csv", header=True).rdd
demographics_rdd = spark.read.csv("customers_demographics.csv", header=True).rdd

# Create key-value pairs based on customer_id
purchases_kv = purchases_rdd.map(lambda row: (row['customer_id'], (row['customer_name'], int(row['purchase_amount']))))
demographics_kv = demographics_rdd.map(lambda row: (row['customer_id'], (row['customer_name'], row['age'], row['gender'])))

# Apply hash partitioning
partitioned_purchases = purchases_kv.partitionBy(3)
partitioned_demographics = demographics_kv.partitionBy(3)

# Perform the join
joined_rdd = partitioned_purchases.join(partitioned_demographics)

# Collect and print the result of the join
result = joined_rdd.collect()
for item in result:
    print(item)

spark.stop()
```

## Exercise 3: Custom Hash Partitioning

Read the same `customers_purchases.csv` and apply custom hash partitioning based on the length of the customer names.

### Code

```python
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Exercise3").getOrCreate()

# Read data from CSV
rdd = spark.read.csv("customers_purchases.csv", header=True).rdd

# Create key-value pairs based on customer_name
customer_rdd = rdd.map(lambda row: (row['customer_name'], int(row['purchase_amount'])))

# Custom partitioner based on the length of the customer name
def custom_partitioner(key):
    return len(key) % 3

# Apply custom partitioning
partitioned_rdd = customer_rdd.partitionBy(3, custom_partitioner)

# Inspect partitions
partitioned_data = partitioned_rdd.mapPartitionsWithIndex(
    lambda index, it: [f"Partition {index}: {list(it)}"]
).collect()

# Print partitioned data
for partition in partitioned_data:
    print(partition)

spark.stop()
```

## Exercise 4: Hash Partitinoing with GroupByKey

Read the `students_grades.csv` file, partition the data by student names using hash partitioning, and use `groupByKey()` to group the grades by each student.

### Code

```python
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Exercise4").getOrCreate()

# Read the data from CSV
rdd = spark.read.csv("students_grades.csv", header=True).rdd

# Create key-value pairs based on student names
student_rdd = rdd.map(lambda row: (row['student_name'], int(row['grade'])))

# Apply hash partitioning
partitioned_rdd = student_rdd.partitionBy(3)

# Group grades by student name
grouped_rdd = partitioned_rdd.groupByKey()

# Collect and print the grouped results
result = grouped_rdd.collect()
for student, grades in result:
    print(f"Student: {student}, Grades: {list(grades)}")

spark.stop()
```

### Data (`students_grades.csv`)

```
student_name,subject,grade
John Doe,Math,85
Jane Smith,Science,92
Mary Johnson,History,88
John Doe,English,80
Jane Smith,Math,95
Mary Johnson,Science,89
```

## Exercise 5: Manual Hash Partitioning for Large Datasets

Read the `products_sales.csv` file and apply hash partitioning on `product_id` to distribute the data into 5 partitions. Verify the distribution of data across partitions.

### Code

```python
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Exercise5").getOrCreate()

# Read the data from CSV
rdd = spark.read.csv("products_sales.csv", header=True).rdd

# Create key-value pairs with product IDs as keys
product_rdd = rdd.map(lambda row: (row['product_id'], int(row['sales_amount'])))

# Apply hash partitioning with 5 partitions
partitioned_rdd = product_rdd.partitionBy(5)

# Check the number of records in each partition
partition_count = partitioned_rdd.mapPartitionsWithIndex(
    lambda index, it: [(index, len(list(it)))]
).collect()

# Print the partition counts
for partition in partition_count:
    print(f"Partition {partition[0]}: {partition[1]} records")

spark.stop()
```

### Data (`products_sales.csv`)

```
product_id,product_name,sales_amount
101,Product A,500
102,Product B,300
103,Product C,700
101,Product A,400
102,Product B,600
104,Product D,200
```
