from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Example 5').getOrCreate()

df = spark.read.csv('data/aggregate_example.csv', header=True, inferSchema=True)

df.printSchema()

df.show()

df.groupBy('Name').sum().show()

df.groupBy('Department').sum().show()
df.groupBy('Department').mean().show()

df.groupBy('Department').count().show()

df.agg({'Salary': 'sum'}).show()

df.groupBy('Name').max().show()
