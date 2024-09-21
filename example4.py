from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Example 4').getOrCreate()

df = spark.read.csv('data/test_with_null.csv', header=True, inferSchema=True)

df.show()

df = df.na.drop()

df.show()

df.filter("Salary <= 20000").show()

df.filter("Salary <= 20000").select(['Name', 'age']).show()

df.filter(df['Salary'] <= 20000).show()

df.filter((df['Salary'] <= 20000) & (df['Salary'] >= 15000)).show()
df.filter(~(df['Salary'] <= 20000)).show()
